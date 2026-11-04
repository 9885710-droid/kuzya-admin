"""Админка КУЗЯ (moscow, этап 3, 04.11.2026): диалоги, рассылки, посты в группы, аналитика.

Пишется заново по прежней (Питер, июль–август 2026, восстановлена по журналу лишь частично): FastAPI + Jinja SSR,
подписанная cookie-сессия, общая БД kuzya со своими таблицами. Переписку пишет приёмник бота в kuzya-web
(kuzya_messages / kuzya_dialogs) — админка читает и отвечает от имени бота.

Вход: пароль задаёт сам человек по одноразовой ссылке (24 ч, в БД только sha256 токена), bcrypt, лимит неудачных
попыток по логину и по IP. Боевая рассылка — только admin, только при BROADCAST_ENABLED=1 и только после теста
и холостого прогона ЭТОГО ЖЕ содержимого за последний час.
"""
import asyncio
import hashlib
import ipaddress
import json
import logging
import os
import re
import secrets
import sys
from datetime import datetime, timezone, timedelta, date
from pathlib import Path

import asyncpg
import bcrypt
from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from . import maxbot

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("kuzya-admin")
for _n in ("httpx", "httpcore"):          # адреса запросов (id чатов, ссылки загрузки) в журнал не пишем
    logging.getLogger(_n).setLevel(logging.WARNING)

BASE = Path(__file__).resolve().parent
UPLOADS = Path(os.environ.get("UPLOADS_DIR", "/app/uploads"))
PUBLIC_URL = os.environ.get("PUBLIC_URL", "https://adminkuzya.velcom-holding.ru").rstrip("/")
VITRINA_URL = os.environ.get("VITRINA_URL", "https://kuzya.velcom-holding.ru")
BOT_USERNAME = os.environ.get("BOT_USERNAME", "id5530005476_bot")
MONITOR_CHAT_ID = os.environ.get("MONITOR_CHAT_ID", "")
ADMIN_NOTIFY_UID = os.environ.get("ADMIN_NOTIFY_UID", "")      # Max uid, кому уходят ссылки установки пароля
BROADCAST_ENABLED = os.environ.get("BROADCAST_ENABLED", "0") == "1"
BROADCAST_DELAY = float(os.environ.get("BROADCAST_DELAY", "0.4"))
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "1") == "1"
OMSK = timezone(timedelta(hours=6))
SINCE = date(2026, 11, 3)                 # бот на moscow с 03.11 — аналитика с этого дня
MAX_PHOTOS, MAX_PHOTO_MB, MAX_VIDEO_MB = 5, 10, 100
LOGIN_FAILS_PER_LOGIN, LOGIN_FAILS_PER_IP, LOGIN_WINDOW = 5, 20, timedelta(minutes=15)
INVITE_TTL = timedelta(hours=24)
# доверяем X-Forwarded-For только от Traefik (сеть docker / хост); последний адрес в цепочке дописал он сам
TRUSTED_PROXIES = [ipaddress.ip_network(n) for n in ("127.0.0.0/8", "172.16.0.0/12", "10.0.0.0/8")]

# роли: admin — всё; marketer — диалоги, посты, тест и холостой прогон рассылки, аналитика
USERS = {"vadim": ("admin", "Вадим"), "nikolay": ("admin", "Николай"),
         "svetlana": ("marketer", "Светлана"), "irina": ("marketer", "Ирина"),
         "tatyana": ("marketer", "Татьяна"), "marina": ("marketer", "Марина")}
ROLE_TITLES = {"admin": "Администратор", "marketer": "Маркетолог"}

app = FastAPI(title="Админка КУЗЯ", docs_url=None, redoc_url=None, openapi_url=None)
tpl = Jinja2Templates(directory=str(BASE / "templates"))
_pool: asyncpg.Pool | None = None


# ═══════════════════════════════ БД ═══════════════════════════════

async def _init_conn(conn):
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def pool():
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(os.environ["DATABASE_URL"], min_size=1, max_size=6, init=_init_conn)
    return _pool


async def q(sql, *args):
    async with (await pool()).acquire() as c:
        return await c.fetch(sql, *args)


async def q1(sql, *args):
    async with (await pool()).acquire() as c:
        return await c.fetchrow(sql, *args)


async def qv(sql, *args):
    async with (await pool()).acquire() as c:
        return await c.fetchval(sql, *args)


async def qx(sql, *args):
    async with (await pool()).acquire() as c:
        return await c.execute(sql, *args)


SCHEMA = """
CREATE TABLE IF NOT EXISTS kuzya_admin_users (
  login TEXT PRIMARY KEY, role TEXT NOT NULL, name TEXT, pass_hash TEXT, pass_set_at TIMESTAMPTZ,
  max_uid BIGINT, active BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS kuzya_admin_invites (
  token_hash TEXT PRIMARY KEY, login TEXT NOT NULL REFERENCES kuzya_admin_users(login), created_by TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), expires_at TIMESTAMPTZ NOT NULL, used_at TIMESTAMPTZ);
CREATE TABLE IF NOT EXISTS kuzya_admin_login_attempts (
  id BIGSERIAL PRIMARY KEY, login TEXT, ip TEXT, ok BOOLEAN, ts TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE INDEX IF NOT EXISTS kuzya_admin_login_attempts_ts ON kuzya_admin_login_attempts(ts);
CREATE TABLE IF NOT EXISTS kuzya_admin_log (
  id BIGSERIAL PRIMARY KEY, who TEXT, action TEXT, details TEXT, ip TEXT, ts TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS kuzya_broadcasts (
  id BIGSERIAL PRIMARY KEY, kind TEXT NOT NULL, text TEXT, media JSONB, btn_text TEXT, btn_url TEXT,
  audience TEXT NOT NULL, content_hash TEXT NOT NULL, total INT NOT NULL DEFAULT 0, sent INT NOT NULL DEFAULT 0,
  failed INT NOT NULL DEFAULT 0, status TEXT NOT NULL, created_by TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ, result JSONB);
CREATE SEQUENCE IF NOT EXISTS kuzya_bot_chats_number START 101;
CREATE TABLE IF NOT EXISTS kuzya_bot_chats (
  chat_id BIGINT PRIMARY KEY, title TEXT, number INT UNIQUE DEFAULT nextval('kuzya_bot_chats_number'),
  kind TEXT NOT NULL DEFAULT 'shop', active BOOLEAN NOT NULL DEFAULT TRUE, source TEXT, participants INT,
  bot_admin BOOLEAN, added_by TEXT, checked_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS kuzya_scheduled_posts (
  id BIGSERIAL PRIMARY KEY, text TEXT, media JSONB, groups JSONB, run_at TIMESTAMPTZ, status TEXT NOT NULL,
  created_by TEXT, published_at TIMESTAMPTZ, result JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS kuzya_post_tags (
  tag TEXT PRIMARY KEY, chat_id BIGINT, title TEXT, post_id BIGINT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW());
CREATE TABLE IF NOT EXISTS kuzya_loyalty_bindings (
  user_id BIGINT NOT NULL, phone TEXT NOT NULL, bound_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (user_id, phone));
ALTER TABLE kuzya_messages ADD COLUMN IF NOT EXISTS delivery TEXT;
ALTER TABLE kuzya_messages ADD COLUMN IF NOT EXISTS edited_at TIMESTAMPTZ;
"""


async def log_action(who, action, details="", ip=None):
    await qx("INSERT INTO kuzya_admin_log(who, action, details, ip) VALUES ($1,$2,$3,$4)", who, action, details[:2000], ip)


# ═══════════════════════════════ безопасность ═══════════════════════════════

def client_ip(request: Request):
    peer = request.client.host if request.client else ""
    try:
        trusted = any(ipaddress.ip_address(peer) in n for n in TRUSTED_PROXIES)
    except ValueError:
        trusted = False
    xff = request.headers.get("x-forwarded-for", "")
    if trusted and xff:
        return xff.split(",")[-1].strip()
    return peer


@app.middleware("http")
async def guard(request: Request, call_next):
    # POST только со своей страницы: чужой Origin → 403 (вдобавок к SameSite=Strict)
    if request.method == "POST":
        origin = request.headers.get("origin")
        if origin and origin.split("://", 1)[-1] != request.headers.get("host", ""):
            return Response("bad origin", status_code=403)
    resp = await call_next(request)
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "no-referrer"      # токен ссылки установки пароля не утечёт в Referer
    resp.headers["Cache-Control"] = "no-store"
    return resp


app.add_middleware(SessionMiddleware, secret_key=os.environ.get("SESSION_SECRET") or secrets.token_hex(32),
                   session_cookie="kadm", max_age=7 * 86400, https_only=COOKIE_SECURE, same_site="strict")


async def current_user(request: Request):
    """Пользователь из БД на КАЖДЫЙ запрос: роль и блокировка действуют сразу, смена пароля гасит старые сессии."""
    s = request.session.get("u")
    if not s:
        return None
    r = await q1("SELECT login, role, name, max_uid, pass_set_at FROM kuzya_admin_users WHERE login=$1 AND active", s.get("login"))
    if not r or not r["pass_set_at"] or r["pass_set_at"].isoformat() != s.get("ps"):
        request.session.clear()
        return None
    return dict(r)


def is_admin(u):
    return bool(u) and u["role"] == "admin"


def ctx(request, u, active, **kw):
    return {"request": request, "user": u, "active": active, "role_title": ROLE_TITLES.get(u["role"], u["role"]),
            "is_admin": is_admin(u), **kw}


def need_login():
    return RedirectResponse("/login", status_code=303)


# ═══════════════════════════════ время ═══════════════════════════════

def omsk(dt):
    return dt.astimezone(OMSK) if dt else None


def fmt(dt):
    return omsk(dt).strftime("%d.%m %H:%M") if dt else ""


tpl.env.filters["fmt"] = fmt


# ═══════════════════════════════ вход ═══════════════════════════════

@app.api_route("/login", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def login_page(request: Request):
    return tpl.TemplateResponse("login.html", {"request": request, "error": None, "user": None})


@app.post("/login", response_class=HTMLResponse)
async def login_submit(request: Request, login: str = Form(""), password: str = Form("")):
    login, ip = login.strip().lower()[:40], client_ip(request)
    since = datetime.now(timezone.utc) - LOGIN_WINDOW
    fails = await q1("""SELECT COUNT(*) FILTER (WHERE login=$1) l, COUNT(*) FILTER (WHERE ip=$2) i
                        FROM kuzya_admin_login_attempts WHERE NOT ok AND ts > $3""", login, ip, since)
    if fails["l"] >= LOGIN_FAILS_PER_LOGIN or fails["i"] >= LOGIN_FAILS_PER_IP:
        await log_action(login, "login_blocked", f"{fails['l']}/{fails['i']} неудач за 15 мин", ip)
        return tpl.TemplateResponse("login.html", {"request": request, "user": None,
            "error": "Слишком много неудачных попыток. Подождите 15 минут."}, status_code=429)
    r = await q1("SELECT login, pass_hash, pass_set_at FROM kuzya_admin_users WHERE login=$1 AND active", login)
    ok = False
    if r and r["pass_hash"]:
        try: ok = bcrypt.checkpw(password.encode()[:72], r["pass_hash"].encode())
        except ValueError: ok = False
    else:
        bcrypt.checkpw(b"x", bcrypt.hashpw(b"y", bcrypt.gensalt(4)))   # время ответа не выдаёт, есть ли логин
    await qx("INSERT INTO kuzya_admin_login_attempts(login, ip, ok) VALUES ($1,$2,$3)", login, ip, ok)
    if not ok:
        return tpl.TemplateResponse("login.html", {"request": request, "user": None,
            "error": "Неверный логин или пароль"}, status_code=401)
    request.session.clear()
    request.session["u"] = {"login": login, "ps": r["pass_set_at"].isoformat()}
    await log_action(login, "login", "", ip)
    return RedirectResponse("/", status_code=303)


@app.post("/logout")
async def logout(request: Request):
    u = await current_user(request)
    if u:
        await log_action(u["login"], "logout", "", client_ip(request))
    request.session.clear()
    return RedirectResponse("/login", status_code=303)


def _token_hash(t):
    return hashlib.sha256(t.encode()).hexdigest()


async def make_invite(login, by):
    """Новая ссылка гасит прежние неиспользованные ссылки этого логина."""
    t = secrets.token_urlsafe(32)
    await qx("UPDATE kuzya_admin_invites SET used_at=NOW() WHERE login=$1 AND used_at IS NULL", login)
    await qx("INSERT INTO kuzya_admin_invites(token_hash, login, created_by, expires_at) VALUES ($1,$2,$3,$4)",
             _token_hash(t), login, by, datetime.now(timezone.utc) + INVITE_TTL)
    return f"{PUBLIC_URL}/setpass?t={t}"


async def _invite_row(t):
    return await q1("""SELECT i.token_hash, i.login, u.name FROM kuzya_admin_invites i JOIN kuzya_admin_users u USING (login)
                       WHERE i.token_hash=$1 AND i.used_at IS NULL AND i.expires_at > NOW() AND u.active""", _token_hash(t or ""))


@app.get("/setpass", response_class=HTMLResponse)
async def setpass_page(request: Request, t: str = ""):
    r = await _invite_row(t)
    return tpl.TemplateResponse("setpass.html", {"request": request, "user": None, "t": t, "inv": r,
        "error": None if r else "Ссылка недействительна или устарела. Попросите новую у администратора."})


@app.post("/setpass", response_class=HTMLResponse)
async def setpass_submit(request: Request, t: str = Form(""), p1: str = Form(""), p2: str = Form("")):
    r = await _invite_row(t)
    err = None
    if not r: err = "Ссылка недействительна или устарела. Попросите новую у администратора."
    elif len(p1) < 10: err = "Пароль — не короче 10 символов."
    elif len(p1.encode()) > 72: err = "Пароль слишком длинный (до 72 байт)."
    elif p1 != p2: err = "Пароли не совпадают."
    if err:
        return tpl.TemplateResponse("setpass.html", {"request": request, "user": None, "t": t, "inv": r, "error": err})
    h = bcrypt.hashpw(p1.encode(), bcrypt.gensalt(12)).decode()
    async with (await pool()).acquire() as c, c.transaction():
        done = await c.execute("UPDATE kuzya_admin_invites SET used_at=NOW() WHERE token_hash=$1 AND used_at IS NULL", r["token_hash"])
        if done != "UPDATE 1":
            return tpl.TemplateResponse("setpass.html", {"request": request, "user": None, "t": t, "inv": None,
                                                         "error": "Ссылка уже использована."})
        await c.execute("UPDATE kuzya_admin_users SET pass_hash=$1, pass_set_at=NOW() WHERE login=$2", h, r["login"])
    await log_action(r["login"], "set_password", "по одноразовой ссылке", client_ip(request))
    return RedirectResponse("/login", status_code=303)


# ═══════════════════════════════ пользователи (admin) ═══════════════════════════════

@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    u = await current_user(request)
    if not u: return need_login()
    if not is_admin(u): return RedirectResponse("/", status_code=303)
    users = await q("""SELECT u.*, (SELECT MAX(expires_at) FROM kuzya_admin_invites i WHERE i.login=u.login AND used_at IS NULL
                         AND expires_at > NOW()) invite_until FROM kuzya_admin_users u ORDER BY role, login""")
    logs = await q("SELECT * FROM kuzya_admin_log ORDER BY id DESC LIMIT 200")
    return tpl.TemplateResponse("users.html", ctx(request, u, "users", users=users, logs=logs))


@app.post("/users/{login}/invite")
async def users_invite(request: Request, login: str):
    u = await current_user(request)
    if not is_admin(u): return Response(status_code=403)
    if not await qv("SELECT 1 FROM kuzya_admin_users WHERE login=$1", login):
        return Response(status_code=404)
    link = await make_invite(login, u["login"])
    await deliver_invites([(login, link)])
    await log_action(u["login"], "invite", login, client_ip(request))
    return RedirectResponse("/users", status_code=303)


@app.post("/users/{login}/update")
async def users_update(request: Request, login: str, max_uid: str = Form(""), active: str = Form("")):
    u = await current_user(request)
    if not is_admin(u): return Response(status_code=403)
    uid = int(max_uid) if max_uid.strip().isdigit() else None
    act = active == "1" or login == u["login"]          # себя не заблокировать
    await qx("UPDATE kuzya_admin_users SET max_uid=$1, active=$2 WHERE login=$3", uid, act, login)
    await log_action(u["login"], "user_update", f"{login}: max_uid={'есть' if uid else '—'}, active={act}", client_ip(request))
    return RedirectResponse("/users", status_code=303)


async def deliver_invites(pairs):
    """Ссылки — одним сообщением в личку ADMIN_NOTIFY_UID (Вадим раздаёт сам); в журнал и в ответ HTTP не пишутся."""
    if not ADMIN_NOTIFY_UID:
        raise RuntimeError("ADMIN_NOTIFY_UID не задан")
    lines = [f"• {login} ({USERS.get(login, ('', login))[1]}): {link}" for login, link in pairs]
    await maxbot.send(user_id=int(ADMIN_NOTIFY_UID), text="🔑 Админка КУЗЯ — ссылки установки пароля (одноразовые, 24 ч):\n"
                      + "\n".join(lines) + "\n\nПеред тем как переслать — проверьте, кому. Ссылку не публиковать.")


# ═══════════════════════════════ главная ═══════════════════════════════

@app.api_route("/", methods=["GET", "HEAD"])
async def root(request: Request):
    u = await current_user(request)
    return RedirectResponse("/dialogs" if u else "/login", status_code=303)


@app.get("/healthz")
async def healthz():
    return {"ok": await qv("SELECT 1") == 1}


# ═══════════════════════════════ диалоги ═══════════════════════════════

def _digits(s):
    return re.sub(r"\D", "", s or "")


@app.get("/dialogs", response_class=HTMLResponse)
async def dialogs_page(request: Request, only: str = ""):
    u = await current_user(request)
    if not u: return need_login()
    q_ = request.query_params.get("q", "").strip()
    where, args = ["TRUE"], []
    if only == "open":
        where.append("d.unanswered > 0")
    if q_:
        dg = _digits(q_)
        if len(dg) >= 6:
            # телефон (последние 10 цифр) — из контактов заказов и привязок карт; или uid целиком
            args += [dg[-10:], int(dg)]
            where.append(f"""(d.user_id = ${len(args)} OR d.user_id IN (
                SELECT user_id FROM kuzya_user_contacts WHERE right(regexp_replace(phone,'\\D','','g'),10) = ${len(args)-1}
                UNION SELECT user_id FROM kuzya_loyalty_bindings WHERE right(regexp_replace(phone,'\\D','','g'),10) = ${len(args)-1}))""")
        else:
            args.append(f"%{q_}%")
            where.append(f"(bu.max_name ILIKE ${len(args)} OR bu.name ILIKE ${len(args)} OR c.name ILIKE ${len(args)})")
    rows = await q(f"""
        SELECT d.*, COALESCE(bu.name, bu.max_name, c.name) AS uname, c.phone,
               (SELECT text FROM kuzya_messages m WHERE m.user_id=d.user_id AND m.kind IN ('text','operator')
                 ORDER BY id DESC LIMIT 1) AS last_text
        FROM kuzya_dialogs d LEFT JOIN kuzya_bot_users bu USING (user_id) LEFT JOIN kuzya_user_contacts c USING (user_id)
        WHERE {' AND '.join(where)} ORDER BY d.unanswered > 0 DESC, d.last_at DESC NULLS LAST LIMIT 200""", *args)
    return tpl.TemplateResponse("dialogs.html", ctx(request, u, "dialogs", rows=rows, q=q_, only=only))


async def _user_card(uid):
    return await q1("""SELECT bu.user_id, COALESCE(bu.name, bu.max_name) uname, bu.max_username, bu.source, bu.created_at,
                              c.phone, c.name cname, d.unanswered
                       FROM kuzya_bot_users bu LEFT JOIN kuzya_user_contacts c USING (user_id)
                       LEFT JOIN kuzya_dialogs d USING (user_id) WHERE bu.user_id=$1""", uid)


def _msg_json(m):
    return {"id": m["id"], "dir": m["direction"], "kind": m["kind"], "text": m["text"] or "", "payload": m["payload"],
            "operator": m["operator"], "at": fmt(m["created_at"]), "delivery": m["delivery"],
            "edited": bool(m["edited_at"]), "editable": m["kind"] == "operator" and bool(m["max_mid"])}


@app.get("/dialogs/{uid}", response_class=HTMLResponse)
async def dialog_page(request: Request, uid: int):
    u = await current_user(request)
    if not u: return need_login()
    card = await _user_card(uid)
    if not card: return Response("Нет такого пользователя бота", status_code=404)
    orders = await q("SELECT id, site_order_id, total, status, pay_status, is_test, created_at FROM kuzya_orders_log "
                     "WHERE user_id=$1 ORDER BY id DESC LIMIT 10", uid)
    return tpl.TemplateResponse("dialog.html", ctx(request, u, "dialogs", card=card, orders=orders, vitrina=VITRINA_URL))


@app.get("/api/dialogs/{uid}/messages")
async def dialog_messages(request: Request, uid: int, after: int = 0):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    rows = await q("SELECT * FROM kuzya_messages WHERE user_id=$1 AND id > $2 ORDER BY id LIMIT 500", uid, after)
    return {"ok": True, "messages": [_msg_json(m) for m in rows]}


async def mirror_operator(uid, login, text):
    if MONITOR_CHAT_ID:
        try:
            await maxbot.send(chat_id=int(MONITOR_CHAT_ID), text=f"👤 id {uid}\n🤖 оператор {login}: {text}")
        except Exception as e:
            log.warning("mirror: %s", e)


@app.post("/api/dialogs/{uid}/reply")
async def dialog_reply(request: Request, uid: int, text: str = Form("")):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    text = text.strip()
    if not text: return {"ok": False, "error": "Пустой ответ"}
    if not await qv("SELECT 1 FROM kuzya_bot_users WHERE user_id=$1", uid):
        return {"ok": False, "error": "Нет такого пользователя бота"}
    mid, delivery = None, None
    try:
        mid = await maxbot.send(user_id=uid, text=text)
    except Exception as e:
        delivery = f"failed: {str(e)[:150]}"       # заблокировал бота и т.п. — сохраняем, оператор видит
    await qx("INSERT INTO kuzya_messages(user_id, direction, kind, text, max_mid, operator, delivery) VALUES ($1,'out','operator',$2,$3,$4,$5)",
             uid, text, mid, u["login"], delivery)
    if not delivery:
        await qx("UPDATE kuzya_dialogs SET last_at=NOW(), last_out_at=NOW(), unanswered=0 WHERE user_id=$1", uid)
        asyncio.create_task(mirror_operator(uid, u["login"], text))
    await log_action(u["login"], "reply", f"uid {uid}: {'ок' if not delivery else delivery}", client_ip(request))
    return {"ok": not delivery, "error": delivery and "Не доставлено: " + delivery}


@app.post("/api/messages/{mid_id}/edit")
async def message_edit(request: Request, mid_id: int, text: str = Form("")):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    m = await q1("SELECT * FROM kuzya_messages WHERE id=$1 AND kind='operator'", mid_id)
    if not m or not m["max_mid"]: return {"ok": False, "error": "Это сообщение нельзя править"}
    if m["operator"] != u["login"] and not is_admin(u): return {"ok": False, "error": "Править можно только свои ответы"}
    text = text.strip()
    if not text: return {"ok": False, "error": "Пустой текст"}
    try:
        await maxbot.edit(m["max_mid"], text)
    except Exception as e:
        return {"ok": False, "error": f"Max не принял правку: {str(e)[:150]}"}
    await qx("UPDATE kuzya_messages SET text=$1, edited_at=NOW() WHERE id=$2", text, mid_id)
    await log_action(u["login"], "edit_reply", f"msg {mid_id}", client_ip(request))
    return {"ok": True}


@app.post("/api/dialogs/{uid}/done")
async def dialog_done(request: Request, uid: int):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    await qx("UPDATE kuzya_dialogs SET unanswered=0 WHERE user_id=$1", uid)
    await log_action(u["login"], "dialog_done", f"uid {uid}", client_ip(request))
    return {"ok": True}


@app.get("/api/products")
async def products(request: Request):
    """Поиск товара для вставки ссылки /p/<код> в ответ."""
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    s = request.query_params.get("q", "").strip()
    if len(s) < 2: return {"ok": True, "items": []}
    rows = await q("""SELECT code, title FROM kuzya_products WHERE in_stock AND (title ILIKE $1 OR code = $2)
                      ORDER BY title LIMIT 15""", f"%{s}%", s)
    return {"ok": True, "items": [{"code": r["code"], "title": r["title"], "url": f"{VITRINA_URL}/p/{r['code']}"} for r in rows]}


# ═══════════════════════════════ медиа (рассылки и посты) ═══════════════════════════════

async def save_media(files):
    """До 5 фото ИЛИ одно видео. Файлы по sha256 содержимого: тот же файл → тот же путь (и тот же хеш рассылки)."""
    files = [f for f in (files or []) if f is not None and f.filename]
    if not files: return [], None
    vids = [f for f in files if maxbot.is_video(f.filename)]
    if vids and len(files) > 1: return None, "Видео — только одно и без фото"
    if len(files) > MAX_PHOTOS: return None, f"Не больше {MAX_PHOTOS} фото"
    UPLOADS.mkdir(parents=True, exist_ok=True)
    out = []
    for f in files:
        ext = os.path.splitext(f.filename)[1].lower()
        if ext not in (".jpg", ".jpeg", ".png", ".webp", ".gif") + maxbot.VIDEO_EXT:
            return None, f"Неподдерживаемый файл: {f.filename}"
        data = await f.read()
        lim = (MAX_VIDEO_MB if maxbot.is_video(f.filename) else MAX_PHOTO_MB) * 1024 * 1024
        if len(data) > lim: return None, f"{f.filename}: больше {lim // 1048576} МБ"
        p = UPLOADS / (hashlib.sha256(data).hexdigest()[:32] + ext)
        if not p.exists(): p.write_bytes(data)
        out.append(str(p))
    return out, None


async def upload_all(paths):
    atts = []
    for p in paths or []:
        atts.append(await maxbot.upload(p))
    return atts


# ═══════════════════════════════ рассылки ═══════════════════════════════

async def segment_uids(audience):
    if audience == "card2":
        rows = await q("""SELECT DISTINCT b.user_id FROM kuzya_loyalty_bindings b JOIN kuzya_bot_users u USING (user_id)
                          JOIN kuzya_loyalty_cards c ON right(c.phone,10) = right(regexp_replace(b.phone,'\\D','','g'),10)
                          WHERE c.card_type = 2""")
    else:
        rows = await q("SELECT user_id FROM kuzya_bot_users ORDER BY user_id")
    return [r["user_id"] for r in rows]


def content_hash(text, media, btn_text, btn_url, audience):
    return hashlib.sha256(json.dumps([text, media, btn_text, btn_url, audience], ensure_ascii=False).encode()).hexdigest()[:24]


def bcast_keyboard(btn_text, btn_url):
    if not (btn_text or "").strip(): return None
    return maxbot.link_keyboard([[(btn_text.strip()[:60], (btn_url or "").strip() or VITRINA_URL)]])


@app.get("/broadcasts", response_class=HTMLResponse)
async def broadcasts_page(request: Request):
    u = await current_user(request)
    if not u: return need_login()
    n_all = await qv("SELECT COUNT(*) FROM kuzya_bot_users")
    card2_total = await qv("SELECT COUNT(*) FROM kuzya_loyalty_cards WHERE card_type=2")
    card2_bound = len(await segment_uids("card2"))
    hist = await q("SELECT * FROM kuzya_broadcasts ORDER BY id DESC LIMIT 40")
    return tpl.TemplateResponse("broadcasts.html", ctx(request, u, "broadcasts", n_all=n_all, card2_total=card2_total,
        card2_bound=card2_bound, hist=hist, enabled=BROADCAST_ENABLED, delay=BROADCAST_DELAY, vitrina=VITRINA_URL))


async def _bcast_form(text, btn_text, btn_url, audience, files):
    media, err = await save_media(files)
    if err: return None, err
    text = (text or "").strip()
    if not text and not media: return None, "Пустая рассылка: добавьте текст, фото или видео"
    audience = audience if audience in ("all", "card2") else "all"
    return {"text": text, "media": media, "btn_text": btn_text.strip() or None, "btn_url": btn_url.strip() or None,
            "audience": audience, "hash": content_hash(text, media, btn_text.strip(), btn_url.strip(), audience)}, None


@app.post("/api/broadcasts/{step}")
async def broadcasts_step(request: Request, step: str, text: str = Form(""), btn_text: str = Form(""), btn_url: str = Form(""),
                          audience: str = Form("all"), confirm_total: str = Form(""), files: list[UploadFile] = File([])):
    """step: test → себе (max_uid пользователя); dry → холостой прогон (никому не шлёт); send → боевая."""
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    if step not in ("test", "dry", "send"): return Response(status_code=404)
    f, err = await _bcast_form(text, btn_text, btn_url, audience, files)
    if err: return {"ok": False, "error": err}
    ip = client_ip(request)
    if step == "test":
        if not u["max_uid"]:
            return {"ok": False, "error": "Не задан ваш Max uid — администратор заполняет его в «Пользователях»"}
        try:
            atts = await upload_all(f["media"])
            kb = bcast_keyboard(f["btn_text"], f["btn_url"])
            await maxbot.send(user_id=u["max_uid"], text=f["text"], attachments=atts + ([kb] if kb else []))
        except Exception as e:
            return {"ok": False, "error": f"Тест не ушёл: {str(e)[:150]}"}
        await qx("""INSERT INTO kuzya_broadcasts(kind, text, media, btn_text, btn_url, audience, content_hash, total, sent, status,
                    created_by, finished_at) VALUES ('test',$1,$2,$3,$4,$5,$6,1,1,'done',$7,NOW())""",
                 f["text"], f["media"], f["btn_text"], f["btn_url"], f["audience"], f["hash"], u["login"])
        await log_action(u["login"], "broadcast_test", f["hash"], ip)
        return {"ok": True, "message": "Тест отправлен вам в Max. Проверьте вид сообщения, затем — холостой прогон."}

    uids = await segment_uids(f["audience"])
    if not uids: return {"ok": False, "error": "В сегменте нет получателей"}
    if step == "dry":
        if not await qv("""SELECT 1 FROM kuzya_broadcasts WHERE kind='test' AND content_hash=$1
                           AND created_at > NOW() - interval '1 hour'""", f["hash"]):
            return {"ok": False, "error": "Сначала «Тест себе» с этим же содержимым"}
        await qx("""INSERT INTO kuzya_broadcasts(kind, text, media, btn_text, btn_url, audience, content_hash, total, sent, status,
                    created_by, finished_at) VALUES ('dry',$1,$2,$3,$4,$5,$6,$7,0,'dry_done',$8,NOW())""",
                 f["text"], f["media"], f["btn_text"], f["btn_url"], f["audience"], f["hash"], len(uids), u["login"])
        await log_action(u["login"], "broadcast_dry", f"{f['hash']}: {len(uids)} получателей", ip)
        mins = round(len(uids) * BROADCAST_DELAY / 60, 1)
        return {"ok": True, "total": len(uids), "message": f"Холостой прогон: получат {len(uids)} чел., отправка займёт ~{mins} мин. "
                                                            "Никому ничего не отправлено."}
    # send
    if not is_admin(u): return {"ok": False, "error": "Боевую рассылку запускает администратор"}
    if not BROADCAST_ENABLED: return {"ok": False, "error": "Боевые рассылки выключены (BROADCAST_ENABLED). Включаются по решению Вадима."}
    if not await qv("""SELECT 1 FROM kuzya_broadcasts WHERE kind='dry' AND content_hash=$1 AND total=$2
                       AND created_at > NOW() - interval '1 hour'""", f["hash"], len(uids)):
        return {"ok": False, "error": "Нужен холостой прогон этого же содержимого за последний час (и число получателей не изменилось)"}
    if confirm_total.strip() != str(len(uids)):
        return {"ok": False, "error": f"Для подтверждения введите число получателей: {len(uids)}"}
    if await qv("SELECT 1 FROM kuzya_broadcasts WHERE kind='send' AND status='running'"):
        return {"ok": False, "error": "Другая рассылка ещё идёт"}
    bid = await qv("""INSERT INTO kuzya_broadcasts(kind, text, media, btn_text, btn_url, audience, content_hash, total, status, created_by)
                      VALUES ('send',$1,$2,$3,$4,$5,$6,$7,'running',$8) RETURNING id""",
                   f["text"], f["media"], f["btn_text"], f["btn_url"], f["audience"], f["hash"], len(uids), u["login"])
    await log_action(u["login"], "broadcast_send", f"#{bid} {f['hash']}: {len(uids)} получателей", ip)
    asyncio.create_task(run_broadcast(bid, uids, f))
    return {"ok": True, "id": bid, "total": len(uids), "message": f"Рассылка #{bid} запущена"}


async def run_broadcast(bid, uids, f):
    sent = failed = 0
    errors = {}
    try:
        atts = await upload_all(f["media"])            # файлы в Max — один раз на всю рассылку
        kb = bcast_keyboard(f["btn_text"], f["btn_url"])
        atts = atts + ([kb] if kb else [])
        for i, uid in enumerate(uids):
            try:
                mid = await maxbot.send(user_id=uid, text=f["text"], attachments=atts)
                sent += 1
                await qx("INSERT INTO kuzya_messages(user_id, direction, kind, text, max_mid, payload) VALUES ($1,'out','broadcast',$2,$3,$4)",
                         uid, f["text"], mid, f"bcast#{bid}")
            except Exception as e:
                failed += 1
                k = str(e)[:80]
                errors[k] = errors.get(k, 0) + 1
            if (i + 1) % 10 == 0:
                await qx("UPDATE kuzya_broadcasts SET sent=$1, failed=$2 WHERE id=$3", sent, failed, bid)
            await asyncio.sleep(BROADCAST_DELAY)
        status = "done"
    except Exception as e:
        errors[f"остановлено: {str(e)[:100]}"] = 1
        status = "failed"
    await qx("UPDATE kuzya_broadcasts SET sent=$1, failed=$2, status=$3, finished_at=NOW(), result=$4 WHERE id=$5",
             sent, failed, status, {"errors": errors}, bid)


@app.get("/api/broadcasts/status/{bid}")
async def broadcasts_status(request: Request, bid: int):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    r = await q1("SELECT status, total, sent, failed FROM kuzya_broadcasts WHERE id=$1", bid)
    return {"ok": bool(r), **(dict(r) if r else {})}


# ═══════════════════════════════ посты в группы ═══════════════════════════════

def post_tag(number):
    return f"g{number}_{datetime.now(timezone(timedelta(hours=3))):%m%d}"     # метка по МСК, как у прежнего бота


def post_keyboard(tag):
    return maxbot.link_keyboard([[("🛍 Каталог товаров", f"https://max.ru/{BOT_USERNAME}?startapp={tag}")]])


async def add_chat(ref, by, source="manual", kind="shop"):
    info = await maxbot.chat_info(ref)
    await qx("""INSERT INTO kuzya_bot_chats(chat_id, title, kind, source, participants, bot_admin, added_by, checked_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,NOW()) ON CONFLICT (chat_id) DO UPDATE SET title=EXCLUDED.title,
                participants=EXCLUDED.participants, bot_admin=EXCLUDED.bot_admin, checked_at=NOW(), active=TRUE""",
             info["chat_id"], info["title"], kind, source, info["participants"], info["bot_admin"], by)
    return info


async def refresh_new_chats():
    """Группы, которые приёмник бота записал по событиям без названия, — дотянуть название и участников."""
    for r in await q("SELECT chat_id FROM kuzya_bot_chats WHERE title IS NULL LIMIT 20"):
        try:
            info = await maxbot.chat_info(str(r["chat_id"]))
            await qx("UPDATE kuzya_bot_chats SET title=$1, participants=$2, bot_admin=$3, checked_at=NOW() WHERE chat_id=$4",
                     info["title"], info["participants"], info["bot_admin"], r["chat_id"])
        except Exception as e:
            await qx("UPDATE kuzya_bot_chats SET title=$1, active=FALSE, checked_at=NOW() WHERE chat_id=$2",
                     f"(недоступна: {str(e)[:60]})", r["chat_id"])


@app.get("/posts", response_class=HTMLResponse)
async def posts_page(request: Request):
    u = await current_user(request)
    if not u: return need_login()
    await refresh_new_chats()
    chats = await q("SELECT * FROM kuzya_bot_chats ORDER BY active DESC, kind, title")
    hist = await q("SELECT * FROM kuzya_scheduled_posts ORDER BY id DESC LIMIT 40")
    now = datetime.now(OMSK).strftime("%Y-%m-%dT%H:%M")
    return tpl.TemplateResponse("posts.html", ctx(request, u, "posts", chats=chats, hist=hist, now=now))


@app.post("/posts/chats/add")
async def posts_chat_add(request: Request, ref: str = Form(""), kind: str = Form("shop")):
    u = await current_user(request)
    if not u: return need_login()
    try:
        info = await add_chat(ref, u["login"], "manual", kind if kind in ("shop", "service") else "shop")
        await log_action(u["login"], "chat_add", info["title"], client_ip(request))
    except Exception as e:
        return tpl.TemplateResponse("msg.html", ctx(request, u, "posts", title="Группа не добавлена",
            text=f"Max ответил: {str(e)[:200]}. Бот должен состоять в группе.", back="/posts"))
    return RedirectResponse("/posts", status_code=303)


@app.post("/posts/chats/{chat_id}/toggle")
async def posts_chat_toggle(request: Request, chat_id: int):
    u = await current_user(request)
    if not u: return need_login()
    await qx("UPDATE kuzya_bot_chats SET active = NOT active WHERE chat_id=$1", chat_id)
    await log_action(u["login"], "chat_toggle", str(chat_id), client_ip(request))
    return RedirectResponse("/posts", status_code=303)


async def publish(post_id, text, media, groups):
    atts = await upload_all(media)
    results = []
    for g in groups:
        tag = post_tag(g["number"])
        try:
            await maxbot.send(chat_id=g["chat_id"], text=text, attachments=atts + [post_keyboard(tag)])
            await qx("INSERT INTO kuzya_post_tags(tag, chat_id, title, post_id) VALUES ($1,$2,$3,$4) ON CONFLICT (tag) DO UPDATE "
                     "SET post_id=EXCLUDED.post_id", tag, g["chat_id"], g["title"], post_id)
            results.append({"title": g["title"], "ok": True})
        except Exception as e:
            results.append({"title": g["title"], "ok": False, "error": str(e)[:150]})
        await asyncio.sleep(0.4)
    ok = sum(r["ok"] for r in results)
    await qx("UPDATE kuzya_scheduled_posts SET status=$1, published_at=NOW(), result=$2 WHERE id=$3",
             "published" if ok else "failed", results, post_id)
    return results


@app.post("/api/posts/publish")
async def posts_publish(request: Request, text: str = Form(""), groups: list[str] = Form([]), when: str = Form("now"),
                        run_at: str = Form(""), files: list[UploadFile] = File([])):
    u = await current_user(request)
    if not u: return JSONResponse({"ok": False}, status_code=401)
    media, err = await save_media(files)
    if err: return {"ok": False, "error": err}
    text = text.strip()
    if not text and not media: return {"ok": False, "error": "Пустой пост"}
    ids = [int(g) for g in groups if re.fullmatch(r"-?\d+", g)]
    targets = [dict(r) for r in await q("SELECT chat_id, title, number FROM kuzya_bot_chats WHERE active AND chat_id = ANY($1)", ids)]
    if not targets: return {"ok": False, "error": "Не выбрана ни одна группа"}
    run_utc = None
    if when == "schedule":
        try: run_utc = datetime.strptime(run_at, "%Y-%m-%dT%H:%M").replace(tzinfo=OMSK).astimezone(timezone.utc)
        except ValueError: return {"ok": False, "error": "Неверные дата и время"}
        if run_utc < datetime.now(timezone.utc): return {"ok": False, "error": "Время уже прошло"}
    pid = await qv("INSERT INTO kuzya_scheduled_posts(text, media, groups, run_at, status, created_by) VALUES ($1,$2,$3,$4,$5,$6) RETURNING id",
                   text, media, targets, run_utc or datetime.now(timezone.utc), "queued" if run_utc else "running", u["login"])
    await log_action(u["login"], "post_" + ("schedule" if run_utc else "publish"), f"#{pid}: {len(targets)} групп", client_ip(request))
    if run_utc:
        return {"ok": True, "message": f"Пост #{pid} запланирован на {fmt(run_utc)} (Омск), групп: {len(targets)}"}
    res = await publish(pid, text, media, targets)
    ok = sum(r["ok"] for r in res)
    return {"ok": ok > 0, "message": f"Опубликовано: {ok} из {len(res)}", "results": res}


async def scheduler_loop():
    await asyncio.sleep(10)
    while True:
        try:
            for r in await q("""UPDATE kuzya_scheduled_posts SET status='running' WHERE id IN (SELECT id FROM kuzya_scheduled_posts
                                WHERE status='queued' AND run_at <= NOW() ORDER BY run_at LIMIT 3) RETURNING *"""):
                await publish(r["id"], r["text"] or "", r["media"] or [], r["groups"] or [])
        except Exception as e:
            log.warning("scheduler: %s", e)
        await asyncio.sleep(30)


# ═══════════════════════════════ аналитика ═══════════════════════════════

@app.get("/analytics", response_class=HTMLResponse)
async def analytics(request: Request):
    u = await current_user(request)
    if not u: return need_login()
    t0 = datetime.combine(SINCE, datetime.min.time(), tzinfo=OMSK)
    days = await q("""
        WITH d AS (SELECT generate_series($1::date, (NOW() AT TIME ZONE 'Asia/Omsk')::date, '1 day')::date AS d)
        SELECT d.d,
          (SELECT COUNT(*) FROM kuzya_bot_users WHERE (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d
             AND COALESCE(source,'') <> 'recovered') AS subs,
          (SELECT COUNT(*) FROM kuzya_messages WHERE kind='start' AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS starts,
          (SELECT COUNT(*) FROM kuzya_messages WHERE kind='text' AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS questions,
          (SELECT COUNT(DISTINCT COALESCE(user_id::text, id::text)) FROM kuzya_vitrina_entries
             WHERE (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS vitrina,
          (SELECT COUNT(*) FROM kuzya_orders_log WHERE NOT is_test AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS orders,
          (SELECT COALESCE(SUM(total),0) FROM kuzya_orders_log WHERE NOT is_test AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS orders_sum,
          (SELECT COUNT(*) FROM kuzya_orders_log WHERE NOT is_test AND pay_status='paid' AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS paid,
          (SELECT COALESCE(SUM(total),0) FROM kuzya_orders_log WHERE NOT is_test AND pay_status='paid' AND (created_at AT TIME ZONE 'Asia/Omsk')::date = d.d) AS paid_sum
        FROM d ORDER BY d.d DESC""", SINCE)
    base = await q1("""SELECT COUNT(*) total, COUNT(*) FILTER (WHERE source='recovered') recovered,
                              COUNT(*) FILTER (WHERE created_at >= $1 AND COALESCE(source,'') <> 'recovered') new_since
                       FROM kuzya_bot_users""", t0)
    shops = await q("""SELECT COALESCE(s.city || ', ' || s.address, o.shop_id::text) shop, COUNT(*) n, SUM(o.total) s,
                              COUNT(*) FILTER (WHERE o.pay_status='paid') paid
                       FROM kuzya_orders_log o LEFT JOIN kuzya_shops s ON s.id=o.shop_id WHERE NOT o.is_test GROUP BY 1 ORDER BY 2 DESC""")
    sources = await q("""SELECT COALESCE(source,'(без метки)') src, COUNT(*) n FROM kuzya_bot_users
                         WHERE COALESCE(source,'') <> 'recovered' AND created_at >= $1 GROUP BY 1 ORDER BY 2 DESC LIMIT 30""", t0)
    tests = await qv("SELECT COUNT(*) FROM kuzya_orders_log WHERE is_test")
    return tpl.TemplateResponse("analytics.html", ctx(request, u, "analytics", days=days, base=base, shops=shops,
                                                      sources=sources, tests=tests, since=SINCE))


# ═══════════════════════════════ запуск ═══════════════════════════════

@app.on_event("startup")
async def startup():
    async with (await pool()).acquire() as c:
        await c.execute(SCHEMA)
        for login, (role, name) in USERS.items():
            await c.execute("INSERT INTO kuzya_admin_users(login, role, name) VALUES ($1,$2,$3) ON CONFLICT (login) DO UPDATE "
                            "SET role=EXCLUDED.role", login, role, name)
    asyncio.create_task(scheduler_loop())


async def _cli(argv):
    """python -m app.main invite <login…|all-unset>   — ссылки установки пароля в Max ADMIN_NOTIFY_UID
       python -m app.main seed-chats <файл id> <kind>  — группы по id (файл вне git)"""
    await startup()
    if argv[0] == "invite":
        logins = argv[1:]
        if logins == ["all-unset"]:
            logins = [r["login"] for r in await q("SELECT login FROM kuzya_admin_users WHERE pass_hash IS NULL AND active ORDER BY role, login")]
        pairs = [(lg, await make_invite(lg, "cli")) for lg in logins if lg in USERS]
        await deliver_invites(pairs)
        for lg, _ in pairs:
            await log_action("cli", "invite", lg)
        print("ссылок отправлено:", len(pairs))
    elif argv[0] == "seed-chats":
        kind = argv[2] if len(argv) > 2 else "shop"
        for line in open(argv[1]):
            ref = line.split()[0] if line.strip() else ""
            if not ref: continue
            try:
                info = await add_chat(ref, "seed", "seed", kind)
                print("ok", info["title"])
            except Exception as e:
                print("fail", str(e)[:80])


if __name__ == "__main__":
    asyncio.run(_cli(sys.argv[1:]))
