"""Тонкий клиент Max Bot API для админки: только platform-api.max.ru и токен заголовком (как kuzya-web).

Вместо библиотеки maxapi прежней админки: её адрес API и способ передачи токена на moscow не проверены,
а нам нужны четыре вызова. Файл грузится в Max ОДИН раз — токен вложения переиспользуется для всех адресатов.
"""
import asyncio
import os
import re

import httpx

TOKEN = os.environ.get("MAX_KUZYA_BOT_TOKEN", "")
API = os.environ.get("MAX_API_URL", "https://platform-api.max.ru")
VIDEO_EXT = (".mp4", ".mov", ".mkv", ".webm")


class MaxError(Exception):
    pass


def _client(timeout=30):
    return httpx.AsyncClient(base_url=API, headers={"Authorization": TOKEN}, timeout=timeout)


async def _call(method, path, *, params=None, json=None):
    async with _client() as c:
        r = await c.request(method, path, params=params, json=json)
    if r.status_code >= 400:
        try: d = r.json()
        except Exception: d = {}
        raise MaxError(f"{r.status_code} {d.get('code') or ''} {d.get('message') or r.text[:120]}".strip())
    return r.json() if r.content else {}


def is_video(path):
    return bool(path) and os.path.splitext(path)[1].lower() in VIDEO_EXT


async def upload(path):
    """Файл → вложение {"type": image|video, "payload": {"token": …}}."""
    kind = "video" if is_video(path) else "image"
    slot = await _call("POST", "/uploads", params={"type": kind})
    async with httpx.AsyncClient(timeout=300) as c:
        with open(path, "rb") as f:
            r = await c.post(slot["url"], files={"data": (os.path.basename(path), f)})
    if r.status_code >= 400:
        raise MaxError(f"upload {r.status_code}")
    if kind == "video":
        token = slot.get("token")
    else:
        d = r.json()
        token = d.get("token") or next(iter((d.get("photos") or {}).values()), {}).get("token")
    if not token:
        raise MaxError("upload: нет токена вложения")
    return {"type": kind, "payload": {"token": token}}


def link_keyboard(rows):
    """rows: [[(текст, url), …], …] → вложение inline_keyboard."""
    return {"type": "inline_keyboard",
            "payload": {"buttons": [[{"type": "link", "text": t, "url": u} for t, u in row] for row in rows]}}


async def send(*, user_id=None, chat_id=None, text="", attachments=None):
    """→ mid. Видео Max обрабатывает не сразу: attachment.not.ready — повторяем с паузой."""
    params = {"user_id": int(user_id)} if user_id else {"chat_id": int(chat_id)}
    body = {"text": text[:4000]}
    if attachments:
        body["attachments"] = attachments
    for attempt in range(6):
        try:
            d = await _call("POST", "/messages", params=params, json=body)
            return ((d.get("message") or {}).get("body") or {}).get("mid")
        except MaxError as e:
            if "not.ready" in str(e) and attempt < 5:
                await asyncio.sleep(2 + attempt * 2)
                continue
            raise


async def edit(mid, text, attachments=None):
    body = {"text": text[:4000]}
    if attachments is not None:
        body["attachments"] = attachments
    await _call("PUT", "/messages", params={"message_id": mid}, json=body)


async def chat_info(ref):
    """id (число) или ссылка max.ru/… → {chat_id, title, participants_count, type}. Бот должен состоять в чате."""
    ref = (ref or "").strip()
    if re.fullmatch(r"-?\d+", ref):
        d = await _call("GET", f"/chats/{int(ref)}")
    else:
        m = re.search(r"max\.ru/(.+)$", ref)
        if not m:
            raise MaxError("нужен id группы или ссылка max.ru/…")
        d = await _call("GET", f"/chats/{m.group(1).strip('/')}")
    me = await _call("GET", f"/chats/{d['chat_id']}/members/me")
    return {"chat_id": d["chat_id"], "title": d.get("title") or str(d["chat_id"]),
            "participants": d.get("participants_count"), "type": d.get("type"), "status": d.get("status"),
            "bot_admin": bool(me.get("is_admin"))}
