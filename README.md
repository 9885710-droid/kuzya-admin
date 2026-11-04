# kuzya-admin

Админка бота КУЗЯ (Max): диалоги, рассылки, посты в группы, аналитика. moscow, `/opt/kuzya-admin`, тестовый порт `127.0.0.1:8083`.
Таблицы — в общей БД `kuzya` (переписку пишет приёмник бота в kuzya-web). Подробности — в docstring `app/main.py`.

- Ссылки установки пароля (в Max `ADMIN_NOTIFY_UID`): `docker exec kuzya-admin python -m app.main invite <логин…>` или `invite all-unset`.
- Группы по id из файла вне git: `docker exec -i kuzya-admin python -m app.main seed-chats /dev/stdin shop < ids.txt`.
- Выкатка: `rsync` без `.env`/`uploads`, затем `docker compose build -q && docker compose up -d --force-recreate`.
- Заголовки Traefik `secure-headers@file`: `COMPOSE_FILE=docker-compose.yml:docker-compose.headers.yml` в `.env` (после того, как общий агент положил файл).
