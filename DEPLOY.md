# Deploy KUS на Railway

## Что нужно (5 минут)

1. **GitHub-аккаунт** (если нет — https://github.com/signup, через email)
2. **Railway-аккаунт** — https://railway.app, кнопка «Login with GitHub»

## Шаги

### 1. Создать репо на GitHub (1 минута)

- https://github.com/new → имя `kus`, **Public** или Private (любое), **БЕЗ** README/LICENSE/.gitignore (у нас уже всё есть).
- На странице репо скопировать команды из секции **«…or push an existing repository from the command line»**.

### 2. Залить локальный код (1 минута)

```bash
cd D:\KUS
git branch -M main
git remote add origin https://github.com/<ТВОЙ-USERNAME>/kus.git
git push -u origin main
```

### 3. Создать проект на Railway (3 минуты)

- https://railway.app/new → **Deploy from GitHub repo** → выбрать `kus`.
- Railway автоматически найдёт `Procfile`, `requirements.txt`, `nixpacks.toml` и начнёт сборку.
- В проекте нажать **«+ New»** → **«Database»** → **PostgreSQL** → создаётся БД, `DATABASE_URL` автоматически становится доступен в env.
- В сервисе Web (не БД) → **Variables** → добавить:
  - `ANTHROPIC_API_KEY` = `sk-ant-...` (для AI-narrative; можно пропустить — без него только кнопка «Объяснить» не работает)
  - `UZS_PER_USD` = `12500`
- В сервисе Web → **Settings** → **Networking** → **Generate Domain** → получишь публичный URL вида `kus-production.up.railway.app`.

### 4. Залить данные на Railway-Postgres (один раз, 30 секунд)

После деплоя Railway-Postgres пуст. Нужно загрузить тендеры. Локально:

```bash
# Установи Railway CLI: https://docs.railway.app/develop/cli
# Linux/Mac:  curl -fsSL https://railway.app/install.sh | sh
# Windows: scoop install railway   (или скачай .exe с GitHub)

railway login
railway link             # выбери проект kus
railway run python -m backend.ingest.loader
railway run python -m backend.ingest.risk
```

Это запустит наш loader **с переменной DATABASE_URL от Railway** — данные уйдут в облачный Postgres.

### 5. Готово

Открой URL который дал Railway — увидишь работающий KUS с реальными данными.

---

## Структура

- `Procfile` — команда запуска web-сервера
- `nixpacks.toml` — конфиг сборки (Python 3.13)
- `railway.json` — health-check, restart policy
- `runtime.txt` — версия Python
- `backend/requirements.txt` — depend
- `backend/.env.example` — шаблон переменных

## Schema apply при первом запуске

Сервер при старте сам применяет `schema.sql` (через lifespan). При первом коннекте к новой БД создаются все таблицы и индексы — никакие миграции вручную запускать не надо.

## Troubleshooting

- **Build fails** → смотри логи в Railway → обычно version mismatch в requirements; обнови pin
- **App crashes on startup** → проверь что DATABASE_URL установлен (Postgres-сервис подключён к Web)
- **Empty dashboard** → не запустил loader. См. шаг 4
- **AI narrative не работает** → нет ANTHROPIC_API_KEY в Variables
