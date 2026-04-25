# KUS — Kill Unlawful Schemes

Анти-коррупционная аналитика госзакупок Узбекистана. Source: `data.egov.uz`.

Проект для республиканского хакатона по борьбе с коррупцией (10-дневный спринт).

## Что внутри

- **Backend:** FastAPI + Jinja2 templates + psycopg 3 (Postgres) или sqlite3 (local).
- **Frontend:** server-rendered HTML + vanilla JS, тёмная control-room тема.
- **Анализ:** индекс риска коррупции 0–100 на трёх флагах (монополия / прямая закупка / завышенная цена).
- **AI:** «Объясни простым языком» через Anthropic Claude.
- **Граф связей:** vis.js network «заказчик ↔ победитель».

## Локальный запуск (с SQLite, без облака)

```bash
pip install -r backend/requirements.txt
python -m backend.ingest.loader     # ~2660 тендеров с egov.uz → SQLite
python -m backend.ingest.risk       # пересчитать risk_score
uvicorn backend.app.main:app --reload
# открыть http://localhost:8000
```

## Production (Postgres)

1. Создать Postgres на [Neon](https://console.neon.tech) (free tier).
2. Скопировать connection string в `backend/.env`:
   ```
   DATABASE_URL=postgresql://...
   ANTHROPIC_API_KEY=sk-ant-...
   ```
3. Запустить `python -m backend.ingest.loader && python -m backend.ingest.risk`.
4. Деплой через Railway / Render / Fly.io по `Procfile`.

## Архитектура

```
backend/
├── app/                # FastAPI
│   ├── main.py         # роуты + страницы
│   ├── templates/      # Jinja2: dashboard / tenders / companies / check / graph
│   └── static/css/     # тёмная тема
├── services/
│   ├── narrative.py    # Anthropic API: объяснение тендера
│   └── graph.py        # SQL-агрегация → vis.js nodes/edges
├── ingest/
│   ├── normalizer.py   # маппинг разных схем датасетов → unified
│   ├── loader.py       # fetch → normalize → UPSERT
│   └── risk.py         # расчёт risk_score
└── db/
    ├── schema.sql          # Postgres
    └── schema_sqlite.sql   # local fallback
```
