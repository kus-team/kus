"""KUS API — Kill Unlawful Schemes. FastAPI + Jinja templates + static."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from backend.db.connection import connect
from backend.services.graph import build_network
from backend.services.narrative import explain_tender

APP_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=APP_DIR / "templates")

RISK_RED = 70
RISK_YELLOW = 30


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ensure schema exists; safe re-run
    try:
        from backend.db.connection import apply_schema, dialect
        apply_schema()
        print(f"[+] DB ready ({dialect()})")
    except Exception as e:
        print(f"[!] DB init failed: {e}")
    yield


app = FastAPI(title="KUS — Kill Unlawful Schemes", version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)
app.mount("/static", StaticFiles(directory=APP_DIR / "static"), name="static")


# Запрет кэширования HTML и API-ответов — чтобы изменения подхватывались сразу при reload.
@app.middleware("http")
async def no_cache(request: Request, call_next):
    resp = await call_next(request)
    if request.url.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "public, max-age=60"
    else:
        resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
    return resp


# ------------------------------------------------------------------ pages

@app.get("/", response_class=HTMLResponse)
def page_dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/tenders", response_class=HTMLResponse)
def page_tenders(request: Request):
    return templates.TemplateResponse("tenders.html", {"request": request})


@app.get("/tenders/{tender_id}", response_class=HTMLResponse)
def page_tender_detail(request: Request, tender_id: int):
    return templates.TemplateResponse("check.html", {"request": request, "prefill_id": tender_id})


@app.get("/companies", response_class=HTMLResponse)
def page_companies(request: Request):
    return templates.TemplateResponse("companies.html", {"request": request})


@app.get("/companies/{tin}", response_class=HTMLResponse)
def page_company(request: Request, tin: str):
    return templates.TemplateResponse("company_profile.html", {"request": request, "tin": tin})


@app.get("/check", response_class=HTMLResponse)
def page_check(request: Request):
    return templates.TemplateResponse("check.html", {"request": request})


@app.get("/graph", response_class=HTMLResponse)
def page_graph(request: Request):
    return templates.TemplateResponse("graph.html", {"request": request})


def _decode_json_fields(row: dict) -> dict:
    """risk_flags / raw в SQLite приходят как TEXT; в Postgres — уже dict. Унифицируем в dict."""
    if not row:
        return row
    import json as _json
    for k in ("risk_flags", "raw"):
        v = row.get(k)
        if isinstance(v, str):
            try:
                row[k] = _json.loads(v)
            except Exception:
                pass
    return row


def fetch_all(sql: str, params: tuple | dict = ()) -> list[dict]:
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        return [_decode_json_fields(r) for r in cur.fetchall()]
    finally:
        con.close()


def fetch_one(sql: str, params: tuple | dict = ()) -> dict | None:
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return _decode_json_fields(dict(row)) if row else None
    finally:
        con.close()


# ------------------------------------------------------------------ health

@app.get("/api/health")
def health() -> dict[str, Any]:
    row = fetch_one("SELECT COUNT(*) AS n FROM tenders")
    return {"ok": True, "tenders": (row or {}).get("n", 0)}


# ------------------------------------------------------------------ tenders

@app.get("/api/tenders")
def list_tenders(
    q: str | None = Query(None, description="поиск по title (ILIKE)"),
    min_risk: int = Query(0, ge=0, le=100),
    max_risk: int = Query(100, ge=0, le=100),
    category: str | None = None,
    customer_tin: str | None = None,
    winner_tin: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    order: str = Query("risk_desc",
                       pattern="^(risk_desc|amount_desc|date_desc|date_asc)$"),
) -> dict[str, Any]:
    where = ["1=1"]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if q:
        # SQLite не знает ILIKE; LIKE по умолчанию case-insensitive для ASCII, для UTF-8 — нет.
        # Postgres ILIKE → COLLATE NOCASE через LIKE LOWER(). Унифицируем через LOWER().
        where.append("(LOWER(title) LIKE LOWER(%(q)s) OR lot_id = %(q_exact)s OR contract_id = %(q_exact)s)")
        params["q"] = f"%{q}%"
        params["q_exact"] = q
    if min_risk:
        where.append("risk_score >= %(min_risk)s")
        params["min_risk"] = min_risk
    if max_risk < 100:
        where.append("risk_score <= %(max_risk)s")
        params["max_risk"] = max_risk
    if category:
        where.append("category = %(category)s")
        params["category"] = category
    if customer_tin:
        where.append("customer_tin = %(customer_tin)s")
        params["customer_tin"] = customer_tin
    if winner_tin:
        where.append("winner_tin = %(winner_tin)s")
        params["winner_tin"] = winner_tin

    # SQLite не знает NULLS LAST — но "DESC" по умолчанию ставит NULL в конец, "ASC" — в начало; оба ок для нашего UX.
    order_sql = {
        "risk_desc":   "risk_score DESC, amount_uzs DESC",
        "amount_desc": "amount_uzs DESC",
        "date_desc":   "date DESC",
        "date_asc":    "date ASC",
    }[order]

    sql_data = f"""
        SELECT id, source_dataset, lot_id, contract_id, title,
               customer_tin, customer_name, winner_tin, winner_name,
               amount_uzs, amount_usd, date, category,
               is_direct_purchase, risk_score, risk_flags
        FROM tenders
        WHERE {' AND '.join(where)}
        ORDER BY {order_sql}
        LIMIT %(limit)s OFFSET %(offset)s
    """
    sql_count = f"SELECT COUNT(*) AS n FROM tenders WHERE {' AND '.join(where)}"
    return {
        "data":   fetch_all(sql_data, params),
        "total":  (fetch_one(sql_count, params) or {}).get("n", 0),
        "limit":  limit,
        "offset": offset,
    }


@app.get("/api/tenders/suspicious")
def suspicious_tenders(limit: int = Query(100, ge=1, le=500)) -> dict[str, Any]:
    return {"data": fetch_all(
        "SELECT id, title, customer_name, winner_name, amount_uzs, amount_usd, "
        "       date, category, risk_score, risk_flags "
        "FROM tenders WHERE risk_score >= %s "
        "ORDER BY risk_score DESC, amount_uzs DESC NULLS LAST LIMIT %s",
        (RISK_RED, limit),
    )}


@app.get("/api/tenders/{tender_id}")
def tender_detail(tender_id: int) -> dict[str, Any]:
    row = fetch_one("SELECT * FROM tenders WHERE id = %s", (tender_id,))
    if not row:
        raise HTTPException(404, "tender not found")
    return row


# ------------------------------------------------------------------ analytics

@app.get("/api/analytics/stats")
def stats() -> dict[str, Any]:
    row = fetch_one("""
        SELECT
            COUNT(*)                                                           AS total,
            SUM(CASE WHEN risk_score >= %(red)s THEN 1 ELSE 0 END)             AS red,
            SUM(CASE WHEN risk_score >= %(yel)s AND risk_score < %(red)s THEN 1 ELSE 0 END) AS yellow,
            SUM(CASE WHEN risk_score <  %(yel)s THEN 1 ELSE 0 END)             AS green,
            COALESCE(SUM(amount_uzs), 0)                                       AS total_uzs,
            COALESCE(SUM(amount_usd), 0)                                       AS total_usd,
            SUM(CASE WHEN is_direct_purchase THEN 1 ELSE 0 END) AS direct_purchases
        FROM tenders
    """, {"red": RISK_RED, "yel": RISK_YELLOW})
    return row or {}


@app.get("/api/analytics/by-category")
def by_category(limit: int = Query(30, ge=1, le=200)) -> dict[str, Any]:
    return {"data": fetch_all("""
        SELECT category,
               COUNT(*) AS n,
               ROUND(AVG(amount_uzs), 0) AS avg_uzs,
               SUM(CASE WHEN risk_score >= %(red)s THEN 1 ELSE 0 END) AS red
        FROM tenders
        WHERE category IS NOT NULL AND category <> ''
        GROUP BY category
        ORDER BY n DESC
        LIMIT %(lim)s
    """, {"red": RISK_RED, "lim": limit})}


@app.get("/api/analytics/top-risky-companies")
def top_risky_companies(limit: int = Query(20, ge=1, le=100)) -> dict[str, Any]:
    return {"data": fetch_all("""
        SELECT t.winner_tin                                              AS winner_tin,
               COALESCE(MAX(t.winner_name), MAX(od.name))                AS winner_name,
               COUNT(*)                                                  AS wins,
               SUM(CASE WHEN t.risk_score >= %(red)s THEN 1 ELSE 0 END)  AS red_wins,
               COALESCE(SUM(t.amount_uzs), 0)                            AS total_uzs,
               COALESCE(SUM(t.amount_usd), 0)                            AS total_usd,
               ROUND(AVG(t.risk_score), 1)                               AS avg_risk
        FROM tenders t
        LEFT JOIN org_directory od ON od.tin = t.winner_tin
        WHERE t.winner_tin IS NOT NULL
        GROUP BY t.winner_tin
        HAVING SUM(CASE WHEN t.risk_score >= %(red)s THEN 1 ELSE 0 END) > 0
        ORDER BY red_wins DESC, total_uzs DESC
        LIMIT %(lim)s
    """, {"red": RISK_RED, "lim": limit})}


# ------------------------------------------------------------------ company

@app.get("/api/company/{tin}")
def company_profile(tin: str) -> dict[str, Any]:
    summary = fetch_one("""
        SELECT t.winner_tin                                              AS winner_tin,
               COALESCE(MAX(t.winner_name), MAX(od.name))                AS name,
               COUNT(*)                                                  AS wins,
               SUM(CASE WHEN t.risk_score >= %(red)s THEN 1 ELSE 0 END)  AS red_wins,
               COALESCE(SUM(t.amount_uzs), 0)                            AS total_uzs,
               COALESCE(SUM(t.amount_usd), 0)                            AS total_usd,
               MIN(t.date) AS first_win, MAX(t.date) AS last_win
        FROM tenders t
        LEFT JOIN org_directory od ON od.tin = t.winner_tin
        WHERE t.winner_tin = %(tin)s
        GROUP BY t.winner_tin
    """, {"red": RISK_RED, "tin": tin})
    if not summary:
        raise HTTPException(404, "company not found")
    customers = fetch_all("""
        SELECT t.customer_tin                                  AS customer_tin,
               COALESCE(MAX(t.customer_name), MAX(od.name))    AS customer_name,
               COUNT(*)                                        AS wins,
               COALESCE(SUM(t.amount_uzs), 0)                  AS total_uzs
        FROM tenders t
        LEFT JOIN org_directory od ON od.tin = t.customer_tin
        WHERE t.winner_tin = %(tin)s
        GROUP BY t.customer_tin
        ORDER BY wins DESC
        LIMIT 20
    """, {"tin": tin})
    recent = fetch_all("""
        SELECT id, title, customer_tin, customer_name, amount_uzs, amount_usd, date, category, risk_score, risk_flags, is_direct_purchase
        FROM tenders WHERE winner_tin = %(tin)s
        ORDER BY date DESC LIMIT 50
    """, {"tin": tin})
    # Timeline: группировка побед по месяцу
    timeline = fetch_all("""
        SELECT TO_CHAR(date, 'YYYY-MM') AS bucket,
               COUNT(*)                        AS n,
               COALESCE(SUM(amount_uzs), 0)    AS uzs,
               SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS red
        FROM tenders
        WHERE winner_tin = %(tin)s AND date IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """, {"tin": tin})
    # Категории, в которых эта компания выигрывала
    categories = fetch_all("""
        SELECT category,
               COUNT(*) AS n,
               COALESCE(SUM(amount_uzs), 0) AS uzs
        FROM tenders WHERE winner_tin = %(tin)s AND category IS NOT NULL AND category <> ''
        GROUP BY category
        ORDER BY n DESC
        LIMIT 10
    """, {"tin": tin})
    return {"summary": summary, "customers": customers, "recent": recent,
            "timeline": timeline, "categories": categories}


# ------------------------------------------------------------------ AI narrative

@app.post("/api/tenders/{tender_id}/explain")
def tender_explain(tender_id: int, force: bool = Query(False, description="перегенерировать, игнорируя кеш")) -> dict[str, Any]:
    t = fetch_one("SELECT * FROM tenders WHERE id = %s", (tender_id,))
    if not t:
        raise HTTPException(404, "tender not found")

    if not force and t.get("ai_narrative"):
        return {"narrative": t["ai_narrative"], "cached": True, "generated_at": t.get("ai_narrative_at")}

    try:
        narrative = explain_tender(t)
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    except Exception as e:
        raise HTTPException(500, f"LLM error: {e}")

    con = connect()
    try:
        cur = con.cursor()
        cur.execute(
            "UPDATE tenders SET ai_narrative = %(n)s, ai_narrative_at = CURRENT_TIMESTAMP WHERE id = %(id)s",
            {"n": narrative, "id": tender_id},
        )
        con.commit()
    finally:
        con.close()
    return {"narrative": narrative, "cached": False}


# ------------------------------------------------------------------ Graph

@app.get("/api/graph/network")
def graph_network(min_wins: int = Query(2, ge=1, le=50), limit_pairs: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    return build_network(min_wins=min_wins, limit_pairs=limit_pairs)


# ------------------------------------------------------------------ Map (regions)

@app.get("/api/analytics/by-region")
def by_region() -> dict[str, Any]:
    """Агрегация по районам/городам.

    Sources:
      1. Поле `Tuman` в raw для датасета аукционов земли (6225c27ed31e97c0521ec8a1).
      2. Источник датасета (source_dataset) для остальных — все тендеры идут как
         «штаб» владеющего датасетом органа (Минэкономфин = Ташкент, Алмалык ГМК = Алмалык).
    """
    rows: list[dict] = []

    # 1) реальная гео-разбивка по Tuman (только аукционы земли)
    region_rows = fetch_all("""
        SELECT raw->>'Tuman' AS region,
               COUNT(*)                  AS n,
               COALESCE(SUM(amount_uzs), 0) AS total_uzs,
               COALESCE(SUM(amount_usd), 0) AS total_usd,
               ROUND(AVG(risk_score), 0) AS avg_risk,
               SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS red
        FROM tenders
        WHERE source_dataset = '6225c27ed31e97c0521ec8a1' AND raw->>'Tuman' IS NOT NULL
        GROUP BY raw->>'Tuman'
        ORDER BY n DESC
    """)
    for r in region_rows:
        rows.append({
            "key": r["region"],
            "label": r["region"],
            "scope": "district",
            "n": int(r["n"]),
            "total_uzs": float(r["total_uzs"] or 0),
            "total_usd": float(r["total_usd"] or 0),
            "avg_risk": int(r["avg_risk"] or 0),
            "red": int(r["red"] or 0),
        })

    # 2) для остальных — агрегируем по датасету и привязываем к штабу
    by_ds = fetch_all("""
        SELECT source_dataset AS ds,
               COUNT(*)                    AS n,
               COALESCE(SUM(amount_uzs), 0) AS total_uzs,
               COALESCE(SUM(amount_usd), 0) AS total_usd,
               ROUND(AVG(risk_score), 0)   AS avg_risk,
               SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) AS red
        FROM tenders
        WHERE source_dataset <> '6225c27ed31e97c0521ec8a1'
        GROUP BY source_dataset
    """)
    DS_TO_HQ = {
        "613eeda614665dbb8ec80453": "Tashkent",        # Минэкономфин
        "6142ef46ba3615f6f07bca0f": "Tashkent",
        "64dc981eb04a41cb2e29d57b": "Tashkent",
        "61137447db32b99538e086fc": "Almalyk",         # Алмалыкский ГМК
    }
    for r in by_ds:
        hq = DS_TO_HQ.get(r["ds"], "Tashkent")
        rows.append({
            "key": hq + "::" + r["ds"],
            "label": hq + " (" + r["ds"][:8] + "…)",
            "scope": "hq",
            "hq": hq,
            "n": int(r["n"]),
            "total_uzs": float(r["total_uzs"] or 0),
            "total_usd": float(r["total_usd"] or 0),
            "avg_risk": int(r["avg_risk"] or 0),
            "red": int(r["red"] or 0),
        })
    return {"data": rows}


@app.get("/map", response_class=HTMLResponse)
def page_map(request: Request):
    return templates.TemplateResponse("map.html", {"request": request})
