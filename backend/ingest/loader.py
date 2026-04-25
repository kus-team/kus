"""
Loader: fetch → normalize → UPSERT в Postgres.

Использование:
    python -m backend.ingest.loader                 # грузит DEFAULT_DATASETS
    python -m backend.ingest.loader --ids A,B,C     # свой список structId
    python -m backend.ingest.loader --dry-run       # только показать статистику
    python -m backend.ingest.loader --init          # перед загрузкой применить schema.sql
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from typing import Any, Iterable

import requests

from backend.config import DATABASE_URL, EGOV_BASE, UZS_PER_USD, DEFAULT_DATASETS
from backend.ingest.normalizer import NormalizedTender, normalize_dataset

TIMEOUT = 60


def fetch_meta(struct_id: str) -> dict[str, Any]:
    r = requests.get(f"{EGOV_BASE}/apiClient/Main/GetById",
                     params={"id": struct_id}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json().get("result", {}) or {}


def fetch_data(struct_id: str) -> list[dict[str, Any]]:
    r = requests.get(
        f"{EGOV_BASE}/apiData/MainData/GetByFile",
        params={"id": struct_id, "fileType": 1, "tableType": 2},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def dedup_tenders(ts: list[NormalizedTender]) -> list[NormalizedTender]:
    """в одном датасете могут быть дубли по (source, lot_id, contract_id); оставляем последний."""
    seen: dict[tuple[str, str, str], NormalizedTender] = {}
    for t in ts:
        key = (t.source_dataset, t.lot_id or "", t.contract_id or "")
        seen[key] = t
    return list(seen.values())


UPSERT_SQL = """
INSERT INTO tenders (
    source_dataset, lot_id, contract_id, title,
    customer_tin, customer_name, winner_tin, winner_name,
    amount_uzs, amount_usd, currency_raw, date,
    category, funding_source, purchase_method, is_direct_purchase,
    raw
) VALUES (
    %(source_dataset)s, %(lot_id)s, %(contract_id)s, %(title)s,
    %(customer_tin)s, %(customer_name)s, %(winner_tin)s, %(winner_name)s,
    %(amount_uzs)s, %(amount_usd)s, %(currency_raw)s, %(date)s,
    %(category)s, %(funding_source)s, %(purchase_method)s, %(is_direct_purchase)s,
    %(raw)s
)
ON CONFLICT (source_dataset, lot_id, contract_id)
DO UPDATE SET
    title              = EXCLUDED.title,
    customer_tin       = EXCLUDED.customer_tin,
    customer_name      = EXCLUDED.customer_name,
    winner_tin         = EXCLUDED.winner_tin,
    winner_name        = EXCLUDED.winner_name,
    amount_uzs         = EXCLUDED.amount_uzs,
    amount_usd         = EXCLUDED.amount_usd,
    currency_raw       = EXCLUDED.currency_raw,
    date               = EXCLUDED.date,
    category           = EXCLUDED.category,
    funding_source     = EXCLUDED.funding_source,
    purchase_method    = EXCLUDED.purchase_method,
    is_direct_purchase = EXCLUDED.is_direct_purchase,
    raw                = EXCLUDED.raw
"""


def row_params(t: NormalizedTender) -> dict[str, Any]:
    d = asdict(t)
    # NOT NULL DEFAULT '' в schema → подставляем '' вместо None для UNIQUE-ключа
    d["lot_id"] = d.get("lot_id") or ""
    d["contract_id"] = d.get("contract_id") or ""
    # is_direct_purchase: True/False (Postgres BOOLEAN); SQLite примет как 0/1
    d["is_direct_purchase"] = bool(d.get("is_direct_purchase"))
    # дата → ISO string (sqlite не знает date() напрямую)
    if d.get("date") is not None:
        d["date"] = str(d["date"])
    d["raw"] = json.dumps(t.raw, ensure_ascii=False)
    return d


def upsert_tenders(con, ts: list[NormalizedTender]) -> tuple[int, int]:
    """Возвращает (inserted, updated). В SQLite дешевле проверить наличие отдельно."""
    ins = upd = 0
    cur = con.cursor()
    for t in ts:
        params = row_params(t)
        # сначала смотрим, существует ли строка
        cur.execute(
            "SELECT id FROM tenders WHERE source_dataset = %(s)s AND lot_id = %(l)s AND contract_id = %(c)s",
            {"s": params["source_dataset"], "l": params["lot_id"], "c": params["contract_id"]},
        )
        existed = cur.fetchone() is not None
        cur.execute(UPSERT_SQL, params)
        if existed:
            upd += 1
        else:
            ins += 1
    return ins, upd


def upsert_org(con, tin: str, name: str, source: str = "tender_row") -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO org_directory (tin, name, source) VALUES (%(t)s, %(n)s, %(s)s)
        ON CONFLICT (tin) DO UPDATE SET name = EXCLUDED.name, updated_at = CURRENT_TIMESTAMP
        WHERE org_directory.source <> 'manual'
        """,
        {"t": tin, "n": name, "s": source},
    )


def log_ingest(con, struct_id: str, fetched: int, ins: int, upd: int, error: str | None = None) -> None:
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO ingest_log (source_dataset, rows_fetched, rows_inserted, rows_updated, error, finished_at)
        VALUES (%(d)s, %(f)s, %(i)s, %(u)s, %(e)s, CURRENT_TIMESTAMP)
        """,
        {"d": struct_id, "f": fetched, "i": ins, "u": upd, "e": error},
    )


def org_name_from_meta(meta: dict) -> str | None:
    o = meta.get("orgName") or {}
    return o.get("engText") or o.get("rusText") or o.get("uzbText")


def load_one(con, struct_id: str, label: str, *, dry_run: bool) -> dict[str, Any]:
    t0 = time.time()
    print(f"[>] {struct_id}  {label}")
    meta = fetch_meta(struct_id)
    rows = fetch_data(struct_id)
    tenders = normalize_dataset(rows, struct_id, UZS_PER_USD)
    tenders = dedup_tenders(tenders)

    # подставим customer_name из метаданных датасета, если пусто
    meta_org = org_name_from_meta(meta)
    for t in tenders:
        if not t.customer_name and meta_org and not t.customer_tin:
            # нет ИНН заказчика → значит заказчик = владелец датасета (напр. Алмалыкский ГМК)
            t.customer_name = meta_org

    n_with_amount = sum(1 for t in tenders if t.amount_uzs is not None)
    n_direct = sum(1 for t in tenders if t.is_direct_purchase)
    amounts = sorted(t.amount_uzs for t in tenders if t.amount_uzs is not None)
    median = amounts[len(amounts) // 2] if amounts else None

    stats = {
        "struct_id": struct_id,
        "fetched": len(rows),
        "after_dedup": len(tenders),
        "with_amount": n_with_amount,
        "direct_purchase": n_direct,
        "median_uzs": median,
    }

    if dry_run:
        print(f"    DRY  rows={stats['fetched']}  unique={stats['after_dedup']}  "
              f"direct={n_direct}  median_uzs={median}")
        return stats

    ins, upd = upsert_tenders(con, tenders)
    # попутно заполняем org_directory из строк, где есть и ИНН и имя
    for t in tenders:
        if t.winner_tin and t.winner_name:
            upsert_org(con, t.winner_tin, t.winner_name, "tender_row")
        if t.customer_tin and t.customer_name:
            upsert_org(con, t.customer_tin, t.customer_name, "tender_row")
    log_ingest(con, struct_id, len(rows), ins, upd)
    con.commit()
    dt = time.time() - t0
    print(f"    OK  rows={stats['fetched']}  ins={ins}  upd={upd}  direct={n_direct}  "
          f"median_uzs={median}  ({dt:.1f}s)")
    return {**stats, "inserted": ins, "updated": upd, "elapsed_s": dt}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ids", help="comma-separated structIds, override DEFAULT_DATASETS")
    p.add_argument("--dry-run", action="store_true", help="без записи в БД")
    p.add_argument("--init", action="store_true", help="перед загрузкой применить schema.sql")
    args = p.parse_args(argv)

    datasets: list[tuple[str, str]] = (
        [(s.strip(), s.strip()) for s in args.ids.split(",") if s.strip()]
        if args.ids else DEFAULT_DATASETS
    )

    if args.dry_run:
        print("[*] dry-run: в БД не пишем")
        for sid, label in datasets:
            try:
                load_one(None, sid, label, dry_run=True)
            except Exception as e:
                print(f"    [!] {sid}: {e}")
        return 0

    from backend.db.connection import connect, apply_schema, dialect
    print(f"[*] dialect: {dialect()}  (DATABASE_URL {'set' if DATABASE_URL else 'EMPTY → using local SQLite'})")
    apply_schema()  # idempotent

    con = connect()
    try:
        for sid, label in datasets:
            try:
                load_one(con, sid, label, dry_run=False)
            except Exception as e:
                print(f"    [!] {sid}: {e}")
                try:
                    con.rollback()
                    log_ingest(con, sid, 0, 0, 0, error=str(e))
                    con.commit()
                except Exception:
                    pass
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
