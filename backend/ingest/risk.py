"""
Risk calculator: пересчитывает risk_score и красные флаги для всех тендеров.

КЛЮЧЕВЫЕ (влияют на score 0–100):
  1. Монополизация (+30): пара (customer_tin, winner_tin) > MONOPOLY_THRESHOLD побед.
  2. Прямая закупка без конкурса (+30): is_direct_purchase = TRUE.
  3. Завышенная цена (+40): amount_uzs > OVERPRICE_MULT * avg(amount_uzs) по категории.

ДОПОЛНИТЕЛЬНЫЕ СИГНАЛЫ (только в risk_flags JSONB, на UI отображаются как chips,
но НЕ меняют score — чтобы не ломать существующие пороги зон):
  • end_of_quarter   — дата ≤ 4 дня до конца квартала (срочно потратить бюджет).
  • round_number     — сумма «круглая» (10/50/100 млн без копеек).
  • splitting        — пара (cust,win) имеет ≥2 контракта в одну дату (обход порога).
  • one_off_winner   — у победителя ровно 1 контракт за всю историю (фирма-«прокладка»).
  • concentration    — заказчик отдал >50% объёма одному победителю.
  • weekend          — контракт оформлен в выходной (бумажная формальность).
  • dumping          — цена в 5+ раз ниже средней по категории (демпинг ради контракта).

Чистая Python-агрегация → один UPDATE на тендер. Совместимо с SQLite и Postgres.

Использование:
    python -m backend.ingest.risk
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import date, datetime
from typing import Any

# ---- ключевые пороги (влияют на score) ----
MONOPOLY_THRESHOLD = 5
OVERPRICE_MULT = 1.30

# ---- доп. эвристики (только подсветка в UI) ----
EOQ_DAYS_BEFORE = 4              # последние N дней квартала
DUMPING_MULT = 0.20              # < 20% от avg по категории
CONCENTRATION_RATIO = 0.50       # > 50% объёма заказчика на одного поставщика


def _parse_date(d) -> date | None:
    if d is None:
        return None
    if isinstance(d, date):
        return d
    s = str(d).strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _is_eoq(d: date | None) -> bool:
    if not d:
        return False
    if d.month not in (3, 6, 9, 12):
        return False
    # последний день месяца квартала: для марта 31, июня 30, сент 30, дек 31
    last_day = 30 if d.month in (6, 9) else 31
    return (last_day - d.day) <= EOQ_DAYS_BEFORE - 1


def _is_round(amount: float | None) -> bool:
    """«Подозрительно круглая» — сумма ровно делится на крупный шаг (без копеек)."""
    if not amount or amount < 1_000_000:
        return False
    a = float(amount)
    # дробной части быть не должно
    if abs(a - round(a)) > 0.5:
        return False
    a = int(round(a))
    # минимум 5 нулей в конце для контрактов > 10M, 4 нуля для меньших
    if a >= 100_000_000 and a % 10_000_000 == 0:
        return True
    if a >= 10_000_000  and a % 1_000_000  == 0:
        return True
    # Для контрактов 1–10M порог % 500_000 давал 37% false positives → ужесточили до % 1M.
    if a >= 1_000_000   and a % 1_000_000  == 0:
        return True
    return False


def recalc_all() -> dict[str, Any]:
    from backend.db.connection import connect

    con = connect()
    try:
        cur = con.cursor()

        # 1) загрузить всё нужное в память
        cur.execute("""
            SELECT id, customer_tin, winner_tin, category, amount_uzs, is_direct_purchase, date
            FROM tenders
        """)
        rows = cur.fetchall()

        # ---- Pre-aggregates ----
        sum_by_cat: dict[str, float] = defaultdict(float)
        cnt_by_cat: dict[str, int] = defaultdict(int)
        prices_by_cat: dict[str, list[float]] = defaultdict(list)
        pair_counts: Counter = Counter()
        pair_dates: dict[tuple, list] = defaultdict(list)         # (cust,win) → [date,...]
        winner_total: Counter = Counter()                         # winner_tin → all wins count
        customer_total_amount: dict[str, float] = defaultdict(float)
        customer_total_count: Counter = Counter()                 # сколько всего контрактов у заказчика
        pair_total_amount: dict[tuple, float] = defaultdict(float)

        for r in rows:
            cat = r.get("category")
            amt = r.get("amount_uzs")
            ct, wt = r.get("customer_tin"), r.get("winner_tin")
            d = _parse_date(r.get("date"))

            if cat and amt is not None and float(amt) > 0:
                sum_by_cat[cat] += float(amt)
                cnt_by_cat[cat] += 1
                prices_by_cat[cat].append(float(amt))
            if ct and wt:
                pair_counts[(ct, wt)] += 1
                if d:
                    pair_dates[(ct, wt)].append(d)
            if wt:
                winner_total[wt] += 1
            if ct:
                customer_total_count[ct] += 1
                if amt is not None and float(amt) > 0:
                    customer_total_amount[ct] += float(amt)
                    if wt:
                        pair_total_amount[(ct, wt)] += float(amt)

        avg_by_cat = {c: sum_by_cat[c] / cnt_by_cat[c] for c in sum_by_cat}

        # для splitting — сколько раз каждый день встречается у пары
        pair_date_counts: dict[tuple, dict[date, int]] = {}
        for k, dlist in pair_dates.items():
            cnt = Counter(dlist)
            pair_date_counts[k] = {d: c for d, c in cnt.items() if c >= 2}

        # ---- 4) Обновить analytics_cache ----
        cur.execute("DELETE FROM analytics_cache")
        for cat, avg in avg_by_cat.items():
            prices = sorted(prices_by_cat[cat])
            if not prices:
                continue   # защита: категория без валидных сумм
            median = prices[len(prices) // 2]
            cur.execute(
                "INSERT INTO analytics_cache (category, avg_price, median_price, total_count) "
                "VALUES (%(c)s, %(a)s, %(m)s, %(n)s)",
                {"c": cat, "a": round(avg, 2), "m": round(median, 2), "n": cnt_by_cat[cat]},
            )

        # ---- 5) Каждому тендеру — флаги + score ----
        red = yellow = green = 0
        n_mon = n_nc = n_op = 0
        # счётчики дополнительных
        n_eoq = n_round = n_split = n_oneoff = n_conc = n_wknd = n_dump = 0

        for r in rows:
            ct, wt = r.get("customer_tin"), r.get("winner_tin")
            cat = r.get("category")
            amt = r.get("amount_uzs")
            direct = bool(r.get("is_direct_purchase"))
            d = _parse_date(r.get("date"))

            # ---- Ключевые ----
            pair_wins = pair_counts.get((ct, wt), 0) if (ct and wt) else 0
            f_mon = pair_wins > MONOPOLY_THRESHOLD
            f_nc = direct
            cat_avg = avg_by_cat.get(cat) if cat else None
            f_op = bool(amt is not None and cat_avg and float(amt) > cat_avg * OVERPRICE_MULT)
            score = (30 if f_mon else 0) + (30 if f_nc else 0) + (40 if f_op else 0)
            score = min(100, score)

            # ---- Дополнительные ----
            f_eoq = _is_eoq(d)
            f_round = _is_round(amt)
            f_split = bool(ct and wt and d and pair_date_counts.get((ct, wt), {}).get(d, 0) >= 2)
            f_oneoff = bool(wt and winner_total.get(wt, 0) == 1)
            cust_total = customer_total_amount.get(ct, 0) if ct else 0
            pair_total = pair_total_amount.get((ct, wt), 0) if (ct and wt) else 0
            cust_n = customer_total_count.get(ct, 0) if ct else 0
            # минимум 5 контрактов у заказчика, чтобы концентрация имела статистический смысл.
            f_conc = bool(cust_total > 0 and cust_n >= 5 and pair_total / cust_total > CONCENTRATION_RATIO)
            f_wknd = bool(d and d.weekday() >= 5)
            f_dump = bool(amt is not None and cat_avg and float(amt) > 0 and float(amt) < cat_avg * DUMPING_MULT)

            flags_json = json.dumps({
                "monopoly":      f_mon,
                "no_compete":    f_nc,
                "overpriced":    f_op,
                "pair_wins":     pair_wins,
                "category_avg":  round(cat_avg, 2) if cat_avg else None,
                # доп. сигналы
                "end_of_quarter": f_eoq,
                "round_number":   f_round,
                "splitting":      f_split,
                "one_off_winner": f_oneoff,
                "concentration":  f_conc,
                "weekend":        f_wknd,
                "dumping":        f_dump,
                "concentration_pct": round(pair_total / cust_total * 100, 1) if cust_total else None,
            }, ensure_ascii=False)

            cur.execute(
                """UPDATE tenders SET
                       risk_score      = %(s)s,
                       risk_flags      = %(j)s,
                       flag_monopoly   = %(fm)s,
                       flag_no_compete = %(fn)s,
                       flag_overpriced = %(fo)s
                   WHERE id = %(id)s""",
                {"s": score, "j": flags_json,
                 "fm": bool(f_mon), "fn": bool(f_nc), "fo": bool(f_op),
                 "id": r["id"]},
            )

            if score >= 70: red += 1
            elif score >= 30: yellow += 1
            else: green += 1
            if f_mon: n_mon += 1
            if f_nc:  n_nc += 1
            if f_op:  n_op += 1
            if f_eoq: n_eoq += 1
            if f_round: n_round += 1
            if f_split: n_split += 1
            if f_oneoff: n_oneoff += 1
            if f_conc: n_conc += 1
            if f_wknd: n_wknd += 1
            if f_dump: n_dump += 1

        con.commit()
        return {
            "total": len(rows), "red": red, "yellow": yellow, "green": green,
            "n_monopoly": n_mon, "n_no_compete": n_nc, "n_overpriced": n_op,
            "categories_scored": len(avg_by_cat),
            # доп. сигналы
            "n_end_of_quarter": n_eoq,
            "n_round_number":   n_round,
            "n_splitting":      n_split,
            "n_one_off":        n_oneoff,
            "n_concentration":  n_conc,
            "n_weekend":        n_wknd,
            "n_dumping":        n_dump,
        }
    finally:
        con.close()


if __name__ == "__main__":
    s = recalc_all()
    print("=== Risk recalculated ===")
    for k, v in s.items():
        print(f"  {k:20s} = {v}")
