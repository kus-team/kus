"""
Risk calculator: пересчитывает risk_score и три бинарных флага для всех тендеров.

Три красных флага:
  1. Монополизация (+30): пара (customer_tin, winner_tin) > MONOPOLY_THRESHOLD побед.
  2. Прямая закупка без конкурса (+30): is_direct_purchase = TRUE.
  3. Завышенная цена (+40): amount_uzs > OVERPRICE_MULT * avg(amount_uzs) по своей категории.

Чистая Python-агрегация → один UPDATE на тендер. Совместимо с SQLite и Postgres.

Использование:
    python -m backend.ingest.risk
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from typing import Any

MONOPOLY_THRESHOLD = 5
OVERPRICE_MULT = 1.30


def recalc_all() -> dict[str, Any]:
    from backend.db.connection import connect

    con = connect()
    try:
        cur = con.cursor()

        # 1) загрузить всё нужное в память (для MVP объёмов это ОК)
        cur.execute("""
            SELECT id, customer_tin, winner_tin, category, amount_uzs, is_direct_purchase
            FROM tenders
        """)
        rows = cur.fetchall()

        # 2) средняя цена по категории
        sum_by_cat: dict[str, float] = defaultdict(float)
        cnt_by_cat: dict[str, int] = defaultdict(int)
        prices_by_cat: dict[str, list[float]] = defaultdict(list)
        for r in rows:
            cat = r.get("category")
            amt = r.get("amount_uzs")
            if cat and amt is not None and float(amt) > 0:
                sum_by_cat[cat] += float(amt)
                cnt_by_cat[cat] += 1
                prices_by_cat[cat].append(float(amt))

        avg_by_cat = {c: sum_by_cat[c] / cnt_by_cat[c] for c in sum_by_cat}

        # 3) частота побед по парам (заказчик ↔ победитель)
        pair_counts: Counter = Counter()
        for r in rows:
            ct, wt = r.get("customer_tin"), r.get("winner_tin")
            if ct and wt:
                pair_counts[(ct, wt)] += 1

        # 4) обновить analytics_cache
        cur.execute("DELETE FROM analytics_cache")
        for cat, avg in avg_by_cat.items():
            prices = sorted(prices_by_cat[cat])
            median = prices[len(prices) // 2]
            cur.execute(
                "INSERT INTO analytics_cache (category, avg_price, median_price, total_count) "
                "VALUES (%(c)s, %(a)s, %(m)s, %(n)s)",
                {"c": cat, "a": round(avg, 2), "m": round(median, 2), "n": cnt_by_cat[cat]},
            )

        # 5) для каждого тендера — посчитать score+флаги, обновить
        red = yellow = green = 0
        n_mon = n_nc = n_op = 0
        for r in rows:
            ct, wt = r.get("customer_tin"), r.get("winner_tin")
            cat = r.get("category")
            amt = r.get("amount_uzs")
            direct = bool(r.get("is_direct_purchase"))

            pair_wins = pair_counts.get((ct, wt), 0) if (ct and wt) else 0
            f_mon = pair_wins > MONOPOLY_THRESHOLD
            f_nc = direct
            cat_avg = avg_by_cat.get(cat) if cat else None
            f_op = bool(amt is not None and cat_avg and float(amt) > cat_avg * OVERPRICE_MULT)

            score = (30 if f_mon else 0) + (30 if f_nc else 0) + (40 if f_op else 0)
            score = min(100, score)

            flags_json = json.dumps({
                "monopoly":     f_mon,
                "no_compete":   f_nc,
                "overpriced":   f_op,
                "pair_wins":    pair_wins,
                "category_avg": round(cat_avg, 2) if cat_avg else None,
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

        con.commit()
        return {
            "total": len(rows), "red": red, "yellow": yellow, "green": green,
            "n_monopoly": n_mon, "n_no_compete": n_nc, "n_overpriced": n_op,
            "categories_scored": len(avg_by_cat),
        }
    finally:
        con.close()


if __name__ == "__main__":
    s = recalc_all()
    print("=== Risk recalculated ===")
    for k, v in s.items():
        print(f"  {k:20s} = {v}")
