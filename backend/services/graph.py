"""
Построитель графа связей «заказчик ↔ победитель».

Возвращает узлы и рёбра в формате, пригодном для vis.js Network.
Узлы:
  - customer (синий) и winner (красный/оранжевый по avg_risk).
Рёбра:
  - между парой (customer_tin, winner_tin), вес = число побед, тултип = total_uzs.

Фильтры:
  - min_wins: показывать только пары с хотя бы N победами (по умолчанию 2);
  - limit_pairs: ограничение по количеству рёбер.
"""
from __future__ import annotations

from typing import Any

from backend.db.connection import connect


def build_network(min_wins: int = 2, limit_pairs: int = 200) -> dict[str, Any]:
    sql = """
        SELECT t.customer_tin                                       AS customer_tin,
               COALESCE(MAX(c_od.name), MAX(t.customer_name))       AS customer_name,
               t.winner_tin                                         AS winner_tin,
               COALESCE(MAX(w_od.name), MAX(t.winner_name))         AS winner_name,
               COUNT(*)                                             AS wins,
               COALESCE(SUM(t.amount_uzs), 0)                       AS total_uzs,
               COALESCE(SUM(t.amount_usd), 0)                       AS total_usd,
               CAST(ROUND(AVG(t.risk_score), 0) AS INTEGER)         AS avg_risk,
               SUM(CASE WHEN t.risk_score >= 70 THEN 1 ELSE 0 END)  AS red_wins
        FROM tenders t
        LEFT JOIN org_directory c_od ON c_od.tin = t.customer_tin
        LEFT JOIN org_directory w_od ON w_od.tin = t.winner_tin
        WHERE t.customer_tin IS NOT NULL AND t.winner_tin IS NOT NULL
        GROUP BY t.customer_tin, t.winner_tin
        HAVING COUNT(*) >= %(mw)s
        ORDER BY wins DESC, total_uzs DESC
        LIMIT %(lim)s
    """
    con = connect()
    try:
        cur = con.cursor()
        cur.execute(sql, {"mw": min_wins, "lim": limit_pairs})
        pairs = list(cur.fetchall())
    finally:
        con.close()

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    def color_for_winner(avg_risk: int | None) -> str:
        if avg_risk is None:
            return "#5a6a80"
        if avg_risk >= 70:
            return "#e74c3c"
        if avg_risk >= 30:
            return "#e67e22"
        return "#27ae60"

    for p in pairs:
        cid = f"c:{p['customer_tin']}"
        wid = f"w:{p['winner_tin']}"
        if cid not in nodes:
            nodes[cid] = {
                "id": cid,
                "label": (p["customer_name"] or p["customer_tin"])[:40],
                "title": f"Заказчик · ИНН {p['customer_tin']}",
                "group": "customer",
                "color": "#3498db",
                "shape": "diamond",
            }
        if wid not in nodes:
            nodes[wid] = {
                "id": wid,
                "label": (p["winner_name"] or p["winner_tin"])[:40],
                "title": f"Победитель · ИНН {p['winner_tin']} · avg risk {p['avg_risk']}",
                "group": "winner",
                "color": color_for_winner(p["avg_risk"]),
                "shape": "dot",
                "value": float(p["total_uzs"] or 0),
            }
        edges.append({
            "from": cid,
            "to": wid,
            "value": p["wins"],
            "title": f"{p['wins']} побед · {round(float(p['total_usd'] or 0)):,} USD · avg risk {p['avg_risk']}",
            "color": {"color": "#c0392b" if p["red_wins"] > 0 else "#5a6a80"},
        })

    return {"nodes": list(nodes.values()), "edges": edges, "pairs": len(pairs)}
