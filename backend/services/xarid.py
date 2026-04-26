"""
Live-feed свежих сделок с xarid.uzex.uz (UZEX национальный магазин).

Публичный API портала возвращает только последние 11-12 завершённых сделок
(витрина), с реальными именами компаний, регионами, числом участников.
Кэшируем in-memory на 5 минут, чтобы не дёргать сервер на каждый запрос.
"""
from __future__ import annotations

import time
from typing import Any

import requests

API_URL = "https://xarid-api-shop.uzex.uz/Common/GetCompletedDeals"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://xarid.uzex.uz",
    "Referer": "https://xarid.uzex.uz/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/130 Safari/537.36",
    "Accept-Language": "ru-RU,ru;q=0.9",
}
CACHE_TTL = 300  # 5 min

_cache: dict[str, Any] = {"data": None, "ts": 0}


def _fetch(year: int, from_m: int, to_m: int, on_national: bool = False) -> list[dict]:
    payload = {
        "region_ids": [],
        "display_on_shop": 0 if on_national else 1,
        "display_on_national": 1 if on_national else 0,
        "year_id": year,
        "from": from_m,
        "to": to_m,
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=20)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else []


def fetch_recent() -> dict[str, Any]:
    """Свежие сделки UZEX. Кэш на 5 минут."""
    now = time.time()
    if _cache["data"] is not None and now - _cache["ts"] < CACHE_TTL:
        return {"data": _cache["data"], "cached": True, "age_s": round(now - _cache["ts"])}

    from datetime import datetime
    today = datetime.utcnow()
    try:
        # Берём текущий месяц с national + shop
        national = _fetch(today.year, today.month, today.month, on_national=True)
        shop = _fetch(today.year, today.month, today.month, on_national=False)
        # Если в текущем месяце пусто — берём весь год
        if not national and not shop:
            national = _fetch(today.year, 1, 12, on_national=True)
            shop = _fetch(today.year, 1, 12, on_national=False)
        # Объединяем, помечаем источник
        for x in national: x["_source"] = "national"
        for x in shop:     x["_source"] = "shop"
        merged = sorted(national + shop, key=lambda x: x.get("deal_date", ""), reverse=True)[:24]
    except Exception as e:
        return {"data": [], "error": str(e), "cached": False}

    _cache["data"] = merged
    _cache["ts"] = now
    return {"data": merged, "cached": False, "age_s": 0}
