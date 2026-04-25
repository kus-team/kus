"""
Обзорщик data.egov.uz: ищет датасеты, похожие на сырые тендеры (с полями
winner/customer/amount/participants), а не на агрегированную статистику.

Использует публичные эндпоинты портала:
  GET apiClient/main/gettable   — поиск/листинг датасетов
  GET apiClient/Main/GetById    — метаданные (поля и т.п.)
  GET apiData/MainData/GetByFile?id=...&fileType=1&tableType=2  — сами данные JSON

Запуск:
  python scripts/explore_datasets.py
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import requests

BASE = "https://data.egov.uz"
OUT = Path(__file__).resolve().parent.parent / "data" / "explore"
OUT.mkdir(parents=True, exist_ok=True)

# ключевые слова для поиска (на узб/рус/англ)
SEARCH_TERMS = ["xarid", "tender", "tanlov", "закуп", "тендер", "savdo", "конкурс"]

# поля, намекающие на сырые тендеры
RAW_TENDER_HINTS = [
    r"winner", r"yutuvchi", r"yutgan", r"pobed",
    r"customer", r"buyurtmachi", r"zakazchik", r"заказч",
    r"amount", r"summa", r"narx", r"price", r"сумм", r"цена",
    r"participant", r"ishtirokchi", r"участ",
    r"date", r"sana", r"дата",
    r"category", r"toifa", r"катего",
    r"contract", r"shartnoma", r"договор",
    r"tender[_ ]?id", r"lot",
]
HINT_RE = re.compile("|".join(RAW_TENDER_HINTS), re.IGNORECASE)

# поля, характерные для "просто статистики" — такие датасеты нам не нужны
STAT_HINTS = [r"foiz", r"процент", r"percent", r"count only", r"total"]


@dataclass
class DatasetInfo:
    struct_id: str
    code: str
    title_en: str
    org_en: str
    update_date: str
    fields: list[dict[str, Any]] = field(default_factory=list)
    score: int = 0
    sample_rows: int = 0
    match_fields: list[str] = field(default_factory=list)

    def as_row(self) -> dict[str, Any]:
        return {
            "struct_id": self.struct_id,
            "code": self.code,
            "title_en": self.title_en,
            "org_en": self.org_en,
            "update_date": self.update_date,
            "score": self.score,
            "field_count": len(self.fields),
            "sample_rows": self.sample_rows,
            "match_fields": self.match_fields,
        }


def search(term: str, limit: int = 50, offset: int = 0) -> list[dict]:
    r = requests.get(
        f"{BASE}/apiClient/main/gettable",
        params={"limit": limit, "offset": offset, "text": term,
                "orgId": "", "sphereId": "", "regionId": ""},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("result", {}).get("data", [])


def get_meta(struct_id: str) -> dict:
    r = requests.get(f"{BASE}/apiClient/Main/GetById",
                     params={"id": struct_id}, timeout=30)
    r.raise_for_status()
    return r.json().get("result", {})


def get_data(struct_id: str) -> list[dict] | None:
    r = requests.get(
        f"{BASE}/apiData/MainData/GetByFile",
        params={"id": struct_id, "fileType": 1, "tableType": 2},
        timeout=60,
    )
    if r.status_code != 200 or not r.text.strip():
        return None
    try:
        data = r.json()
    except ValueError:
        return None
    return data if isinstance(data, list) else None


def score_dataset(meta: dict) -> tuple[int, list[str]]:
    """Оцениваем датасет по полям — чем больше полей похоже на сырые тендеры, тем выше."""
    score = 0
    matches: list[str] = []
    for f in meta.get("tableFields", []) or []:
        text = f.get("name", "") + " " + " ".join(
            (f.get("text") or {}).get(k, "") or "" for k in ("engText", "rusText", "uzbText"))
        if HINT_RE.search(text):
            score += 2
            matches.append(f.get("name", "?"))
    # штраф за «Foizda» / процентные поля
    for f in meta.get("tableFields", []) or []:
        name = f.get("name", "").lower()
        if any(s in name for s in ("foiz", "percent", "процент")):
            score -= 1
    return score, matches


def main() -> None:
    seen: dict[str, DatasetInfo] = {}
    print("[*] Поиск датасетов по ключевым словам...")
    for term in SEARCH_TERMS:
        items = search(term, limit=50)
        print(f"    '{term}': {len(items)} результатов")
        for it in items:
            sid = it.get("structId")
            if not sid or sid in seen:
                continue
            seen[sid] = DatasetInfo(
                struct_id=sid,
                code=it.get("name", ""),
                title_en=(it.get("dataName") or {}).get("engText", "")
                         or (it.get("dataName") or {}).get("rusText", ""),
                org_en=(it.get("orgName") or {}).get("engText", "")
                       or (it.get("orgName") or {}).get("rusText", ""),
                update_date=it.get("updateDate", "")[:10],
            )
    print(f"[*] Уникальных датасетов: {len(seen)}")

    print("[*] Запрашиваем метаданные каждого...")
    for i, ds in enumerate(seen.values(), 1):
        try:
            meta = get_meta(ds.struct_id)
            ds.fields = meta.get("tableFields", []) or []
            ds.score, ds.match_fields = score_dataset(meta)
        except Exception as e:
            print(f"    [!] {ds.struct_id}: {e}")
            continue
        if i % 20 == 0:
            print(f"    {i}/{len(seen)}")
        time.sleep(0.05)

    top = sorted(seen.values(), key=lambda d: d.score, reverse=True)[:15]
    print("\n[*] Топ-15 кандидатов по полям:")
    print(f"{'score':>5} | {'rows':>5} | {'code':12} | {'upd':10} | title")
    for ds in top:
        print(f"{ds.score:>5} | {'?':>5} | {ds.code:12} | {ds.update_date:10} | {ds.title_en[:80]}")

    # для топ-5 пробуем скачать 1 кусок данных и посчитать строки
    print("\n[*] Скачиваем JSON для топ-5 и смотрим реальный объём...")
    for ds in top[:5]:
        data = get_data(ds.struct_id)
        if data is None:
            ds.sample_rows = -1
            continue
        ds.sample_rows = len(data)
        sample_file = OUT / f"{ds.struct_id}.json"
        sample_file.write_text(json.dumps(data[:5], ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"    {ds.struct_id}  rows={len(data):>6}  sample → {sample_file.name}")

    # финальный отчёт
    report = OUT / "report.json"
    report.write_text(
        json.dumps([ds.as_row() for ds in top], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n[*] Отчёт: {report}")


if __name__ == "__main__":
    main()
