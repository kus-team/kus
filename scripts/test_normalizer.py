"""Тест нормализатора на сэмплах из data/explore/."""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from backend.ingest.normalizer import normalize_dataset

UZS_PER_USD = 12500.0

samples = [
    "613eeda614665dbb8ec80453",  # Минэкономфин
    "64dc981eb04a41cb2e29d57b",  # Минэкономфин 2
    "61137447db32b99538e086fc",  # Алмалыкский ГМК
    "6225c27ed31e97c0521ec8a1",  # аукционы земли
    "6142ef46ba3615f6f07bca0f",  # госзакупки (третий)
]

for sid in samples:
    f = ROOT / "data" / "explore" / f"{sid}.json"
    rows = json.loads(f.read_text(encoding="utf-8"))
    tenders = normalize_dataset(rows, sid, UZS_PER_USD)
    if not tenders:
        print(f"[!] {sid}: empty")
        continue
    t = tenders[0]
    amounts = sorted([x.amount_uzs for x in tenders if x.amount_uzs])
    median = amounts[len(amounts) // 2] if amounts else None
    print(f"=== {sid}  (N={len(tenders)}, median_uzs={median}) ===")
    for k in ("title", "customer_tin", "winner_tin", "winner_name",
              "amount_uzs", "amount_usd", "currency_raw", "date",
              "category", "is_direct_purchase"):
        v = getattr(t, k)
        print(f"    {k:20s} = {v}")
    print()
