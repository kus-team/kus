"""
Нормализатор: строка сырого датасета egov.uz → унифицированная схема Tender.

Схемы датасетов плывут — имена полей у разных органов разные (иногда с опечатками).
Мы не делаем жёсткий маппинг 1:1, а ищем поля по ключевым словам-подстрокам.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from datetime import date, datetime
from typing import Any, Iterable


# ---------- распознавание типа поля по имени ----------

# (роль, список подстрок — если совпало хотя бы одно, поле считаем этой ролью)
FIELD_RULES: list[tuple[str, list[str]]] = [
    # ИНН заказчика
    ("customer_tin",
     ["buyurtmachi" + "stir", "buyurtmachining" + "stir", "zakazchikstir", "zakazchik_inn"]),
    ("customer_name",
     ["buyurtmachinomi", "zakazchikname"]),
    # Поставщик / победитель
    ("winner_tin",
     ["yetkazibberuvchi" + "stir", "yetkazibberuvchnomivastir", "yetkazibberuvchinomivastir",
      "postavshchik" + "inn", "postavshchik" + "stir", "stirraqami"]),
    ("winner_name",
     ["yetkazibberuvchi_name", "yetkazibberuvchinomi", "yetkazibberuvch" + "nomi",
      "postavshchik", "golib", "yutuvchi", "yetkazibberuvchi"]),  # последние — после _tin, так что сработают только если tin уже найден
    # Начальная цена ИДЁТ ПЕРВОЙ, чтобы поглотить 'xaridboshlangichqiymat...'
    # до того, как 'qiymat'-подобные правила в amount попробуют это поле.
    ("start_price", ["boshlangichnarx", "xaridboshlangich"]),
    # Финальная сумма контракта
    ("amount", [
        "xaridamalgaoshirilganqiymat",   # финальная цена в 'ming som' датасетах
        "shartnomaqiymat",               # основная форма
        "shatrnomaqiymat",               # sic — опечатка в одном из датасетов
        "summasomda",                    # 'сумма в сумах'
        "sotilgannarx",                  # цена продажи (аукционы)
        "summa",                         # просто 'сумма'
        "umumiynarx",                    # «общая цена»
        "narxi",                         # «цена»
        "narx",                          # цена кратко
        "amount",                        # англ
        "jami",                          # «всего»
        "jamimiqdorihajmiqiymati",       # 'общий объём, ценность'
    ]),
    # Даты
    ("date", ["shartnomasan", "auksionsan", "sana", "contractdate", "bayonnomasanasi"]),
    # Название
    ("title", ["xaridpredmeti", "predmetimahsulot", "predmeti",
               "tovarxizmatnomi", "yerjoylashgan", "nameofproduct",
               "xaridpredmetimaxsulot", "tafsilotlar"]),
    # Категория
    ("category", ["kategoriyasi", "kategoriya", "toifasi", "toifa", "golibturi"]),
    # Лот и контракт (q ↔ k транскрипция)
    ("lot_id", ["lotraqami", "lotraq", "lotrakami", "unikalraqami", "lot", "lotshartnomaraqami"]),
    ("contract_id", ["shartnomaraqami", "shartnomaraq", "shartnomarakami",
                     "shartnomaraqamivasanasi", "shartnomarakamivasanasi"]),
    # Финансирование и способ закупки
    ("funding_source", ["moliyalashtirish" + "manbai", "moliyalashtirish" + "manbalari",
                        "moliyalashtirishmanba"]),
    ("purchase_method", ["xaridturi", "togridantogri" + "xarid", "togridantogri", "xaridaasosi"]),
    # Валюта (у Алмалыка)
    ("currency_raw", ["valyuta"]),
]


def _norm_key(k: str) -> str:
    """привести имя поля к ascii-lowercase без спецсимволов, чтобы сравнивать подстроками."""
    return re.sub(r"[^a-z0-9]", "", k.lower())


def detect_role(field_name: str, already_taken: set[str]) -> str | None:
    """определить роль поля по имени."""
    n = _norm_key(field_name)
    for role, needles in FIELD_RULES:
        # не присваиваем winner_name, если winner_tin ещё не найден — оно слишком жадное
        if role == "winner_name" and "winner_tin" not in already_taken:
            # но 'Golib' отдельно — это явно имя победителя, tin там не обязателен
            if "golib" in n or "yutuvchi" in n:
                return role
            continue
        if role in already_taken:
            continue
        if any(s in n for s in needles):
            return role
    return None


# ---------- приведение значений ----------

_NUM_CLEAN = re.compile(r"[^\d.,-]")

# «Мусорные» значения для ИНН и др. идентификаторов: заменяем на None
_TRASH_VALUES = {"", "X", "-", "—", "?", "N/A", "Н/О", "NULL", "NONE", "0"}


def _clean_id(v: Any) -> str | None:
    """Очищает ИНН/ID: пустые, 'X', '—', '?', '0' → None. Иначе str.strip()."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s.upper() in _TRASH_VALUES:
        return None
    return s


def parse_number(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if not s or s.upper() in {"X", "-", "—", "N/A"}:
        return None
    # "1 599 200 000.01" → "1599200000.01"
    s = _NUM_CLEAN.sub("", s).replace(",", ".")
    # если осталось несколько точек, оставляем последнюю как десятичную
    if s.count(".") > 1:
        head, _, tail = s.rpartition(".")
        s = head.replace(".", "") + "." + tail
    try:
        return float(s)
    except ValueError:
        return None


# Excel serial date (1900 date system): 1 = 1900-01-01
_EXCEL_EPOCH = datetime(1899, 12, 30)


def parse_date(v: Any) -> date | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    # excel serial (1900-system, 1900-01-01 → 1, но Excel считает 1900 как leap year, отсюда -2)
    if re.fullmatch(r"\d{4,6}", s):
        try:
            n = int(s)
            if 20000 < n < 80000:   # разумный диапазон (1954–2119)
                return _EXCEL_EPOCH.fromordinal(_EXCEL_EPOCH.toordinal() + n).date()
        except Exception:
            pass
    # ISO
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ",
                "%d.%m.%Y", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s[:len(fmt) + 6], fmt).date()
        except ValueError:
            continue
    return None


# медианная эвристика для датасета: если медиана сумм < 1_000_000 сум — это 'тыс. сум', умножаем всё на 1000.
# Автоматика по имени поля ненадёжна: в датасете 64dc981e поле называется 'mingsomda',
# но реальные значения — обычные сумы (сотни млн за годовой контракт), а не тыс. сум (что дало бы сотни млрд).
AMOUNT_SCALE_THRESHOLD_UZS = 1_000_000


def scale_amounts(tenders: list["NormalizedTender"], uzs_per_usd: float) -> None:
    """Если медиана сумм подозрительно мала — предполагаем, что это 'тыс. сум', и умножаем на 1000."""
    amounts = [t.amount_uzs for t in tenders if t.amount_uzs is not None and t.amount_uzs > 0]
    if not amounts:
        return
    amounts.sort()
    median = amounts[len(amounts) // 2]
    if median >= AMOUNT_SCALE_THRESHOLD_UZS:
        return  # разумные сумы, ничего не меняем
    for t in tenders:
        if t.amount_uzs is not None:
            t.amount_uzs = round(t.amount_uzs * 1000, 2)
            if uzs_per_usd > 0:
                t.amount_usd = round(t.amount_uzs / uzs_per_usd, 2)


# ---------- маркер прямой закупки ----------

DIRECT_PATTERNS = [
    r"пр[яя]мы[еая]",
    r"t[o'‘]g[‘']ridan[ -]?t[o'‘]g[‘']ri",
    r"togridantogri",
    r"direct (contract|procurement)",
]
DIRECT_RE = re.compile("|".join(DIRECT_PATTERNS), re.IGNORECASE)


def is_direct_purchase(purchase_method: str | None, funding_source: str | None = None) -> bool:
    txt = " ".join(filter(None, [purchase_method, funding_source])).strip()
    return bool(DIRECT_RE.search(txt))


# ---------- собственно нормализатор ----------

@dataclass
class NormalizedTender:
    source_dataset: str
    lot_id: str | None = None
    contract_id: str | None = None
    title: str | None = None
    customer_tin: str | None = None
    customer_name: str | None = None
    winner_tin: str | None = None
    winner_name: str | None = None
    amount_uzs: float | None = None
    amount_usd: float | None = None
    currency_raw: str | None = None
    date: date | None = None
    category: str | None = None
    funding_source: str | None = None
    purchase_method: str | None = None
    is_direct_purchase: bool = False
    raw: dict[str, Any] = field(default_factory=dict)

    def to_row(self) -> dict[str, Any]:
        d = asdict(self)
        d["raw"] = self.raw  # asdict сохраняет уже
        return d


def build_field_map(sample_row: dict[str, Any]) -> dict[str, str]:
    """по одной строке датасета понять: какое поле какую роль играет."""
    roles: dict[str, str] = {}  # field_name → role
    taken: set[str] = set()
    # два прохода: сначала явные (customer_tin, winner_tin, lot, contract, date, title, amount, category)
    # потом мягкие (winner_name) — они используют информацию о первом проходе
    for key in sample_row:
        role = detect_role(key, taken)
        if role:
            roles[key] = role
            taken.add(role)
    # добить те, что были пропущены на первом проходе из-за винлока
    for key in sample_row:
        if key in roles:
            continue
        role = detect_role(key, taken)
        if role:
            roles[key] = role
            taken.add(role)
    return roles


def normalize_row(
    row: dict[str, Any],
    field_map: dict[str, str],
    source_dataset: str,
    uzs_per_usd: float,
) -> NormalizedTender:
    t = NormalizedTender(source_dataset=source_dataset, raw=row)

    for raw_key, role in field_map.items():
        val = row.get(raw_key)
        if val in (None, "", "-"):
            continue
        if role == "amount":
            n = parse_number(val)
            if n is not None:
                t.amount_uzs = round(n, 2)   # масштаб по медиане применяется после normalize_dataset
        elif role == "start_price":
            pass  # пока не используем
        elif role == "date":
            t.date = parse_date(val)
        elif role in ("customer_tin", "winner_tin"):
            cleaned = _clean_id(val)
            if cleaned:
                setattr(t, role, cleaned)
        elif role in ("title", "customer_name",
                      "winner_name", "category",
                      "funding_source", "purchase_method", "lot_id",
                      "contract_id", "currency_raw"):
            setattr(t, role, str(val).strip())

    if t.amount_uzs is not None and uzs_per_usd > 0:
        t.amount_usd = round(t.amount_uzs / uzs_per_usd, 2)

    t.is_direct_purchase = is_direct_purchase(t.purchase_method, t.funding_source)
    return t


def normalize_dataset(
    rows: Iterable[dict[str, Any]],
    source_dataset: str,
    uzs_per_usd: float,
) -> list[NormalizedTender]:
    rows = list(rows)
    if not rows:
        return []
    fmap = build_field_map(rows[0])
    out = [normalize_row(r, fmap, source_dataset, uzs_per_usd) for r in rows]
    scale_amounts(out, uzs_per_usd)
    return out
