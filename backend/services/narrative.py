"""
AI-нарратив для тендера: объясняет гражданину простым русским языком,
почему этот тендер подозрительный (или нет).

Явный system prompt + сжатый контекст из БД + одиночный вызов Anthropic Messages API.
Без tools — простой text-out, быстро и дёшево.
"""
from __future__ import annotations

from typing import Any

import anthropic

from backend.config import settings

MODEL = "claude-sonnet-4-5"   # актуальный alias на момент аудита
MAX_TOKENS = 600
REQUEST_TIMEOUT = 25          # короче чем Railway gateway 30s

SYSTEM = """Ты — независимый антикоррупционный аналитик, который простыми словами
объясняет гражданам Узбекистана, почему конкретный государственный тендер
выглядит подозрительным.

Правила:
- Пиши по-русски, нейтральным тоном, без эмоций и без обвинений конкретных людей.
- НЕ выдумывай факты. Опирайся ТОЛЬКО на данные из блока «ФАКТЫ».
- Если по фактам подозрений нет — так и скажи: «По доступным данным красных флагов нет».
- Объяснение должно быть 3–5 предложений, без списков и заголовков, естественной речью.
- В конце добавь одну строку: «Что проверить:» и 1–2 конкретных рекомендации
  (например: «найдите в реестре, кому ещё этот победитель поставлял услуги»;
  «сравните цену с открытыми каталогами на эту услугу»).
- НЕ упоминай номера статей УК, не ставь юридических диагнозов («коррупция», «откат»);
  используй слова «подозрительный паттерн», «нетипичная цена», «прямая закупка без конкурса».
"""


def _format_facts(t: dict[str, Any], avg_uzs: float | None = None) -> str:
    flags = t.get("risk_flags") or {}
    lines = [
        f"- Risk score: {t.get('risk_score')} из 100",
        f"- Заказчик ИНН: {t.get('customer_tin') or '—'}, имя: {t.get('customer_name') or '—'}",
        f"- Победитель ИНН: {t.get('winner_tin') or '—'}, имя: {t.get('winner_name') or '—'}",
        f"- Сумма: {t.get('amount_uzs')} сум (~ ${t.get('amount_usd')})",
        f"- Дата контракта: {t.get('date') or '—'}",
        f"- Категория: {t.get('category') or '—'}",
        f"- Способ закупки: {t.get('purchase_method') or '—'}",
        f"- Прямая закупка без конкурса: {'да' if t.get('is_direct_purchase') else 'нет'}",
    ]
    if flags:
        lines.append("Сработавшие флаги:")
        if flags.get("monopoly"):
            lines.append(f"  · «своя компания»: эта пара заказчик↔победитель встречалась {flags.get('pair_wins')} раз")
        if flags.get("no_compete"):
            lines.append("  · контракт оформлен как прямая закупка без конкурса")
        if flags.get("overpriced"):
            cat_avg = flags.get("category_avg")
            lines.append(f"  · цена выше средней по категории (avg ≈ {cat_avg} сум)")
    return "\n".join(lines)


def explain_tender(t: dict[str, Any]) -> str:
    if not settings.anthropic_api_key:
        raise RuntimeError("ANTHROPIC_API_KEY не задан в backend/.env")

    facts = _format_facts(t)
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key, timeout=REQUEST_TIMEOUT)
    resp = client.messages.create(
        model=MODEL,
        max_tokens=MAX_TOKENS,
        system=SYSTEM,
        messages=[{
            "role": "user",
            "content": (
                "Объясни простым языком, что не так с этим тендером.\n\n"
                f"ФАКТЫ:\n{facts}\n\n"
                "Если флагов нет — скажи прямо, что подозрений нет, не выдумывай."
            ),
        }],
    )
    parts = []
    for b in resp.content:
        if hasattr(b, "text"):
            parts.append(b.text)
    return "".join(parts).strip()
