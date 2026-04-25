"""Конфиг через pydantic-settings (типизированный, читает .env)."""
from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    database_url: str = ""
    uzs_per_usd: float = 12500.0
    egov_base: str = "https://data.egov.uz"
    anthropic_api_key: str = ""
    perplexity_api_key: str = ""

    model_config = SettingsConfigDict(
        env_file=str(ROOT / "backend" / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()

# обратная совместимость со старыми импортами
DATABASE_URL = settings.database_url
UZS_PER_USD = settings.uzs_per_usd
EGOV_BASE = settings.egov_base

# Датасеты по умолчанию — топ-15 по релевантности и свежести из explore/report.json.
DEFAULT_DATASETS: list[tuple[str, str]] = [
    ("613eeda614665dbb8ec80453", "МинЭкономФин · прямые закупки"),
    ("6142ef46ba3615f6f07bca0f", "Агентство по управлению гос-активами"),
    ("64dc981eb04a41cb2e29d57b", "МинЭкономФин · все тендеры"),
    ("6225c27ed31e97c0521ec8a1", "Хокимият Сурхандарьи · аукционы земли"),
    ("61137447db32b99538e086fc", "Алмалыкский ГМК"),
    ("613854e512027c00f822b040", "Хокимият Ташкентской обл."),
    ("613ef3ee14665dbb8ec80460", "Министерство транспорта"),
    ("613efcf714665dbb8ec804b3", "Министерство здравоохранения"),
    ("613f1d7214665dbb8ec804c7", "Министерство строительства и ЖКХ"),
    ("613f2cdd14665dbb8ec80615", "Министерство цифровых технологий"),
    ("613f359314665dbb8ec8062d", "Министерство культуры"),
    ("61680320ec71c8d084498218", "Хокимият Ташкента · аукционы земли"),
    ("61430fa1ba3615f6f07bcaa4", "Агентство по делам молодёжи"),
    ("6107d14e2a2e256d868e86dc", "Министерство юстиции"),
    ("6107f1f32a2e256d868e8774", "Агентство дошкольного образования"),
]
