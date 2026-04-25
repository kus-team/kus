-- HalolMap — схема БД
-- Идемпотентна: можно запускать повторно.

CREATE EXTENSION IF NOT EXISTS pg_trgm;     -- для ILIKE-поиска по названию
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Справочник ИНН → название (заполняется по мере появления данных)
CREATE TABLE IF NOT EXISTS org_directory (
    tin         TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    source      TEXT,                       -- 'tender_row' | 'manual' | 'external_api'
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Основная таблица тендеров
CREATE TABLE IF NOT EXISTS tenders (
    id                  BIGSERIAL PRIMARY KEY,
    source_dataset      TEXT NOT NULL,              -- structId исходного датасета
    lot_id              TEXT NOT NULL DEFAULT '',   -- Lotraqami (для UNIQUE без COALESCE)
    contract_id         TEXT NOT NULL DEFAULT '',   -- Shartnomaraqami
    title               TEXT,                       -- Predmeti... (наименование)
    customer_tin        TEXT,                       -- ИНН заказчика
    customer_name       TEXT,                       -- из org_directory, подхватывается
    winner_tin          TEXT,                       -- ИНН победителя/поставщика
    winner_name         TEXT,                       -- имя поставщика
    amount_uzs          NUMERIC(18,2),              -- нормализовано к сумам
    amount_usd          NUMERIC(18,2),              -- = amount_uzs / UZS_PER_USD (на момент загрузки)
    currency_raw        TEXT,                       -- исходная валюта
    date                DATE,                       -- дата контракта
    category            TEXT,                       -- Kategoriyasi
    funding_source      TEXT,                       -- Moliyalashtirish manbai
    purchase_method     TEXT,                       -- XaridTuri / Togridantogri…
    is_direct_purchase  BOOLEAN NOT NULL DEFAULT FALSE,
    risk_score          SMALLINT,                   -- 0..100
    risk_flags          JSONB,                      -- {"monopoly":true,"overpriced":true,"no_compete":true,"pair_wins":N,"category_avg":..}
    flag_monopoly       BOOLEAN NOT NULL DEFAULT FALSE,   -- денормализовано для быстрых SUM(CASE)
    flag_no_compete     BOOLEAN NOT NULL DEFAULT FALSE,
    flag_overpriced     BOOLEAN NOT NULL DEFAULT FALSE,
    ai_narrative        TEXT,                       -- кеш LLM-объяснения «почему подозрительно»
    ai_narrative_at     TIMESTAMPTZ,                -- когда сгенерировано
    raw                 JSONB NOT NULL,             -- исходная строка датасета (для отладки)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_dataset, lot_id, contract_id)
);

CREATE INDEX IF NOT EXISTS idx_tenders_customer_tin  ON tenders (customer_tin);
CREATE INDEX IF NOT EXISTS idx_tenders_winner_tin    ON tenders (winner_tin);
CREATE INDEX IF NOT EXISTS idx_tenders_category      ON tenders (category);
CREATE INDEX IF NOT EXISTS idx_tenders_date          ON tenders (date);
CREATE INDEX IF NOT EXISTS idx_tenders_risk          ON tenders (risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_tenders_title_trgm    ON tenders USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_tenders_pair          ON tenders (customer_tin, winner_tin);

-- Кэш средних цен по категории (для расчёта «завышенной цены»)
CREATE TABLE IF NOT EXISTS analytics_cache (
    category      TEXT PRIMARY KEY,
    avg_price     NUMERIC(18,2) NOT NULL,
    median_price  NUMERIC(18,2),
    total_count   INT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Лог загрузок датасетов (чтобы видеть когда что обновлялось)
CREATE TABLE IF NOT EXISTS ingest_log (
    id              BIGSERIAL PRIMARY KEY,
    source_dataset  TEXT NOT NULL,
    rows_fetched    INT,
    rows_inserted   INT,
    rows_updated    INT,
    error           TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_dataset ON ingest_log (source_dataset, started_at DESC);

-- Автообновление updated_at
CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tenders_updated_at ON tenders;
CREATE TRIGGER trg_tenders_updated_at
    BEFORE UPDATE ON tenders
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();
