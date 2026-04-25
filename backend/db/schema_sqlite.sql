-- KUS — схема БД для SQLite (local dev fallback).
-- Идемпотентна: можно запускать повторно.

CREATE TABLE IF NOT EXISTS org_directory (
    tin         TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    source      TEXT,
    updated_at  TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tenders (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_dataset      TEXT NOT NULL,
    lot_id              TEXT NOT NULL DEFAULT '',
    contract_id         TEXT NOT NULL DEFAULT '',
    title               TEXT,
    customer_tin        TEXT,
    customer_name       TEXT,
    winner_tin          TEXT,
    winner_name         TEXT,
    amount_uzs          REAL,
    amount_usd          REAL,
    currency_raw        TEXT,
    date                TEXT,
    category            TEXT,
    funding_source      TEXT,
    purchase_method     TEXT,
    is_direct_purchase  INTEGER NOT NULL DEFAULT 0,
    risk_score          INTEGER,
    risk_flags          TEXT,    -- JSON as text для UI с деталями (pair_wins, category_avg)
    flag_monopoly       INTEGER NOT NULL DEFAULT 0,
    flag_no_compete     INTEGER NOT NULL DEFAULT 0,
    flag_overpriced     INTEGER NOT NULL DEFAULT 0,
    ai_narrative        TEXT,
    ai_narrative_at     TEXT,
    raw                 TEXT NOT NULL,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (source_dataset, lot_id, contract_id)
);

CREATE INDEX IF NOT EXISTS idx_tenders_customer_tin  ON tenders (customer_tin);
CREATE INDEX IF NOT EXISTS idx_tenders_winner_tin    ON tenders (winner_tin);
CREATE INDEX IF NOT EXISTS idx_tenders_category      ON tenders (category);
CREATE INDEX IF NOT EXISTS idx_tenders_date          ON tenders (date);
CREATE INDEX IF NOT EXISTS idx_tenders_risk          ON tenders (risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_tenders_pair          ON tenders (customer_tin, winner_tin);

CREATE TABLE IF NOT EXISTS analytics_cache (
    category      TEXT PRIMARY KEY,
    avg_price     REAL NOT NULL,
    median_price  REAL,
    total_count   INTEGER NOT NULL,
    updated_at    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ingest_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_dataset  TEXT NOT NULL,
    rows_fetched    INTEGER,
    rows_inserted   INTEGER,
    rows_updated    INTEGER,
    error           TEXT,
    started_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_ingest_log_dataset ON ingest_log (source_dataset, started_at DESC);
