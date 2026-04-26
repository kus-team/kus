

CREATE EXTENSION IF NOT EXISTS pg_trgm;     
CREATE EXTENSION IF NOT EXISTS btree_gin;


CREATE TABLE IF NOT EXISTS org_directory (
    tin         TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    source      TEXT,                       
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS tenders (
    id                  BIGSERIAL PRIMARY KEY,
    source_dataset      TEXT NOT NULL,              
    lot_id              TEXT NOT NULL DEFAULT '',   
    contract_id         TEXT NOT NULL DEFAULT '',   
    title               TEXT,                       
    customer_tin        TEXT,                       
    customer_name       TEXT,                       
    winner_tin          TEXT,                       
    winner_name         TEXT,                       
    amount_uzs          NUMERIC(18,2),              
    amount_usd          NUMERIC(18,2),              
    currency_raw        TEXT,                       
    date                DATE,                       
    category            TEXT,                       
    funding_source      TEXT,                       
    purchase_method     TEXT,                       
    is_direct_purchase  BOOLEAN NOT NULL DEFAULT FALSE,
    risk_score          SMALLINT,                   
    risk_flags          JSONB,                     
    flag_monopoly       BOOLEAN NOT NULL DEFAULT FALSE,   
    flag_no_compete     BOOLEAN NOT NULL DEFAULT FALSE,
    flag_overpriced     BOOLEAN NOT NULL DEFAULT FALSE,
    ai_narrative        TEXT,                       
    ai_narrative_at     TIMESTAMPTZ,                
    raw                 JSONB NOT NULL,             
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


CREATE TABLE IF NOT EXISTS analytics_cache (
    category      TEXT PRIMARY KEY,
    avg_price     NUMERIC(18,2) NOT NULL,
    median_price  NUMERIC(18,2),
    total_count   INT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);


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


-- audit_log: кто что смотрел / какие жалобы инициировал / какие AI-объяснения сгенерированы.
-- Не для модерации — для прозрачности и аналитики самой платформы.
CREATE TABLE IF NOT EXISTS audit_log (
    id          BIGSERIAL PRIMARY KEY,
    action      TEXT NOT NULL,        -- 'view_tender' | 'view_check' | 'ai_explain' | 'complaint_open' | 'csv_export'
    target      TEXT,                 -- ID тендера / company / etc.
    ip          TEXT,                 -- может храниться или анонимизироваться по политике
    user_agent  TEXT,
    payload     JSONB,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log (action, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_target ON audit_log (target);


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
