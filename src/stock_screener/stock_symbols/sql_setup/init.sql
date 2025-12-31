-- =========================================
-- DATABASE: ashani_db
-- PURPOSE: Discounted stock screening
-- =========================================

-- ---------- STOCK MASTER ----------
CREATE TABLE IF NOT EXISTS stocks (
    symbol VARCHAR(20) PRIMARY KEY,
    company_name VARCHAR(150),
    industry VARCHAR(100),
    isin VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------- SLOW-CHANGING REFERENCE DATA ----------
CREATE TABLE IF NOT EXISTS valuation_reference (
    symbol VARCHAR(20) PRIMARY KEY,
    avg_5y_pe NUMERIC(10,2),
    discount_threshold_pct NUMERIC(5,2) DEFAULT 30.00,
    last_updated DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------- FAST-CHANGING SNAPSHOTS ----------
CREATE TABLE IF NOT EXISTS valuation_snapshots (
    id SERIAL PRIMARY KEY,

    symbol VARCHAR(20),
    snapshot_date DATE,

    current_price NUMERIC(12,2),
    current_pe NUMERIC(10,2),

    discount_vs_5y_avg NUMERIC(6,2),
    discount_vs_industry NUMERIC(6,2),

    is_discounted BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------- INDEXES ----------
CREATE INDEX IF NOT EXISTS idx_snap_symbol_date
ON valuation_snapshots (symbol, snapshot_date);

CREATE INDEX IF NOT EXISTS idx_discounted
ON valuation_snapshots (is_discounted);

-- ---------- MATERIALIZED VIEWS ----------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_discounted_latest AS
SELECT *
FROM valuation_snapshots
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM valuation_snapshots
)
AND is_discounted = true;
