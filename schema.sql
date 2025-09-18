-- =============================================================================
-- Merlin Chain: Initial Schema & Tables (PostgreSQL 17)
-- =============================================================================
-- 1) Schema
CREATE SCHEMA IF NOT EXISTS merlin_chain AUTHORIZATION admin_user;
-- 2) Helpful extension (optional, for future UUIDs if needed)
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- 3) Common updated_at trigger
CREATE OR REPLACE FUNCTION merlin_chain.set_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END$$;
-- =============================================================================
-- A) Block-by-Timestamp lookups (hourly job)
--    API: /api?module=block&action=getblocknobytime&timestamp=...&closest=after
--    Store unix_ts, ts_utc, closest('after'/'before'), block_number, api status
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.block_check (
  id              BIGSERIAL PRIMARY KEY,
  unix_ts         BIGINT       NOT NULL,                 -- input unix seconds
  ts_utc          TIMESTAMPTZ  NOT NULL,                 -- same in UTC
  closest         TEXT         NOT NULL CHECK (closest IN ('before','after')),
  block_number    BIGINT       NOT NULL,
  api_status      TEXT,                                   -- e.g. "1"
  api_message     TEXT,                                   -- e.g. "OK"
  inserted_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  UNIQUE (unix_ts, closest)
);
CREATE INDEX IF NOT EXISTS idx_block_check_ts_utc
  ON merlin_chain.block_check (ts_utc);
-- Backward-compatible view with your exact requested name
CREATE OR REPLACE VIEW "Merlin_Block_check" AS
SELECT * FROM merlin_chain.block_check;
-- =============================================================================
-- B) Holders_raw (6-hour job)
--    API: /api?module=token&action=tokenholderlist&contractaddress=...&page=...
--    Stores a "trigger_id" bucket (e.g., 2025/09/12-12am) and canonical bucket_start_utc
--    quantity_raw = integer units; quantity_18d = adjusted for 18 decimals
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.holders_raw (
  id                 BIGSERIAL     PRIMARY KEY,
  trigger_id         TEXT          NOT NULL,  -- e.g., "2025/09/12-12am|6am|12pm|6pm"
  bucket_start_utc   TIMESTAMPTZ   NOT NULL,  -- normalized 00:00/06:00/12:00/18:00 UTC
  contract_address   TEXT          NOT NULL,
  holder_address     TEXT          NOT NULL,
  token_decimal      SMALLINT      NOT NULL DEFAULT 18,
  quantity_raw       NUMERIC(78,0) NOT NULL,  -- raw integer string from API
  -- Adjusted to 18 decimals (your requirement)
  quantity_18d       NUMERIC(78,18) GENERATED ALWAYS AS
                     (quantity_raw / POWER(10::NUMERIC, 18)) STORED,
  inserted_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
  UNIQUE (bucket_start_utc, contract_address, holder_address)
);
CREATE INDEX IF NOT EXISTS idx_holders_raw_contract_qty
  ON merlin_chain.holders_raw (contract_address, quantity_18d DESC);
CREATE INDEX IF NOT EXISTS idx_holders_raw_holder
  ON merlin_chain.holders_raw (holder_address);
CREATE INDEX IF NOT EXISTS idx_holders_raw_bucket
  ON merlin_chain.holders_raw (bucket_start_utc);
CREATE TRIGGER holders_raw_set_updated
  BEFORE UPDATE ON merlin_chain.holders_raw
  FOR EACH ROW EXECUTE FUNCTION merlin_chain.set_updated_at();
-- =============================================================================
-- C) refined_wallet_top100 (Top-100 snapshot per token per 6h bucket)
--    One row per (bucket_start_utc, contract_address, holder_address)
--    rnk is 1..100 within each (bucket, token)
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.refined_wallet_top100 (
  bucket_start_utc  TIMESTAMPTZ   NOT NULL,
  contract_address  TEXT          NOT NULL,   -- lowercased 0x...
  rnk               INT           NOT NULL,   -- 1..100
  holder_address    TEXT          NOT NULL,   -- lowercased 0x...
  token_decimal     SMALLINT      NOT NULL,
  balance_raw       NUMERIC(78,0) NOT NULL,   -- base units from explorer
  balance           NUMERIC(78,18) NOT NULL,  -- human units (balance_raw / 10^decimals)
  inserted_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
  CONSTRAINT refined_wallet_top100_uniq
    UNIQUE (bucket_start_utc, contract_address, holder_address)
);

CREATE INDEX IF NOT EXISTS idx_top100_token_bucket_rnk
  ON merlin_chain.refined_wallet_top100 (contract_address, bucket_start_utc DESC, rnk);

ALTER TABLE merlin_chain.refined_wallet_top100 OWNER TO admin_user;
-- =============================================================================
-- D) wallet_transactions (hourly at x:30)
--    API: /api?module=account&action=tokentx&contractaddress=...&address=...&...
--    Store full ERC-20 transfer events per tracked wallet.
--    value_18d is adjusted to 18 decimals (per your requirement).
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.wallet_transactions (
  id                  BIGSERIAL     PRIMARY KEY,
  wallet_address      TEXT          NOT NULL,   -- the tracked wallet we queried for
  wallet_name         TEXT,
  wallet_tag          TEXT,
  contract_address    TEXT          NOT NULL,
  block_number        BIGINT        NOT NULL,
  block_time_unix     BIGINT        NOT NULL,   -- timeStamp from API
  block_time_utc      TIMESTAMPTZ   NOT NULL,   -- derived from block_time_unix
  tx_hash             TEXT          NOT NULL,
  nonce               BIGINT,
  block_hash          TEXT,
  from_address        TEXT          NOT NULL,
  to_address          TEXT          NOT NULL,
  value_raw           NUMERIC(78,0) NOT NULL,
  value_18d           NUMERIC(78,18) GENERATED ALWAYS AS
                     (value_raw / POWER(10::NUMERIC, 18)) STORED,
  token_name          TEXT,
  token_symbol        TEXT,
  token_decimal       SMALLINT      DEFAULT 18,
  transaction_index   INTEGER,
  gas                 BIGINT,
  gas_price           NUMERIC(40,0),
  gas_used            BIGINT,
  cumulative_gas_used BIGINT,
  input               TEXT,
  confirmations       BIGINT,
  ingested_at         TIMESTAMPTZ   NOT NULL DEFAULT now(),
  -- De-dupe safeguard for repeated pages/retries
  UNIQUE (tx_hash, contract_address, wallet_address, value_raw, COALESCE(transaction_index, 0))
);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_wallet_time
  ON merlin_chain.wallet_transactions (wallet_address, block_time_utc DESC);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_contract
  ON merlin_chain.wallet_transactions (contract_address);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_hash
  ON merlin_chain.wallet_transactions (tx_hash);
-- =============================================================================
-- E) ingestion_cursors (internal bookkeeping)
--    Track last scanned block/page per stream (e.g., 'holders:MERL', 'tx:<addr>')
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.ingestion_cursors (
  stream             TEXT         PRIMARY KEY,
  last_scanned_block BIGINT,
  last_page          INTEGER,
  updated_at_utc     TIMESTAMPTZ  NOT NULL DEFAULT now(),
  metadata           JSONB        NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_cursors_updated
  ON merlin_chain.ingestion_cursors (updated_at_utc DESC);
-- =============================================================================
-- F) job_runs (optional audit of ETL executions)
-- =============================================================================
CREATE TABLE IF NOT EXISTS merlin_chain.job_runs (
  id            BIGSERIAL    PRIMARY KEY,
  job_name      TEXT         NOT NULL,  -- e.g., 'block_check_hourly', 'holders_6h', 'wallet_tx_hourly'
  scheduled_for TIMESTAMPTZ  NOT NULL,
  started_at    TIMESTAMPTZ,
  finished_at   TIMESTAMPTZ,
  status        TEXT         NOT NULL CHECK (status IN ('success','error','partial')) DEFAULT 'success',
  stats         JSONB        NOT NULL DEFAULT '{}'::jsonb,
  error         TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_runs_name_time
  ON merlin_chain.job_runs (job_name, scheduled_for DESC);
-- =============================================================================
-- G) Ownership & Privileges
-- =============================================================================
ALTER SCHEMA merlin_chain OWNER TO admin_user;
GRANT USAGE ON SCHEMA merlin_chain TO admin_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA merlin_chain TO admin_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA merlin_chain
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO admin_user;
-- =============================================================================
-- H) Quick Smoke Checks (safe to run)
-- =============================================================================
-- SELECT current_user, current_database();
-- SELECT schema_name FROM information_schema.schemata WHERE schema_name='merlin_chain';
-- \dt merlin_chain.*
