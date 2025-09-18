\set ON_ERROR_STOP on

-- 1) Build Top-100 for latest bucket into a temp table
DROP TABLE IF EXISTS tmp_top100;

CREATE TEMP TABLE tmp_top100 AS
WITH latest AS (
  SELECT max(bucket_start_utc) AS bucket_start_utc
  FROM merlin_chain.holders_raw
  WHERE contract_address = lower(:'token')
),
src AS (
  SELECT
    hr.bucket_start_utc,
    lower(hr.contract_address) AS contract_address,
    lower(hr.holder_address)   AS holder_address,
    hr.token_decimal,
    (hr.quantity_raw)::numeric AS quantity_raw_num,
    ((hr.quantity_raw)::numeric / power(10::numeric, hr.token_decimal)) AS balance
  FROM merlin_chain.holders_raw hr
  JOIN latest l ON l.bucket_start_utc = hr.bucket_start_utc
  WHERE hr.contract_address = lower(:'token')
),
ranked AS (
  SELECT
    bucket_start_utc, contract_address, holder_address,
    token_decimal, quantity_raw_num, balance,
    RANK() OVER (ORDER BY balance DESC, holder_address ASC) AS rnk
  FROM src
)
SELECT * FROM ranked WHERE rnk <= 100;

-- cache latest bucket & rowcount
SELECT max(bucket_start_utc) AS bucket FROM tmp_top100;
\gset
SELECT count(*) AS tcount FROM tmp_top100;
\gset

-- If no rows, exit gracefully
\if :tcount = 0
\echo 'No Top-100 rows for this token (holders_raw is empty for latest bucket). Nothing to do.'
\quit
\endif

-- 2) Discover destination column names & address types
WITH cols AS (
  SELECT column_name, data_type
  FROM information_schema.columns
  WHERE table_schema = 'merlin_chain'
    AND table_name   = 'refined_wallet_top100'
)
SELECT
  -- bucket column (optional)
  (SELECT column_name FROM cols WHERE column_name IN
     ('bucket_start_utc','bucket_utc','asof','bucket_ts','ts_bucket') LIMIT 1) AS col_bucket,
  -- contract column
  (SELECT column_name FROM cols WHERE column_name IN
     ('contract_address','token','token_address','contract') LIMIT 1)          AS col_contract,
  -- holder column
  (SELECT column_name FROM cols WHERE column_name IN
     ('holder_address','address','wallet_address','owner_address') LIMIT 1)    AS col_holder,
  -- rank column
  (SELECT column_name FROM cols WHERE column_name IN
     ('rnk','rank','position') LIMIT 1)                                        AS col_rank,
  -- decimals
  (SELECT column_name FROM cols WHERE column_name IN
     ('token_decimal','decimals','token_decimals') LIMIT 1)                    AS col_decimals,
  -- raw balance
  (SELECT column_name FROM cols WHERE column_name IN
     ('balance_raw','raw_balance','quantity_raw','qty_raw','balance_base_units') LIMIT 1) AS col_balance_raw,
  -- human balance
  (SELECT column_name FROM cols WHERE column_name IN
     ('balance','balance_ip','balance_decimal','qty') LIMIT 1)                 AS col_balance,
  -- inserted_at-like (optional)
  (SELECT column_name FROM cols WHERE column_name IN
     ('inserted_at','updated_at','created_at') LIMIT 1)                        AS col_inserted_at
\gset

-- sanity check: we need these 7 columns mapped
\if :{?col_contract} \else \echo 'Could not map contract column'; \quit 1 \endif
\if :{?col_holder}   \else \echo 'Could not map holder column';   \quit 1 \endif
\if :{?col_rank}     \else \echo 'Could not map rank column';     \quit 1 \endif
\if :{?col_decimals} \else \echo 'Could not map decimals column'; \quit 1 \endif
\if :{?col_balance_raw} \else \echo 'Could not map balance_raw column'; \quit 1 \endif
\if :{?col_balance}  \else \echo 'Could not map balance column';  \quit 1 \endif

-- detect address column types
SELECT CASE WHEN data_type='bytea' THEN 1 ELSE 0 END AS contract_is_bytea
FROM information_schema.columns
WHERE table_schema='merlin_chain' AND table_name='refined_wallet_top100' AND column_name=:'col_contract';
\gset

SELECT CASE WHEN data_type='bytea' THEN 1 ELSE 0 END AS holder_is_bytea
FROM information_schema.columns
WHERE table_schema='merlin_chain' AND table_name='refined_wallet_top100' AND column_name=:'col_holder';
\gset

-- presence flags
SELECT CASE WHEN :'col_bucket' <> '' THEN 1 ELSE 0 END AS has_bucket;
\gset
SELECT CASE WHEN :'col_inserted_at' <> '' THEN 1 ELSE 0 END AS has_inserted_at;
\gset

-- Build reusable expressions depending on address types
\if :contract_is_bytea = 1
\set token_rhs "decode(substring(lower(:'token') from 3), 'hex')::bytea"
\set contract_expr "decode(substring(t.contract_address from 3), 'hex')::bytea"
\else
\set token_rhs "lower(:'token')"
\set contract_expr "t.contract_address"
\endif

\if :holder_is_bytea = 1
\set holder_expr "decode(substring(t.holder_address from 3), 'hex')::bytea"
\else
\set holder_expr "t.holder_address"
\endif

-- 3) Delete current rows for this token (and bucket if present)
\if :has_bucket = 1
DELETE FROM merlin_chain.refined_wallet_top100
WHERE :col_bucket = :'bucket'
  AND :col_contract = :token_rhs;
\else
DELETE FROM merlin_chain.refined_wallet_top100
WHERE :col_contract = :token_rhs;
\endif

-- 4) Insert fresh Top-100
\if :has_bucket = 1
  \if :has_inserted_at = 1
INSERT INTO merlin_chain.refined_wallet_top100
  (:col_bucket, :col_contract, :col_rank, :col_holder, :col_decimals, :col_balance_raw, :col_balance, :col_inserted_at)
SELECT
  t.bucket_start_utc, :contract_expr, t.rnk, :holder_expr, t.token_decimal, t.quantity_raw_num, t.balance, now()
FROM tmp_top100 t;
  \else
INSERT INTO merlin_chain.refined_wallet_top100
  (:col_bucket, :col_contract, :col_rank, :col_holder, :col_decimals, :col_balance_raw, :col_balance)
SELECT
  t.bucket_start_utc, :contract_expr, t.rnk, :holder_expr, t.token_decimal, t.quantity_raw_num, t.balance
FROM tmp_top100 t;
  \endif
\else
  \if :has_inserted_at = 1
INSERT INTO merlin_chain.refined_wallet_top100
  (:col_contract, :col_rank, :col_holder, :col_decimals, :col_balance_raw, :col_balance, :col_inserted_at)
SELECT
  :contract_expr, t.rnk, :holder_expr, t.token_decimal, t.quantity_raw_num, t.balance, now()
FROM tmp_top100 t;
  \else
INSERT INTO merlin_chain.refined_wallet_top100
  (:col_contract, :col_rank, :col_holder, :col_decimals, :col_balance_raw, :col_balance)
SELECT
  :contract_expr, t.rnk, :holder_expr, t.token_decimal, t.quantity_raw_num, t.balance
FROM tmp_top100 t;
  \endif
\endif

DROP TABLE IF EXISTS tmp_top100;
