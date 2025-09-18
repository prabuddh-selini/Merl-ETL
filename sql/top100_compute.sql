\set ON_ERROR_STOP on

-- Inputs: -v token=0x...
WITH latest AS (
  SELECT max(bucket_start_utc) AS b
  FROM merlin_chain.holders_raw
  WHERE contract_address = lower(:'token')
),
ranked AS (
  SELECT
    hr.bucket_start_utc,
    lower(hr.contract_address) AS contract_address,
    lower(hr.holder_address)   AS holder_address,
    hr.token_decimal,
    (hr.quantity_raw)::numeric AS balance_raw,
    ((hr.quantity_raw)::numeric / power(10::numeric, hr.token_decimal)) AS balance,
    RANK() OVER (ORDER BY ((hr.quantity_raw)::numeric / power(10::numeric, hr.token_decimal)) DESC,
                         lower(hr.holder_address) ASC) AS rnk
  FROM merlin_chain.holders_raw hr
  JOIN latest l ON l.b = hr.bucket_start_utc
  WHERE hr.contract_address = lower(:'token')
)
INSERT INTO merlin_chain.refined_wallet_top100
  (bucket_start_utc, contract_address, rnk, holder_address, token_decimal, balance_raw, balance, inserted_at)
SELECT
  r.bucket_start_utc, r.contract_address, r.rnk, r.holder_address,
  r.token_decimal, r.balance_raw, r.balance, now()
FROM ranked r
WHERE r.rnk <= 100
ON CONFLICT (bucket_start_utc, contract_address, holder_address) DO UPDATE
SET rnk          = EXCLUDED.rnk,
    token_decimal= EXCLUDED.token_decimal,
    balance_raw  = EXCLUDED.balance_raw,
    balance      = EXCLUDED.balance,
    inserted_at  = now();
