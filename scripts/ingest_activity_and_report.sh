#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/ingest_tokentx_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"

# 1) Ingest head-only tokentx for the current Top-100 (script has its own head-block guard)
./scripts/ingest_tokentx_top100.sh "$TOKEN"

# 2) Summarize last 60 minutes of activity for those Top-100 wallets
REPORT=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH latest AS (
  SELECT max(bucket_start_utc) AS b
  FROM merlin_chain.refined_wallet_top100
  WHERE contract_address = lower('$TOKEN')
),
top100 AS (
  SELECT holder_address
  FROM merlin_chain.refined_wallet_top100 r
  JOIN latest l ON r.bucket_start_utc = l.b
  WHERE r.contract_address = lower('$TOKEN')
),
w AS (
  -- only transactions in last 60 minutes for the Top-100 holders
  SELECT *
  FROM merlin_chain.wallet_transactions
  WHERE contract_address = lower('$TOKEN')
    AND block_time_utc >= now() - interval '60 minutes'
    AND wallet_address IN (SELECT holder_address FROM top100)
),
agg_per_wallet AS (
  SELECT
    wallet_address,
    SUM(CASE WHEN wallet_address = to_address   THEN value_18d ELSE 0 END) AS in_amt,
    SUM(CASE WHEN wallet_address = from_address THEN value_18d ELSE 0 END) AS out_amt,
    COUNT(*) AS txs
  FROM w
  GROUP BY wallet_address
),
ranked AS (
  SELECT
    wallet_address,
    in_amt,
    out_amt,
    txs,
    GREATEST(in_amt, out_amt) AS max_flow
  FROM agg_per_wallet
),
tot AS (
  SELECT
    COUNT(*)                                    AS active_wallets,
    COALESCE(SUM(in_amt),  0)                   AS total_in,
    COALESCE(SUM(out_amt), 0)                   AS total_out,
    COALESCE(SUM(txs),     0)                   AS tx_rows
  FROM agg_per_wallet
),
top_lines AS (
  SELECT string_agg(
           format('%s | IN:%s OUT:%s tx:%s',
                  wallet_address,
                  round(in_amt::numeric, 6)::text,
                  round(out_amt::numeric, 6)::text,
                  txs::text),
           E'\n'
         ) AS lines
  FROM (
    SELECT wallet_address, in_amt, out_amt, txs
    FROM ranked
    ORDER BY max_flow DESC NULLS LAST, txs DESC, wallet_address ASC
    LIMIT 10
  ) s
)
SELECT
  to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI\"Z\"')      AS asof_utc,
  (SELECT active_wallets FROM tot)                                  AS active_wallets,
  (SELECT tx_rows FROM tot)                                         AS tx_rows,
  (SELECT round(total_in::numeric, 6)::text  FROM tot)              AS total_in,
  (SELECT round(total_out::numeric, 6)::text FROM tot)              AS total_out,
  COALESCE((SELECT lines FROM top_lines), '')                       AS lines
;")

ASOF=$(echo   "$REPORT" | cut -d'|' -f1)
ACTIVE=$(echo "$REPORT" | cut -d'|' -f2)
TXR=$(echo    "$REPORT" | cut -d'|' -f3)
INAMT=$(echo  "$REPORT" | cut -d'|' -f4)
OUTAMT=$(echo "$REPORT" | cut -d'|' -f5)
LINES=$(echo  "$REPORT" | cut -d'|' -f6-)

MSG="MERL Top100 activity (last 60m) ⏱
As of: ${ASOF}
Active wallets: ${ACTIVE}
TX rows: ${TXR}
Inflow: ${INAMT}
Outflow: ${OUTAMT}

Top movers (by max IN/OUT):
${LINES}"

# Plain text send (no Markdown pitfalls)
./scripts/notify_telegram.sh "$MSG" ""
