#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/ingest_tokentx_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"

# 1) Ingest new tokentx for Top100 (head-only; lock is inside the script)
./scripts/ingest_tokentx_top100.sh "$TOKEN"

# 2) Summarize last 60 minutes of activity
REPORT=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH w AS (
  SELECT *
  FROM merlin_chain.wallet_transactions
  WHERE contract_address=lower('$TOKEN')
    AND block_time_utc >= now() - interval '60 minutes'
),
tot AS (
  SELECT count(*) AS tx_count,
         COALESCE(SUM(CASE WHEN wallet_address=to_address   THEN value_18d END),0) AS in_amt,
         COALESCE(SUM(CASE WHEN wallet_address=from_address THEN value_18d END),0) AS out_amt
  FROM w
),
big AS (
  SELECT wallet_address,
         SUM(CASE WHEN wallet_address=to_address   THEN value_18d ELSE 0 END) AS in_amt,
         SUM(CASE WHEN wallet_address=from_address THEN value_18d ELSE 0 END) AS out_amt,
         COUNT(*) AS txc
  FROM w
  GROUP BY wallet_address
  ORDER BY GREATEST(in_amt, out_amt) DESC NULLS LAST
  LIMIT 5
),
lines AS (
  SELECT string_agg(format('%s in=%.2f out=%.2f tx=%s', wallet_address, COALESCE(in_amt,0), COALESCE(out_amt,0), txc::text), E'\n')
  FROM big
)
SELECT (SELECT tx_count FROM tot),
       (SELECT in_amt FROM tot),
       (SELECT out_amt FROM tot),
       COALESCE((SELECT * FROM lines), '')
;")

TXC=$(echo "$REPORT" | cut -d'|' -f1)
INAMT=$(echo "$REPORT" | cut -d'|' -f2)
OUTAMT=$(echo "$REPORT" | cut -d'|' -f3)
TOPW=$(echo "$REPORT" | cut -d'|' -f4-)

MSG="MERL Top100 activity (last 60m) ⏱
Tx: ${TXC}
Inflow: ${INAMT}
Outflow: ${OUTAMT}

Top movers:
${TOPW}"

./scripts/notify_telegram.sh "$MSG"
