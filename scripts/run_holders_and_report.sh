#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/run_holders_and_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"

# 1) Run the snapshot + Top100 (idempotent within bucket)
./scripts/run_holders_and_top100.sh "$TOKEN"

# 2) Build a small summary from DB and send to Telegram
SUMMARY=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH b AS (
  SELECT max(bucket_start_utc) ts FROM merlin_chain.holders_raw
  WHERE contract_address=lower('$TOKEN')
),
agg AS (
  SELECT
    (SELECT count(*) FROM merlin_chain.holders_raw hr JOIN b ON hr.bucket_start_utc=b.ts
      WHERE hr.contract_address=lower('$TOKEN'))                                  AS holders,
    (SELECT count(*) FROM merlin_chain.refined_wallet_top100 r JOIN b ON r.bucket_start_utc=b.ts
      WHERE r.contract_address=lower('$TOKEN'))                                   AS top_rows
),
top5 AS (
  SELECT rnk, holder_address, balance
  FROM merlin_chain.refined_wallet_top100 r
  JOIN b ON r.bucket_start_utc=b.ts
  WHERE r.contract_address=lower('$TOKEN')
  ORDER BY rnk ASC
  LIMIT 5
)
SELECT to_char((SELECT ts FROM b),'YYYY-MM-DD HH24:MI\"Z\"')  AS bucket_utc,
       (SELECT holders FROM agg)                               AS holders,
       (SELECT top_rows FROM agg)                              AS top_rows,
       string_agg(format('#%s %s (%s)', rnk, holder_address, round(balance::numeric, 4)::text), E'\n')
FROM top5;
")

BUCKET_UTC=$(echo "$SUMMARY" | cut -d'|' -f1)
HOLDERS=$(echo     "$SUMMARY" | cut -d'|' -f2)
TOPROWS=$(echo     "$SUMMARY" | cut -d'|' -f3)
TOP5=$(echo        "$SUMMARY" | cut -d'|' -f4-)

MSG="MERL snapshot ✅
Bucket: ${BUCKET_UTC}
Total holders: ${HOLDERS}
Top100 rows: ${TOPROWS}

Top 5:
${TOP5}"

./scripts/notify_telegram.sh "$MSG" ""