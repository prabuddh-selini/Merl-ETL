#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/run_holders_and_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

# 1) Full snapshot + Top100 (idempotent per 6h bucket)
./scripts/run_holders_and_top100.sh "$TOKEN"

# 2) Compose report exactly like your working activity script (HTML, newlines, <a>, <code>)
REPORT=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH b AS (
  SELECT max(bucket_start_utc) AS ts
  FROM merlin_chain.holders_raw
  WHERE contract_address = lower('${TOKEN}')
),
agg AS (
  SELECT
    (SELECT count(*) FROM merlin_chain.holders_raw hr, b
      WHERE hr.contract_address = lower('${TOKEN}') AND hr.bucket_start_utc = b.ts) AS holders,
    (SELECT count(*) FROM merlin_chain.refined_wallet_top100 r, b
      WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc = b.ts) AS top_rows
),
top5 AS (
  SELECT rnk, holder_address, balance
  FROM merlin_chain.refined_wallet_top100 r, b
  WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc = b.ts
  ORDER BY rnk ASC
  LIMIT 5
),
lines AS (
  SELECT string_agg(
           format('#%s <a href=\"${EXPL}/address/%s\">%s…%s</a> | bal:<code>%s</code>',
                  rnk,
                  holder_address,
                  left(holder_address, 6),
                  right(holder_address, 4),
                  to_char(round(balance::numeric, 6), 'FM999999999990.000000')),
           E'\n'
         ) AS html
  FROM top5
)
SELECT
  to_char((SELECT ts FROM b), 'YYYY-MM-DD HH24:MI\"Z\"') AS bucket_utc,
  (SELECT holders  FROM agg)                              AS holders,
  (SELECT top_rows FROM agg)                              AS top_rows,
  COALESCE((SELECT html FROM lines), '')                  AS lines
;")

BUCKET=$(echo "$REPORT" | cut -d'|' -f1)
HOLDERS=$(echo "$REPORT" | cut -d'|' -f2)
TOPROWS=$(echo "$REPORT" | cut -d'|' -f3)
LINES=$(echo  "$REPORT" | cut -d'|' -f4-)

TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">${TOKEN}</a>"

MSG="<b>MERL snapshot</b> ✅
<i>Bucket (UTC):</i> <code>${BUCKET}</code>
<i>Token:</i> ${TOKEN_LINK}
<i>Total holders:</i> <code>${HOLDERS}</code>
<i>Top100 rows:</i> <code>${TOPROWS}</code>

<b>Top 5 holders</b>
${LINES}"

./scripts/notify_telegram.sh "$MSG" "HTML"
