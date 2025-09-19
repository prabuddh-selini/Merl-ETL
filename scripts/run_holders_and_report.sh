#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/run_holders_and_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

# 1) Run the snapshot + Top100 (idempotent within bucket)
./scripts/run_holders_and_top100.sh "$TOKEN"

# 2) Build HTML summary from DB and send to Telegram (use <br>, not <br/>)
SUMMARY=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH bucket AS (
  SELECT max(bucket_start_utc) AS ts
  FROM merlin_chain.holders_raw
  WHERE contract_address = lower('${TOKEN}')
),
agg AS (
  SELECT
    (SELECT count(*) FROM merlin_chain.holders_raw hr, bucket b
      WHERE hr.contract_address = lower('${TOKEN}') AND hr.bucket_start_utc = b.ts) AS holders,
    (SELECT count(*) FROM merlin_chain.refined_wallet_top100 r, bucket b
      WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc = b.ts) AS top_rows
),
top5 AS (
  SELECT r.rnk, r.holder_address, r.balance
  FROM merlin_chain.refined_wallet_top100 r, bucket b
  WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc = b.ts
  ORDER BY r.rnk ASC
  LIMIT 5
),
lines AS (
  SELECT string_agg(
           format('&#35;%s <a href=\"${EXPL}/address/%s\">%s…%s</a> (<code>%s</code>)',
                  rnk,
                  holder_address,
                  left(holder_address, 6),
                  right(holder_address, 4),
                  to_char(round(balance::numeric, 4), 'FM999999999990.0000')
           ),
           '<br>'
         ) AS html
  FROM top5
)
SELECT
  to_char((SELECT ts FROM bucket), 'YYYY-MM-DD HH24:MI\"Z\"')  AS bucket_utc,
  (SELECT holders FROM agg)                                    AS holders,
  (SELECT top_rows FROM agg)                                   AS top_rows,
  COALESCE((SELECT html FROM lines), '')                       AS html_lines
;")

BUCKET_UTC=$(echo "$SUMMARY" | cut -d'|' -f1)
HOLDERS=$(echo     "$SUMMARY" | cut -d'|' -f2)
TOPROWS=$(echo     "$SUMMARY" | cut -d'|' -f3)
TOP5HTML=$(echo    "$SUMMARY" | cut -d'|' -f4-)

TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">${TOKEN}</a>"

MSG="<b>MERL snapshot</b> ✅<br>
<i>Bucket (UTC):</i> <code>${BUCKET_UTC}</code><br>
<i>Token:</i> ${TOKEN_LINK}<br>
<i>Total holders:</i> <code>${HOLDERS}</code><br>
<i>Top100 rows:</i> <code>${TOPROWS}</code><br><br>
<b>Top 5 holders</b><br>
${TOP5HTML}"

./scripts/notify_telegram.sh "$MSG" "HTML"
