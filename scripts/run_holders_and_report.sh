#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/run_holders_and_top100.sh
source scripts/lib_fmt.sh   # <-- same helpers you used in the probe

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

# 1) Run snapshot + Top100 (idempotent within bucket)
./scripts/run_holders_and_top100.sh "$TOKEN"

# 2) Header (single row, no aggregation of Top-10 here)
HDR=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH b AS (
  SELECT max(bucket_start_utc) AS ts
  FROM merlin_chain.holders_raw
  WHERE contract_address = lower('${TOKEN}')
),
agg AS (
  SELECT
    (SELECT count(*) FROM merlin_chain.holders_raw hr, b
      WHERE hr.contract_address=lower('${TOKEN}') AND hr.bucket_start_utc=b.ts) AS holders,
    (SELECT count(*) FROM merlin_chain.refined_wallet_top100 r, b
      WHERE r.contract_address=lower('${TOKEN}') AND r.bucket_start_utc=b.ts) AS top_rows
)
SELECT to_char((SELECT ts FROM b),'YYYY-MM-DD HH24:MI\"Z\"'),
       (SELECT holders FROM agg),
       (SELECT top_rows FROM agg);
")

BUCKET_UTC=$(echo "$HDR" | cut -d'|' -f1)
TOTAL_HOLDERS=$(echo "$HDR" | cut -d'|' -f2)
TOP_ROWS=$(echo "$HDR" | cut -d'|' -f3)

# 3) Top-10 as 10 separate rows (safe to parse)
mapfile -t TOP10 < <(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH b AS (
  SELECT max(bucket_start_utc) AS ts
  FROM merlin_chain.refined_wallet_top100
  WHERE contract_address = lower('${TOKEN}')
)
SELECT rnk, holder_address, balance::text
FROM merlin_chain.refined_wallet_top100 r, b
WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc=b.ts
ORDER BY rnk ASC
LIMIT 10;
")

# 4) Format like the probe
LINES=""
for row in "${TOP10[@]}"; do
  IFS='|' read -r rnk addr bal <<<"$row"
  bal="${bal//,/}"
  full2="$(two_dec "$bal")"
  full_commas="$(commify_decimal "$full2")"
  short2="$(humanize_decimal "$full2")"
  LINES+=$(printf '<b>#%s</b> %s\n<b>bal</b>: <code>%s</code> <i>(%s)</i>\n%s\n' \
           "$rnk" "$(alink "$addr")" "$full_commas" "$short2" "$SPACER_LINE")
done
unset IFS

TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">MERL</a>"

read -r -d '' MSG <<EOF || true
✅ <b>MERL Holders Snapshot</b> ❄️
<i>Bucket:</i> <code>${BUCKET_UTC}</code>  |  <i>Token:</i> ${TOKEN_LINK}
<i>Total holders:</i> <b>${TOTAL_HOLDERS}</b>  |  <i>Top100 rows:</i> <b>${TOP_ROWS}</b>

<b>🏆 Top 10 holders</b>
${LINES}
EOF

./scripts/notify_telegram.sh "$MSG" "HTML"
