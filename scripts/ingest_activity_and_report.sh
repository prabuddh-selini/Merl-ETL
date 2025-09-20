#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi

# Helpers + link formatters (same ones you used in the probe)
source scripts/lib_fmt.sh
chmod +x scripts/ingest_tokentx_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"
TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">MERL</a>"

# 1) Ingest head-only tokentx for the current Top-100 (script has its own head guard)
./scripts/ingest_tokentx_top100.sh "$TOKEN"

# 2) Pull totals for the last 60 minutes (only Top-100 wallets)
META=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH latest AS (
  SELECT max(bucket_start_utc) AS b
  FROM merlin_chain.refined_wallet_top100
  WHERE contract_address = lower('${TOKEN}')
),
top100 AS (
  SELECT holder_address
  FROM merlin_chain.refined_wallet_top100 r
  JOIN latest l ON r.bucket_start_utc = l.b
  WHERE r.contract_address = lower('${TOKEN}')
),
w AS (
  SELECT *
  FROM merlin_chain.wallet_transactions
  WHERE contract_address = lower('${TOKEN}')
    AND block_time_utc >= now() - interval '60 minutes'
    AND wallet_address IN (SELECT holder_address FROM top100)
),
agg_per_wallet AS (
  SELECT wallet_address,
         SUM(CASE WHEN wallet_address = to_address   THEN value_18d ELSE 0 END) AS in_amt,
         SUM(CASE WHEN wallet_address = from_address THEN value_18d ELSE 0 END) AS out_amt,
         COUNT(*) AS txs
  FROM w
  GROUP BY wallet_address
)
SELECT
  to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI\"Z\"') AS asof_utc,
  COALESCE((SELECT COUNT(*)    FROM agg_per_wallet), 0)        AS active_wallets,
  COALESCE((SELECT SUM(txs)    FROM agg_per_wallet), 0)        AS tx_rows,
  COALESCE((SELECT SUM(in_amt) FROM agg_per_wallet), 0)::text  AS total_in,
  COALESCE((SELECT SUM(out_amt)FROM agg_per_wallet), 0)::text  AS total_out,
  (COALESCE((SELECT SUM(in_amt)FROM agg_per_wallet), 0)
   - COALESCE((SELECT SUM(out_amt)FROM agg_per_wallet), 0))::text AS net_flow;
")

IFS='|' read -r ASOF ACTIVE TXR TIN TOUT NET <<< "$META"

# Format totals (2dp + commas + humanized)
INCF="$(commify_decimal "$(two_dec "$TIN")")"
OUTF="$(commify_decimal "$(two_dec "$TOUT")")"
NETF="$(commify_decimal "$(two_dec "$NET")")"

# 3) Top movers (by max IN/OUT), limit 10, plain values from SQL
readarray -t MOVERS < <(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH latest AS (
  SELECT max(bucket_start_utc) AS b
  FROM merlin_chain.refined_wallet_top100
  WHERE contract_address = lower('${TOKEN}')
),
top100 AS (
  SELECT holder_address
  FROM merlin_chain.refined_wallet_top100 r
  JOIN latest l ON r.bucket_start_utc = l.b
  WHERE r.contract_address = lower('${TOKEN}')
),
w AS (
  SELECT *
  FROM merlin_chain.wallet_transactions
  WHERE contract_address = lower('${TOKEN}')
    AND block_time_utc >= now() - interval '60 minutes'
    AND wallet_address IN (SELECT holder_address FROM top100)
),
agg AS (
  SELECT wallet_address,
         COALESCE(SUM(CASE WHEN wallet_address = to_address   THEN value_18d END), 0) AS in_amt,
         COALESCE(SUM(CASE WHEN wallet_address = from_address THEN value_18d END), 0) AS out_amt,
         COUNT(*) AS txs
  FROM w
  GROUP BY wallet_address
),
ranked AS (
  SELECT wallet_address, in_amt, out_amt, txs,
         GREATEST(in_amt, out_amt) AS max_flow
  FROM agg
)
SELECT wallet_address, in_amt::text, out_amt::text, txs::text
FROM ranked
ORDER BY max_flow DESC NULLS LAST, txs DESC, wallet_address ASC
LIMIT 10;
")

# Build the pretty HTML lines
LINES=""
rn=0
for row in "${MOVERS[@]}"; do
  [ -z "$row" ] && continue
  rn=$((rn+1))
  IFS='|' read -r addr in_raw out_raw txs <<< "$row"
  in2="$(two_dec "${in_raw//,/}")";  out2="$(two_dec "${out_raw//,/}")"
  in_full="$(commify_decimal "$in2")"; out_full="$(commify_decimal "$out2")"
  in_sh="$(humanize_decimal "$in2")"; out_sh="$(humanize_decimal "$out2")"
  LINES+=$(printf '<b>#%s</b> %s\n<b>IN</b>: <code>%s</code> <i>(%s)</i>   <b>OUT</b>: <code>%s</code> <i>(%s)</i>   <b>tx</b>: <code>%s</code>\n%s\n' \
           "$rn" "$(alink "$addr")" "$in_full" "$in_sh" "$out_full" "$out_sh" "$txs" "$SPACER_LINE")
done

# 4) Final message (HTML)
read -r -d '' MSG <<EOF || true
📈 <b>MERL Top100 activity</b> ⏱ <i>(last 60m)</i>
<i>As of:</i> <code>${ASOF}</code>  |  <i>Token:</i> ${TOKEN_LINK}
<b>Active wallets:</b> <code>${ACTIVE}</code>  |  <b>TX rows:</b> <code>${TXR}</code>
<b>Inflow:</b> <code>${INCF}</code>  |  <b>Outflow:</b> <code>${OUTF}</code>  |  <b>Net:</b> <code>${NETF}</code>

🏆 <b>Top movers</b> (by max IN/OUT)
${LINES}
EOF

./scripts/notify_telegram.sh "$MSG" "HTML"
