#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/ingest_tokentx_top100.sh
source scripts/lib_fmt.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

# 1) Ingest head-only tokentx for current Top-100
./scripts/ingest_tokentx_top100.sh "$TOKEN"

# 2) Summarize last 60 minutes (DB → pipe-friendly fields)
REPORT=$(psql "$DATABASE_URL" -t -A -F '|' -c "
WITH latest AS (
  SELECT max(bucket_start_utc) AS b
  FROM merlin_chain.refined_wallet_top100
  WHERE contract_address = lower('${TOKEN}')
),
top100 AS (
  SELECT holder_address
  FROM merlin_chain.refined_wallet_top100 r, latest l
  WHERE r.contract_address = lower('${TOKEN}') AND r.bucket_start_utc = l.b
),
w AS (
  SELECT *
  FROM merlin_chain.wallet_transactions
  WHERE contract_address = lower('${TOKEN}')
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
  SELECT wallet_address, in_amt, out_amt, txs, GREATEST(in_amt, out_amt) AS max_flow
  FROM agg_per_wallet
),
tot AS (
  SELECT COUNT(*) AS active_wallets,
         COALESCE(SUM(in_amt),  0) AS total_in,
         COALESCE(SUM(out_amt), 0) AS total_out,
         COALESCE(SUM(txs),     0) AS tx_rows
  FROM agg_per_wallet
),
top_lines AS (
  SELECT string_agg(
           format('%s|%s|%s|%s', wallet_address, COALESCE(in_amt,0), COALESCE(out_amt,0), txs::text),
           E'\n'
         ) AS rows
  FROM (
    SELECT wallet_address, in_amt, out_amt, txs
    FROM ranked
    ORDER BY max_flow DESC NULLS LAST, txs DESC, wallet_address ASC
    LIMIT 10
  ) s
)
SELECT
  to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI\"Z\"'),
  (SELECT active_wallets FROM tot),
  (SELECT tx_rows FROM tot),
  (SELECT total_in FROM tot),
  (SELECT total_out FROM tot),
  ((SELECT total_in FROM tot) - (SELECT total_out FROM tot)),
  COALESCE((SELECT rows FROM top_lines),'')
;")

ASOF=$(echo "$REPORT" | cut -d'|' -f1)
ACTIVE=$(echo "$REPORT" | cut -d'|' -f2)
TXR=$(echo "$REPORT" | cut -d'|' -f3)
INF=$(echo "$REPORT" | cut -d'|' -f4)
OUTF=$(echo "$REPORT" | cut -d'|' -f5)
NET=$(echo "$REPORT" | cut -d'|' -f6)
RAW_LINES=$(echo "$REPORT" | cut -d'|' -f7-)

INCF="$(commify_decimal "$(two_dec "$INF")")"
OUTCF="$(commify_decimal "$(two_dec "$OUTF")")"
NETF="$(commify_decimal "$(two_dec "$NET")")"
TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">MERL</a>"

# Build Top movers lines exactly like probe
LINES=""
IFS=$'\n'
rn=0
for row in $RAW_LINES; do
  IFS='|' read -r addr in out txs <<<"$row"
  rn=$((rn+1))
  in2="$(two_dec "${in//,/}")";  out2="$(two_dec "${out//,/}")"
  in_full="$(commify_decimal "$in2")"; out_full="$(commify_decimal "$out2")"
  in_sh="$(humanize_decimal "$in2")"; out_sh="$(humanize_decimal "$out2")"
  LINES+=$(printf '<b>#%s</b> %s\n<b>IN</b>: <code>%s</code> <i>(%s)</i>   <b>OUT</b>: <code>%s</code> <i>(%s)</i>   <b>tx</b>: <code>%s</code>\n%s\n' \
           "$rn" "$(alink "$addr")" "$in_full" "$in_sh" "$out_full" "$out_sh" "$txs" "$SPACER_LINE")
done

read -r -d '' MSG <<EOF || true
📈 <b>MERL Top100 activity</b> ⏱ <i>(last 60m)</i>
<i>As of:</i> <code>${ASOF}</code>  |  <i>Token:</i> ${TOKEN_LINK}
<b>Active wallets:</b> <code>${ACTIVE}</code>  |  <b>TX rows:</b> <code>${TXR}</code>
<b>Inflow:</b> <code>${INCF}</code>  |  <b>Outflow:</b> <code>${OUTCF}</code>  |  <b>Net:</b> <code>${NETF}</code>

🏆 <b>Top movers</b> (by max IN/OUT)
${LINES}
EOF

./scripts/notify_telegram.sh "$MSG" "HTML"
