#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi
chmod +x scripts/ingest_tokentx_top100.sh

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then echo "TOKEN required"; exit 1; fi
TOKEN="$TOKEN_ARG"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

# 1) Ingest head-only tokentx for the current Top-100 (script has its own head guard)
./scripts/ingest_tokentx_top100.sh "$TOKEN"

# 2) Summarize last 60 minutes (HTML with links + extra insights)
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
  SELECT
    wallet_address, in_amt, out_amt, txs,
    GREATEST(in_amt, out_amt) AS max_flow
  FROM agg_per_wallet
),
tot AS (
  SELECT
    COUNT(*) AS active_wallets,
    COALESCE(SUM(in_amt),  0) AS total_in,
    COALESCE(SUM(out_amt), 0) AS total_out,
    COALESCE(SUM(txs),     0) AS tx_rows
  FROM agg_per_wallet
),
largest_tx AS (
  SELECT
    tx_hash,
    value_18d AS val,
    block_time_utc
  FROM w
  ORDER BY value_18d DESC NULLS LAST
  LIMIT 1
),
top_lines AS (
  SELECT string_agg(
           format('<a href=\"${EXPL}/address/%s\">%s…%s</a> | IN:<code>%s</code> OUT:<code>%s</code> tx:<code>%s</code>',
                  wallet_address,
                  left(wallet_address, 6),
                  right(wallet_address, 4),
                  to_char(round(in_amt::numeric, 6), 'FM999999999990.000000'),
                  to_char(round(out_amt::numeric, 6), 'FM999999999990.000000'),
                  txs::text),
           E'\n'
         ) AS html
  FROM (
    SELECT wallet_address, in_amt, out_amt, txs
    FROM ranked
    ORDER BY max_flow DESC NULLS LAST, txs DESC, wallet_address ASC
    LIMIT 10
  ) s
)
SELECT
  to_char(now() AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI\"Z\"')     AS asof_utc,
  (SELECT active_wallets FROM tot)                                 AS active_wallets,
  (SELECT tx_rows FROM tot)                                        AS tx_rows,
  to_char((SELECT round(total_in::numeric, 6)  FROM tot), 'FM999999999990.000000') AS total_in,
  to_char((SELECT round(total_out::numeric, 6) FROM tot), 'FM999999999990.000000') AS total_out,
  to_char(((SELECT total_in FROM tot) - (SELECT total_out FROM tot)), 'FM999999999990.000000') AS net_flow,
  COALESCE((SELECT html FROM top_lines), '')                       AS lines,
  COALESCE((SELECT to_char(round(val::numeric,6),'FM999999999990.000000') FROM largest_tx), '') AS largest_val,
  COALESCE((SELECT tx_hash FROM largest_tx), '')                   AS largest_tx
;")

ASOF=$(echo    "$REPORT" | cut -d'|' -f1)
ACTIVE=$(echo  "$REPORT" | cut -d'|' -f2)
TXR=$(echo     "$REPORT" | cut -d'|' -f3)
INAMT=$(echo   "$REPORT" | cut -d'|' -f4)
OUTAMT=$(echo  "$REPORT" | cut -d'|' -f5)
NET=$(echo     "$REPORT" | cut -d'|' -f6)
LINES=$(echo   "$REPORT" | cut -d'|' -f7)
LVAL=$(echo    "$REPORT" | cut -d'|' -f8)
LTX=$(echo     "$REPORT" | cut -d'|' -f9)

TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">${TOKEN}</a>"
LTX_HTML=""
if [ -n "$LTX" ]; then
  LTX_HTML="<i>Largest tx:</i> <code>${LVAL}</code> — <a href=\"${EXPL}/tx/${LTX}\">$(echo \"$LTX\" | cut -c1-12)…</a>"
fi

MSG="<b>MERL Top100 activity</b> ⏱ <i>(last 60m)</i>
<i>As of:</i> <code>${ASOF}</code>
<i>Token:</i> ${TOKEN_LINK}
<i>Active wallets:</i> <code>${ACTIVE}</code>
<i>TX rows:</i> <code>${TXR}</code>
<i>Inflow:</i> <code>${INAMT}</code>
<i>Outflow:</i> <code>${OUTAMT}</code>
<i>Net flow:</i> <code>${NET}</code>
${LTX_HTML}

<b>Top movers</b> (by max IN/OUT)
${LINES}"

./scripts/notify_telegram.sh "$MSG" "HTML"
