#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then
  echo "TOKEN not provided. Pass as arg or set TOKEN in .env"; exit 1
fi
TOKEN="$TOKEN_ARG"

# prevent overlap runs
LOCKFILE="/tmp/merl-tokentx-$(echo "$TOKEN" | tr '[:upper:]' '[:lower:]').lock"
exec 9>"$LOCKFILE"
if ! flock -n 9; then
  echo "[tokentx] Another ingestion run is in progress for $TOKEN. Exiting."
  exit 0
fi

PY="./.venv/bin/python"

# Get block number at "now" so we don't backfill history
HEAD_BLOCK=$($PY - <<'PY'
import os, time, requests
BASE=os.getenv("MERLINSCAN_BASE_URL","https://scan.merlinchain.io/api")
KEY=os.getenv("MERLINSCAN_API_KEY")
ts=str(int(time.time()))
r=requests.get(BASE, params={"module":"block","action":"getblocknobytime","timestamp":ts,"closest":"after","api_key":KEY}, timeout=30)
r.raise_for_status()
print(r.json().get("result") or 0)
PY
)

# Latest bucket's Top-100 holders
ADDRESSES=$(psql "$DATABASE_URL" -t -A -c "
  WITH latest AS (
    SELECT max(bucket_start_utc) AS b
    FROM merlin_chain.refined_wallet_top100
    WHERE contract_address = lower('$TOKEN')
  )
  SELECT holder_address
  FROM merlin_chain.refined_wallet_top100 r
  JOIN latest l ON l.b = r.bucket_start_utc
  WHERE r.contract_address = lower('$TOKEN')
  ORDER BY r.rnk ASC;")

# Ingest tokentx for each (resumable via ingestion_cursors)
for WALLET in $ADDRESSES; do
  $PY merlin_etl.py wallet_tokentx \
    --wallet "$WALLET" \
    --token  "$TOKEN" \
    --startblock "$HEAD_BLOCK" \
    --page-size 200
done
