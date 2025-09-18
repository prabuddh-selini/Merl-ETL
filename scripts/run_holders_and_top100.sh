#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Auto-load .env (export all vars while sourcing)
if [ -f ".env" ]; then set -a; source .env; set +a; fi

# Token from arg OR .env
TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then
  echo "TOKEN not provided. Pass as arg or set TOKEN in .env"; exit 1
fi
TOKEN="$TOKEN_ARG"

PY="./.venv/bin/python"

# ---- Tunables (defaults = FULL SNAPSHOT) ----
# You can override per-run: PAGE_SIZE=1000 MAX_PAGES=200000 ./scripts/run_holders_and_top100.sh
PAGE_SIZE=${PAGE_SIZE:-500}
MAX_PAGES=${MAX_PAGES:-100000}       # large cap → run until API returns short page
SNAP_TIMEOUT_SECS=${SNAP_TIMEOUT_SECS:-7200}  # 2h safety fuse for huge holder sets
ETL_RATE_LIMIT_QPS=${ETL_RATE_LIMIT_QPS:-3.0} # polite to API on full runs

# Ensure Python sees QPS
export ETL_RATE_LIMIT_QPS

echo "[holders_and_top100] TOKEN=$TOKEN"
echo "[holders_and_top100] PAGE_SIZE=$PAGE_SIZE MAX_PAGES=$MAX_PAGES QPS=$ETL_RATE_LIMIT_QPS"

# Compute current 6h bucket start (UTC)
BUCKET_START_UTC=$($PY - <<'PY'
import datetime as dt
u=dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
b=u.replace(hour=(u.hour//6)*6, minute=0, second=0, microsecond=0)
print(b.isoformat().replace('+00:00','Z'))
PY
)
TRIGGER_ID=$(date -u +"%Y/%m/%d-%H:%M")
echo "[holders_and_top100] BUCKET_START_UTC=$BUCKET_START_UTC TRIGGER_ID=$TRIGGER_ID"

# 1) holders snapshot (idempotent within bucket)
timeout "${SNAP_TIMEOUT_SECS}s" "$PY" merlin_etl.py holders_snapshot \
  --token "$TOKEN" \
  --bucket-start-utc "$BUCKET_START_UTC" \
  --trigger-id "$TRIGGER_ID" \
  --page-size "$PAGE_SIZE" \
  --max-pages "$MAX_PAGES"

# 2) derive Top-100 from latest bucket
./scripts/run_top100.sh "$TOKEN"

echo "[holders_and_top100] Done."
