#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi

TOKEN_ARG="${1:-${TOKEN:-}}"
if [ -z "$TOKEN_ARG" ]; then
  echo "TOKEN not provided. Pass as arg or set TOKEN in .env"; exit 1
fi
TOKEN="$TOKEN_ARG"

psql "$DATABASE_URL" -v "token=$TOKEN" -f sql/top100_compute.sql
