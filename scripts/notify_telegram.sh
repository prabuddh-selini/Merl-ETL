#!/usr/bin/env bash
set -euo pipefail
# usage: notify_telegram.sh "message text" [parse_mode]
MSG="${1:-}"
MODE="${2:-MarkdownV2}"

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
  echo "[notify] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"; exit 1
fi

# Escape for MarkdownV2 minimal cases
esc() {
  printf '%s' "$1" | sed -e 's/[_*[\]()`~>#+\-=|{}.!]/\\&/g'
}

TEXT="$MSG"
if [ "$MODE" = "MarkdownV2" ]; then
  TEXT="$(esc "$MSG")"
fi

curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
  -d "chat_id=${TELEGRAM_CHAT_ID}" \
  -d "text=${TEXT}" \
  -d "parse_mode=${MODE}" >/dev/null
