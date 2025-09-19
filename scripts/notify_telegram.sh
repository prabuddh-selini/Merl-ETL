#!/usr/bin/env bash
set -euo pipefail

MSG="${1:-}"
MODE="${2:-}"   # empty => plain text (safest)

if [ -z "${TELEGRAM_BOT_TOKEN:-}" ] || [ -z "${TELEGRAM_CHAT_ID:-}" ]; then
  echo "[notify] TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set"; exit 1
fi

API="https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage"

# Minimal escaping only if MarkdownV2 explicitly requested (Telegram’s special chars)
esc() { printf '%s' "$1" | sed -e 's/[_*\[\]()`~>#+\-=|{}.!]/\\&/g'; }

TEXT="$MSG"
if [ "${MODE:-}" = "MarkdownV2" ]; then
  TEXT="$(esc "$MSG")"
fi

ARGS=( -sS -X POST "$API" -d "chat_id=${TELEGRAM_CHAT_ID}" -d "text=${TEXT}" )
[ -n "${MODE:-}" ] && ARGS+=( -d "parse_mode=${MODE}" )

RESP="$(curl "${ARGS[@]}")" || { echo "[notify] HTTP error from Telegram"; exit 1; }
echo "[notify] Telegram response: $RESP"
echo "$RESP" | grep -q '"ok":true' || { echo "[notify] Telegram returned failure"; exit 1; }
