#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f ".env" ]; then set -a; source .env; set +a; fi

MODE="${1:-snapshot}"
EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"
TOKEN="${TOKEN:-0x5c46bFF4B38dc1EAE09C5BAc65872a1D8bc87378}"

# ---------- helpers ----------
short() { local a="$1"; echo "${a:0:6}…${a: -4}"; }
alink() { local a="$1"; echo "<a href=\"${EXPL}/address/${a}\">$(short "$a")</a>"; }
SPACER_LINE="•"

two_dec() {
  local x="$1"; local PY="./.venv/bin/python"; [ -x "$PY" ] || PY="python3"
  "$PY" - <<'PY' "$x"
import sys, decimal as d
d.getcontext().prec = 50
s = sys.argv[1].replace(',', '')
try: n = d.Decimal(s)
except Exception: print(s); raise SystemExit(0)
print(n.quantize(d.Decimal('0.01')))
PY
}

commify_decimal() {
  local x="$1"; local PY="./.venv/bin/python"; [ -x "$PY" ] || PY="python3"
  "$PY" - <<'PY' "$x"
import sys
s = sys.argv[1].strip()
sign = ''
if s.startswith('-'):
    sign, s = '-', s[1:]
if '.' in s:
    intp, frac = s.split('.', 1)
else:
    intp, frac = s, ''
intp = intp.replace(',', '') or '0'
try: intp_fmt = f"{int(intp):,}"
except ValueError: intp_fmt = intp
print(sign + intp_fmt + ('.' + frac if frac else ''))
PY
}

humanize_decimal() {
  local x="$1"; local PY="./.venv/bin/python"; [ -x "$PY" ] || PY="python3"
  "$PY" - <<'PY' "$x"
import sys, decimal as d
d.getcontext().prec = 50
s = sys.argv[1].replace(',', '')
try: n = d.Decimal(s)
except Exception: print(s); raise SystemExit(0)
absn = abs(n)
units = [(d.Decimal('1e12'),'T'),(d.Decimal('1e9'),'B'),(d.Decimal('1e6'),'M'),(d.Decimal('1e3'),'K')]
for k,suf in units:
    if absn >= k:
        print(f"{(n/k).quantize(d.Decimal('0.01'))}{suf}")
        break
else:
    print(n.quantize(d.Decimal('0.01')))
PY
}

send() { ./scripts/notify_telegram.sh "$1" "HTML"; }

# ================= SNAPSHOT =================
if [ "$MODE" = "snapshot" ]; then
  BUCKET_UTC="2025-09-19 12:00Z"
  TOTAL_HOLDERS="328,901"
  TOP_ROWS="100"

  mapfile -t TOP10 < <(cat <<'EOF'
1|0xa2ff5352b918e2ff15798dfc2fc8d0782bad8443|701773784.8270
2|0x635321777ec7f6752ac2dcb09fb721867b326813|305082424.5430
3|0xdc94d3f17e9d9663bb967c971e443f5179a44536|105264064.4311
4|0xbd37daf8519476cf99c336ddda954278394f45ab|88851830.0000
5|0x930365fdf397f9c87065a74d4fb5508b58d5e8e1|88200000.0000
6|0x004b6e753006f85103a0907add94738adc3a0623|5480997.0000
7|0x0057f26cde602c764617849bec09c87b8995c40b|6668.8988
8|0x005d78897d5be4614ef183083c03dcde3522acb8|6532.3725
9|0x00133d35c834d25f17331ef2368e57cb3657ad95|4299.5700
10|0x00530d39ce7aca582954c1c0aa9d2c78741b039a|3487.8815
EOF
)

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

  TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">MERL</a>"

  read -r -d '' MSG <<EOF || true
✅ <b>MERL Holders Snapshot</b> ❄️
<i>Bucket:</i> <code>${BUCKET_UTC}</code>  |  <i>Token:</i> ${TOKEN_LINK}
<i>Total holders:</i> <b>${TOTAL_HOLDERS}</b>  |  <i>Top100 rows:</i> <b>${TOP_ROWS}</b>

<b>🏆 Top 10 holders</b>
${LINES}
EOF

  send "$MSG"; exit 0
fi

# ================= ACTIVITY =================
if [ "$MODE" = "activity" ]; then
  ASOF="2025-09-19 18:02Z"; ACTIVE="7"; TXR="23"
  INF="444123.456789"; OUTF="820.089425"; NET="-375966.632636"
  INCF="$(commify_decimal "$(two_dec "$INF")")"
  OUTF="$(commify_decimal "$(two_dec "$OUTF")")"
  NETF="$(commify_decimal "$(two_dec "$NET")")"
  TOKEN_LINK="<a href=\"${EXPL}/token/${TOKEN}\">MERL</a>"

  # addr | in | out | txs (static sample)
  mapfile -t MOVERS < <(cat <<'EOF'
0x2426191006f378bf33445f87938d355096ee2e8c|0.000000|820.089425|3
0xf89d7b9c864f589bbf53a82105107622b35eaa40|242586.368334|0.000000|2
0x503dea2a76f3b127f75fabb367d48f3d71f95341|2000000.000000|6347444.444922|5
0x5ef1a2c28e58d8abcf1fe2292d4293d0475559ec|120.000000|0.000000|1
0x332ff549cd256b45f2abe916939b82e399731d13|0.000000|389.121697|1
EOF
)

  LINES=""
  rn=0
  for row in "${MOVERS[@]}"; do
    rn=$((rn+1))
    IFS='|' read -r addr in out txs <<<"$row"
    in2="$(two_dec "${in//,/}")";  out2="$(two_dec "${out//,/}")"
    in_full="$(commify_decimal "$in2")"; out_full="$(commify_decimal "$out2")"
    in_sh="$(humanize_decimal "$in2")"; out_sh="$(humanize_decimal "$out2")"
    # No trophy per line; clean two-line block per wallet
    LINES+=$(printf '<b>#%s</b> %s\n<b>IN</b>: <code>%s</code> <i>(%s)</i>   <b>OUT</b>: <code>%s</code> <i>(%s)</i>   <b>tx</b>: <code>%s</code>\n%s\n' \
             "$rn" "$(alink "$addr")" "$in_full" "$in_sh" "$out_full" "$out_sh" "$txs" "$SPACER_LINE")
  done

  read -r -d '' MSG <<EOF || true
📈 <b>MERL Top100 activity</b> ⏱ <i>(last 60m)</i>
<i>As of:</i> <code>${ASOF}</code>  |  <i>Token:</i> ${TOKEN_LINK}
<b>Active wallets:</b> <code>${ACTIVE}</code>  |  <b>TX rows:</b> <code>${TXR}</code>
<b>Inflow:</b> <code>${INCF}</code>  |  <b>Outflow:</b> <code>${OUTF}</code>  |  <b>Net:</b> <code>${NETF}</code>

🏆 <b>Top movers</b> (by max IN/OUT)
${LINES}
EOF

  send "$MSG"; exit 0
fi

echo "Usage: $0 [snapshot|activity]"
exit 1
