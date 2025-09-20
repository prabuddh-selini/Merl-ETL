#!/usr/bin/env bash
set -euo pipefail

EXPL="${EXPLORER_URL:-https://scan.merlinchain.io}"

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
