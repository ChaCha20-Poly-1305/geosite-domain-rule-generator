#!/usr/bin/env bash
# Outputs: ip-geo-detect-domain.list

set -euo pipefail

URL="https://github.com/MetaCubeX/meta-rules-dat/raw/refs/heads/meta/geo/geosite/category-ip-geo-detect.list"
OUTPUT="output/ip-geo-detect-domain.list"
NAME="ip-geo-detect-domain.list"
AUTHOR="ChaCha20-Poly-1305"
REPO="https://github.com/ChaCha20-Poly-1305/geosite-domain-rule-generator"
UPDATED=$(TZ="Asia/Shanghai" date "+%Y-%m-%d %H:%M:%S %Z")

echo "Downloading from: $URL"
RAW=$(curl -fsSL "$URL")

# Process lines: filter, strip, convert, then sort alphabetically by domain
RULES=$(
  echo "$RAW" | while IFS= read -r line; do
    # Strip +. prefix if present
    domain="${line#+.}"

    # Skip blank lines
    [[ -z "${domain// /}" ]] && continue

    # Skip .ru TLDs
    [[ "$domain" == *.ru ]] && continue

    # Skip lines containing blacklisted keywords
    [[ "$domain" == *yandex* ]] && continue
    [[ "$domain" == *2ip* ]] && continue

    echo "DOMAIN-SUFFIX,${domain}"
  done | sort -t',' -k2
)

TOTAL=$(echo "$RULES" | grep -c 'DOMAIN-SUFFIX' || true)

{
  echo "# NAME: ${NAME}"
  echo "# AUTHOR: ${AUTHOR}"
  echo "# REPO: ${REPO}"
  echo "# UPDATED: ${UPDATED}"
  echo "# TOTAL: ${TOTAL}"
  echo ""
  echo "$RULES"
} > "$OUTPUT"

echo "Done."
echo "  Shadowrocket -> $OUTPUT (${TOTAL} rules)"
