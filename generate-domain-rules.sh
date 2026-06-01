#!/usr/bin/env bash
# Outputs: ru-domain.list, ru-unbound-domain-blocklist, ru-unbound-domain-whitelist.conf

set -euo pipefail

INPUT="raw/domain_list.txt"

if [[ ! -f "$INPUT" ]]; then
  echo "Error: file '$INPUT' not found." >&2
  exit 1
fi

SHADOWROCKET="output/ru-domain.list"
UNBOUND_BLOCK="output/ru-unbound-domain-blocklist.conf"
UNBOUND_ALLOW="output/ru-unbound-domain-whitelist.conf"

SR_NAME="ru-domain.list"
UB_BLOCK_NAME="ru-unbound-domain-blocklist.conf"
UB_ALLOW_NAME="ru-unbound-domain-whitelist.conf"
AUTHOR="ChaCha20-Poly-1305"
REPO="https://github.com/ChaCha20-Poly-1305/geosite-domain-rule-generator"
UPDATED=$(TZ="Asia/Shanghai" date "+%Y-%m-%d %H:%M:%S %Z")

# Count non-blank, non-comment lines
TOTAL=$(grep -v '^\s*$' "$INPUT" | grep -v '^\s*#' | wc -l | tr -d ' ')

# --- Shadowrocket ---
{
  echo "# NAME: ${SR_NAME}"
  echo "# AUTHOR: ${AUTHOR}"
  echo "# REPO: ${REPO}"
  echo "# UPDATED: ${UPDATED}"
  echo "# TOTAL: ${TOTAL}"
  echo ""
  while IFS= read -r line; do
    # Preserve blank lines
    if [[ -z "${line// /}" ]]; then
      echo ""
    # Preserve comments as-is
    elif [[ "$line" =~ ^\s*# ]]; then
      echo "$line"
    # Convert domain
    else
      domain="${line// /}"
      echo "DOMAIN-SUFFIX,${domain}"
    fi
  done < "$INPUT"
} > "$SHADOWROCKET"

# --- Unbound blocklist ---
{
  echo "# NAME: ${UB_BLOCK_NAME}"
  echo "# AUTHOR: ${AUTHOR}"
  echo "# REPO: ${REPO}"
  echo "# UPDATED: ${UPDATED}"
  echo "# TOTAL: ${TOTAL}"
  echo ""
  while IFS= read -r line; do
    if [[ -z "${line// /}" ]]; then
      echo ""
    elif [[ "$line" =~ ^\s*# ]]; then
      echo "$line"
    else
      domain="${line// /}"
      echo "local-zone: \"${domain}\" always_nxdomain"
    fi
  done < "$INPUT"
} > "$UNBOUND_BLOCK"


# --- Unbound whitelist ---
{
  echo "# NAME: ${UB_ALLOW_NAME}"
  echo "# AUTHOR: ${AUTHOR}"
  echo "# REPO: ${REPO}"
  echo "# UPDATED: ${UPDATED}"
  echo "# TOTAL: ${TOTAL}"
  echo ""
  while IFS= read -r line; do
    if [[ -z "${line// /}" ]]; then
      echo ""
    elif [[ "$line" =~ ^\s*# ]]; then
      echo "$line"
    else
      domain="${line// /}"
      echo "local-zone: \"${domain}\" transparent"
    fi
  done < "$INPUT"
} > "$UNBOUND_ALLOW"

echo "Done."
echo "  Shadowrocket  -> $SHADOWROCKET (${TOTAL} rules)"
echo "  Unbound Allow -> $UNBOUND_ALLOW (${TOTAL} rules)"
echo "  Unbound Block -> $UNBOUND_BLOCK (${TOTAL} rules)"
