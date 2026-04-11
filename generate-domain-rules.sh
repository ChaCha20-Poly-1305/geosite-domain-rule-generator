#!/usr/bin/env bash
# Outputs: ru-foreign-TLD-domain.list and ru-unbound-domain-blocklist.conf

set -euo pipefail

INPUT="raw/domain_list.txt"

if [[ ! -f "$INPUT" ]]; then
  echo "Error: file '$INPUT' not found." >&2
  exit 1
fi

SHADOWROCKET="output/ru-domain.list"
UNBOUND="output/ru-unbound-domain-blocklist.conf"

SR_NAME="ru-domain.list"
UB_NAME="ru-unbound-domain-blocklist.conf"
AUTHOR="ChaCha20-Poly-1305"
REPO="https://github.com/ChaCha20-Poly-1305/rule-scripting-playground"
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

# --- Unbound ---
{
  echo "# NAME: ${UB_NAME}"
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
} > "$UNBOUND"

echo "Done."
echo "  Shadowrocket -> $SHADOWROCKET (${TOTAL} rules)"
echo "  Unbound      -> $UNBOUND (${TOTAL} rules)"
