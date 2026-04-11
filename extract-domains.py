#!/usr/bin/env python3
"""
v2fly GeoSite Domain Extractor
--------------------------------
Downloads dlc.dat from v2fly/domain-list-community, parses the specified
category (and all recursively included sub-categories), and writes a clean
domain list to a .txt file.

Filtered out:
  - Blank lines / comments
  - Domains whose TLD is in EXCLUDED_TLDS
  - Regex-type entries (not plain hostnames)
  - Attribute-only entries (e.g. @cn)
"""

import urllib.request
import struct
import sys
import os
import time

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DLC_URL = "https://github.com/v2fly/domain-list-community/releases/latest/download/dlc.dat"
OUTPUT_FILE = "raw/domain_list_extracted.txt"

# Root category to extract (will also pull every include: inside it)
ROOT_CATEGORY = "category-ru"

# TLDs to exclude (matched against the last label of each domain)
EXCLUDED_TLDS = {
    "moscow",
    "ru",
    "su",
    "tatar",
    "yandex",
    "xn--80adxhks",   # .москва
    "xn--80asehdb",   # .онлайн
    "xn--80aswg",     # .сайт
    "xn--c1avg",      # .орг
    "xn--d1acj3b",    # .дети
    "xn--p1acf",      # .рус
    "xn--p1ai",       # .рф
}

# Domain type constants (from v2ray-core/app/router/config.proto)
TYPE_PLAIN  = 0   # keyword match — skip, not a usable hostname
TYPE_REGEX  = 1   # regex — skip
TYPE_DOMAIN = 2   # subdomain wildcard  →  emit without leading dot
TYPE_FULL   = 3   # exact full domain

# ---------------------------------------------------------------------------
# Minimal protobuf wire-format decoder
# ---------------------------------------------------------------------------

def _read_varint(data: bytes, pos: int):
    result, shift = 0, 0
    while pos < len(data):
        b = data[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
    raise ValueError("Truncated varint")


def _iter_fields(data: bytes):
    """Yield (field_number, wire_type, value, end_pos) for every field."""
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field_number = tag >> 3
        wire_type    = tag & 0x7

        if wire_type == 0:          # varint
            val, pos = _read_varint(data, pos)
            yield field_number, 0, val, pos

        elif wire_type == 1:        # 64-bit fixed
            val = data[pos:pos+8]; pos += 8
            yield field_number, 1, val, pos

        elif wire_type == 2:        # length-delimited (string / bytes / embedded msg)
            length, pos = _read_varint(data, pos)
            val = data[pos:pos+length]; pos += length
            yield field_number, 2, val, pos

        elif wire_type == 5:        # 32-bit fixed
            val = data[pos:pos+4]; pos += 4
            yield field_number, 5, val, pos

        else:
            raise ValueError(f"Unsupported wire type {wire_type} at offset {pos}")


def _parse_domain(data: bytes):
    """
    Parse a Domain message.
    Returns (type_int, value_str) or None if the message is malformed.
    """
    dtype  = TYPE_DOMAIN   # default when field 1 is absent
    dvalue = None
    for fn, wt, val, _ in _iter_fields(data):
        if fn == 1 and wt == 0:
            dtype = val
        elif fn == 2 and wt == 2:
            dvalue = val.decode("utf-8", errors="replace")
    if dvalue is None:
        return None
    return dtype, dvalue


def _parse_geosite(data: bytes):
    """
    Parse a GeoSite message.
    Returns (country_code_lower, list_of_(type, value)).
    """
    code    = None
    domains = []
    for fn, wt, val, _ in _iter_fields(data):
        if fn == 1 and wt == 2:
            code = val.decode("utf-8", errors="replace").lower()
        elif fn == 2 and wt == 2:
            result = _parse_domain(val)
            if result:
                domains.append(result)
    return code, domains


def parse_dlc(data: bytes):
    """
    Parse a GeoSiteList protobuf blob.
    Returns dict: category_name_lower → list of (type_int, value_str).
    """
    catalog = {}
    for fn, wt, val, _ in _iter_fields(data):
        if fn == 1 and wt == 2:             # repeated GeoSite entry = 1
            code, domains = _parse_geosite(val)
            if code:
                catalog[code] = domains
    return catalog

# ---------------------------------------------------------------------------
# Download helper
# ---------------------------------------------------------------------------

def download_dlc(url: str, dest: str = "dlc.dat") -> str:
    """Download dlc.dat (with progress) and return the local path."""
    if os.path.exists(dest):
        age = time.time() - os.path.getmtime(dest)
        if age < 86400:
            print(f"[i] Using cached {dest} (age {int(age/60)} min)")
            return dest
        print(f"[i] Cache expired, re-downloading…")

    print(f"[↓] Downloading {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "geosite-extractor/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        chunks = []
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            chunks.append(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded * 100 // total
                print(f"\r    {downloaded:,} / {total:,} bytes  ({pct}%)   ", end="", flush=True)
    print()
    raw = b"".join(chunks)
    with open(dest, "wb") as f:
        f.write(raw)
    print(f"[✓] Saved to {dest}  ({len(raw):,} bytes)")
    return dest

# ---------------------------------------------------------------------------
# Category resolution (handles include: directives)
# ---------------------------------------------------------------------------

def resolve_categories(catalog: dict, root: str) -> set:
    """
    Return all category names reachable from *root* via include: entries.
    include: entries have TYPE_PLAIN (0) and the value is 'include:catname'.
    """
    visited = set()
    stack   = [root.lower()]

    while stack:
        name = stack.pop()
        if name in visited:
            continue
        visited.add(name)

        if name not in catalog:
            print(f"[!] Category '{name}' not found in dlc.dat — skipping")
            continue

        for dtype, dvalue in catalog[name]:
            if dtype == TYPE_PLAIN and dvalue.startswith("include:"):
                included = dvalue[len("include:"):].strip().lower()
                if included not in visited:
                    stack.append(included)

    return visited

# ---------------------------------------------------------------------------
# Domain extraction & filtering
# ---------------------------------------------------------------------------

def tld_of(domain: str) -> str:
    """Return the last label (TLD) of a domain, lower-cased."""
    return domain.rsplit(".", 1)[-1].lower() if "." in domain else domain.lower()


def extract_domains(catalog: dict, categories: set) -> list:
    """
    Collect all usable domain strings from the resolved category set,
    excluding EXCLUDED_TLDS, regex, and plain-keyword entries.
    Returns a sorted, deduplicated list.
    """
    seen = set()

    for cat in sorted(categories):
        entries = catalog.get(cat, [])
        for dtype, dvalue in entries:
            # Skip include directives, regex, and bare keywords
            if dtype == TYPE_PLAIN:
                continue
            if dtype == TYPE_REGEX:
                continue

            domain = dvalue.lower().strip()

            # Strip any residual leading dot
            if domain.startswith("."):
                domain = domain[1:]

            if not domain:
                continue

            # TLD filter
            if tld_of(domain) in EXCLUDED_TLDS:
                continue

            seen.add(domain)

    return sorted(seen)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    # 1. Download
    dat_path = download_dlc(DLC_URL)

    # 2. Parse protobuf
    print("[…] Parsing dlc.dat…")
    with open(dat_path, "rb") as f:
        raw = f.read()
    catalog = parse_dlc(raw)
    print(f"[✓] Found {len(catalog)} categories in dlc.dat")

    # 3. Resolve includes
    print(f"[…] Resolving includes for '{ROOT_CATEGORY}'…")
    categories = resolve_categories(catalog, ROOT_CATEGORY)
    print(f"[✓] Resolved {len(categories)} categories: {', '.join(sorted(categories))}")

    # 4. Extract & filter
    domains = extract_domains(catalog, categories)
    print(f"[✓] Extracted {len(domains)} unique domains after TLD filtering")

    # 5. Write output
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(domains))
        f.write("\n")
    print(f"[✓] Written to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
