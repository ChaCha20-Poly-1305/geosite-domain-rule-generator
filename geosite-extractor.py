#!/usr/bin/env python3
"""
Usage:
  python geosite-extractor.py                          # interactive, with tab-completion
  python geosite-extractor.py CATEGORY-RU              # run directly
  python geosite-extractor.py --category cn            # same with explicit flag
  python geosite-extractor.py --dat /path/to/dlc.dat CATEGORY-RU
  python geosite-extractor.py --output domains.txt CATEGORY-RU
  python geosite-extractor.py --no-includes CATEGORY-RU   # skip include resolution
  python geosite-extractor.py --refresh-includes CATEGORY-RU  # re-download include map
  python geosite-extractor.py --list                   # print all available categories

The script:
  1. Downloads dlc.dat from the official v2fly release (or reuses an existing file).
  2. Downloads the source repository ZIP (once, ~3 MB) to build a complete include map
     and caches it as dlc-includes.json next to the dat file.
  3. Resolves include: directives recursively - every nested sub-category is pulled in.
  4. Writes a clean, alphabetically sorted, deduplicated .txt - one domain per line,
     no blank lines, no comments, no full: / @cn artefacts, no regex patterns.

No third-party dependencies are required (stdlib only).
"""

import argparse
import io
import json
import os
import re
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DAT_DEFAULT = "dlc.dat"
INCLUDES_SUFFIX = "-includes.json"   # saved next to the dat file

DAT_URL = (
    "https://github.com/v2fly/domain-list-community"
    "/releases/latest/download/dlc.dat"
)
SOURCE_ZIP_URL = (
    "https://github.com/v2fly/domain-list-community"
    "/archive/refs/heads/master.zip"
)


# ---------------------------------------------------------------------------
# Minimal protobuf reader - no external deps
# ---------------------------------------------------------------------------

def _read_varint(buf: bytes, pos: int):
    result = shift = 0
    while True:
        b = buf[pos]; pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7


def _read_ld(buf: bytes, pos: int):
    length, pos = _read_varint(buf, pos)
    return buf[pos: pos + length], pos + length


def _parse_domain_entry(buf: bytes):
    """Return (type_int, value_str) for a Domain protobuf message."""
    pos = 0
    dtype = 2      # default: Domain / subdomain match
    value = None
    while pos < len(buf):
        tag, pos = _read_varint(buf, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            v, pos = _read_varint(buf, pos)
            if field == 1:
                dtype = v
        elif wire == 2:
            raw, pos = _read_ld(buf, pos)
            if field == 2:
                value = raw.decode("utf-8", errors="replace")
        elif wire == 5:
            pos += 4
        elif wire == 1:
            pos += 8
    return dtype, value


def parse_dat(path: str) -> dict:
    """
    Parse a GeoSite .dat file.

    Returns { "CATEGORY-NAME": ["domain1", "domain2", ...], ... }

    Domain types kept:
      2 = Domain  (subdomain match, e.g. "google.com" also matches "www.google.com")
      3 = Full    (exact hostname match)

    Domain type 1 (Regex) is always skipped - regex patterns are not real hostnames.
    Entries with no dot (TLD stubs, bare keywords) are also excluded.
    """
    with open(path, "rb") as fh:
        data = fh.read()

    entries = {}
    pos = 0
    while pos < len(data):
        tag, pos = _read_varint(data, pos)
        field, wire = tag >> 3, tag & 7
        if wire == 2:
            raw, pos = _read_ld(data, pos)
            if field == 1:
                ipos = 0
                code = None
                domains = []
                while ipos < len(raw):
                    itag, ipos = _read_varint(raw, ipos)
                    ifield, iwire = itag >> 3, itag & 7
                    if iwire == 2:
                        iraw, ipos = _read_ld(raw, ipos)
                        if ifield == 1:
                            code = iraw.decode("utf-8")
                        elif ifield == 2:
                            dtype, value = _parse_domain_entry(iraw)
                            if (
                                value
                                and dtype in (2, 3)
                                and "." in value
                                and not value.startswith(".")
                            ):
                                domains.append(value.lower().strip())
                    elif iwire == 0:
                        _, ipos = _read_varint(raw, ipos)
                    elif iwire == 5:
                        ipos += 4
                    elif iwire == 1:
                        ipos += 8
                if code:
                    entries[code.upper()] = domains
        elif wire == 0:
            _, pos = _read_varint(data, pos)
        elif wire == 5:
            pos += 4
        elif wire == 1:
            pos += 8
    return entries


# ---------------------------------------------------------------------------
# Include map - built once from the source ZIP and cached as JSON
# ---------------------------------------------------------------------------

def _includes_path(dat_path: str) -> str:
    """Return the path where the include map JSON is cached."""
    stem = os.path.splitext(dat_path)[0]
    return stem + INCLUDES_SUFFIX


def _build_include_map_from_zip(zip_bytes: bytes) -> dict:
    """
    Parse every file under data/ in the repository ZIP and return:
        { "CATEGORY-NAME": ["INCLUDED-CAT-1", "INCLUDED-CAT-2", ...], ... }

    Source file format (one entry per line):
        # comment
        domain.tld
        full:domain.tld
        include:other-category
        regexp:...            (ignored)
        domain.tld @attr      (attribute stripped, domain kept)
    """
    include_map = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            # Files live at <repo-root>/data/<category-name>
            parts = name.replace("\\", "/").split("/")
            if len(parts) < 3 or parts[-2] != "data" or parts[-1] == "":
                continue
            cat_name = parts[-1].upper()
            includes = []
            try:
                text = zf.read(name).decode("utf-8", errors="replace")
            except Exception:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                m = re.match(r"^include:(\S+)", line, re.IGNORECASE)
                if m:
                    includes.append(m.group(1).upper())
            include_map[cat_name] = includes
    return include_map


def load_include_map(dat_path: str, force_refresh: bool = False):
    """
    Return the include map, downloading and building it if necessary.

    The map is cached as a JSON file next to the dat.  Pass force_refresh=True
    to re-download even if the cache already exists.

    Returns None if the source ZIP cannot be fetched (network unavailable).
    The caller should warn and continue without include resolution in that case.
    """
    cache_path = _includes_path(dat_path)

    if not force_refresh and os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            print(
                f"[info] Loaded include map from cache ({cache_path},"
                f" {len(data)} categories)",
                file=sys.stderr,
            )
            return data
        except Exception as exc:
            print(
                f"[warn] Could not read cached include map ({exc}); re-downloading",
                file=sys.stderr,
            )

    print(
        f"[info] Downloading source ZIP (~3 MB) to build include map ...\n"
        f"       {SOURCE_ZIP_URL}",
        file=sys.stderr,
    )
    try:
        req = urllib.request.Request(
            SOURCE_ZIP_URL,
            headers={"User-Agent": "geosite-extract/2.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            zip_bytes = resp.read()
    except urllib.error.URLError as exc:
        print(
            f"[warn] Could not download source ZIP: {exc}\n"
            f"       Include resolution will be skipped for this run.\n"
            f"       Use --no-includes to suppress this warning, or check your\n"
            f"       network connection and try again.",
            file=sys.stderr,
        )
        return None

    print(
        f"[info] Building include map from ZIP ({len(zip_bytes):,} bytes) ...",
        file=sys.stderr,
    )
    include_map = _build_include_map_from_zip(zip_bytes)
    print(
        f"[info] Include map built ({len(include_map)} categories)",
        file=sys.stderr,
    )

    try:
        with open(cache_path, "w", encoding="utf-8") as fh:
            json.dump(include_map, fh, separators=(",", ":"))
        print(f"[info] Saved include map -> {cache_path}", file=sys.stderr)
    except Exception as exc:
        print(f"[warn] Could not cache include map: {exc}", file=sys.stderr)

    return include_map


# ---------------------------------------------------------------------------
# Recursive include resolution
# ---------------------------------------------------------------------------

def resolve_categories(root: str, all_categories: set, include_map) -> set:
    """
    Return the set of all category names that should be merged for *root*:
    the root itself plus every include: target, resolved recursively.

    Only categories that actually exist in *all_categories* (i.e. in the binary)
    are returned.  Missing includes are reported as warnings.

    If *include_map* is None (source ZIP unavailable) only the root is returned.
    """
    root = root.upper()
    visited = set()
    queue = [root]

    while queue:
        cat = queue.pop().upper()
        if cat in visited:
            continue
        visited.add(cat)

        if cat not in all_categories:
            if cat != root:
                print(
                    f"  [warn] '{cat}' is listed as an include but is not present"
                    f" in the dat file - skipping",
                    file=sys.stderr,
                )
            continue

        if include_map is None:
            continue

        sub_includes = include_map.get(cat, [])
        new = [s for s in sub_includes if s.upper() not in visited]
        if new:
            print(
                f"  [info] {cat} includes: {', '.join(new)}",
                file=sys.stderr,
            )
        queue.extend(new)

    return visited & all_categories


# ---------------------------------------------------------------------------
# Downloading dlc.dat
# ---------------------------------------------------------------------------

def ensure_dat(path: str) -> str:
    """Download dlc.dat if *path* doesn't exist yet. Returns the resolved path."""
    if os.path.exists(path):
        size = os.path.getsize(path)
        print(f"[info] Using existing {path} ({size:,} bytes)", file=sys.stderr)
        return path

    print(f"[info] Downloading dlc.dat ...\n       {DAT_URL}", file=sys.stderr)
    try:
        req = urllib.request.Request(
            DAT_URL,
            headers={"User-Agent": "geosite-extract/2.0"},
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = resp.read()
        with open(path, "wb") as fh:
            fh.write(data)
        print(f"[info] Saved {len(data):,} bytes -> {path}", file=sys.stderr)
    except urllib.error.URLError as exc:
        sys.exit(f"[error] Could not download dlc.dat: {exc}")
    return path


# ---------------------------------------------------------------------------
# Interactive category selection with readline tab-completion
# ---------------------------------------------------------------------------

def pick_category_interactive(categories: list) -> str:
    """Prompt the user to type a category name with readline tab-completion."""
    try:
        import readline

        def _completer(text: str, state: int):
            text_up = text.upper()
            matches = [c for c in categories if c.upper().startswith(text_up)]
            return matches[state] if state < len(matches) else None

        readline.set_completer(_completer)
        # macOS ships libedit instead of GNU readline
        if sys.platform == "darwin":
            readline.parse_and_bind("bind ^I rl_complete")
        else:
            readline.parse_and_bind("tab: complete")
        readline.set_completer_delims("")   # whole input is one token
    except ImportError:
        pass   # no completion available, still functional

    print(
        f"  {len(categories)} categories available.  "
        f"Press TAB to autocomplete.\n",
        file=sys.stderr,
    )

    while True:
        try:
            raw = input("Category: ").strip().upper()
        except (EOFError, KeyboardInterrupt):
            print(file=sys.stderr)
            sys.exit(0)
        if not raw:
            continue
        if raw in categories:
            return raw
        close = [c for c in categories if raw in c][:8]
        if close:
            print(
                f"  Not found. Did you mean: {', '.join(close)}?",
                file=sys.stderr,
            )
        else:
            print(
                f"  '{raw}' not found ({len(categories)} categories available).",
                file=sys.stderr,
            )


# ---------------------------------------------------------------------------
# Domain validation
# ---------------------------------------------------------------------------

_IP_RE = re.compile(r"^\d{1,3}(\.\d{1,3}){3}$")
_BAD_CHARS_RE = re.compile(r"[\s/@]")


def _is_valid_domain(d: str) -> bool:
    if not d or "." not in d:
        return False
    if _BAD_CHARS_RE.search(d):
        return False
    if _IP_RE.fullmatch(d):
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Extract domains from a v2fly GeoSite .dat file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "category",
        nargs="?",
        help="Category name to extract (e.g. CATEGORY-RU). "
             "Omit for interactive prompt.",
    )
    parser.add_argument(
        "--category", "-c",
        dest="category_flag",
        metavar="NAME",
        help="Category name (alternative to positional argument).",
    )
    parser.add_argument(
        "--dat", "-d",
        default=DAT_DEFAULT,
        metavar="FILE",
        help=f"Path to dlc.dat (default: {DAT_DEFAULT!r}; downloaded if absent).",
    )
    parser.add_argument(
        "--output", "-o",
        metavar="FILE",
        help="Output file path (default: <CATEGORY>.txt in the current directory).",
    )
    parser.add_argument(
        "--no-includes",
        action="store_true",
        help="Skip recursive include resolution; use only the category's own domains.",
    )
    parser.add_argument(
        "--refresh-includes",
        action="store_true",
        help="Re-download the source ZIP even if a cached include map already exists.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print all available category names and exit.",
    )
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 1. Ensure dat file exists
    # ------------------------------------------------------------------
    dat_path = ensure_dat(args.dat)

    # ------------------------------------------------------------------
    # 2. Parse binary
    # ------------------------------------------------------------------
    print("[info] Parsing dat ...", file=sys.stderr)
    db = parse_dat(dat_path)
    categories = sorted(db.keys())

    if args.list:
        print("\n".join(categories))
        return

    # ------------------------------------------------------------------
    # 3. Determine requested category
    # ------------------------------------------------------------------
    requested = (args.category_flag or args.category or "").strip().upper()
    if not requested:
        requested = pick_category_interactive(categories)

    if requested not in db:
        close = [c for c in categories if requested in c][:8]
        hint = (f"\n  Close matches: {', '.join(close)}" if close else "")
        sys.exit(f"[error] Category '{requested}' not found in dat.{hint}")

    # ------------------------------------------------------------------
    # 4. Load include map (unless --no-includes)
    # ------------------------------------------------------------------
    if args.no_includes:
        include_map = None
    else:
        print("[info] Loading include map ...", file=sys.stderr)
        include_map = load_include_map(dat_path, force_refresh=args.refresh_includes)

    # ------------------------------------------------------------------
    # 5. Resolve includes recursively
    # ------------------------------------------------------------------
    resolved = resolve_categories(requested, set(categories), include_map)
    if len(resolved) > 1:
        print(
            f"[info] Merging {len(resolved)} categories: "
            f"{', '.join(sorted(resolved))}",
            file=sys.stderr,
        )
    else:
        print(f"[info] Extracting category: {requested}", file=sys.stderr)

    # ------------------------------------------------------------------
    # 6. Collect, clean, sort, deduplicate
    # ------------------------------------------------------------------
    raw_domains = set()
    for cat in resolved:
        raw_domains.update(db[cat])

    clean = sorted(
        {
            d.lower().strip().strip(".")
            for d in raw_domains
            if _is_valid_domain(d.lower().strip().strip("."))
        }
    )

    # ------------------------------------------------------------------
    # 7. Write output
    # ------------------------------------------------------------------
    out_path = args.output or f"{requested}.txt"
    Path(out_path).write_text("\n".join(clean) + "\n", encoding="utf-8")
    print(
        f"[info] Wrote {len(clean):,} domains -> {out_path}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()
