"""
import_kb_product_index.py
==========================
Imports the Striven Item Profitability CSV export into the `kb_product_index`
Supabase table, aggregated by vendor/brand.

Brand assignment strategy
--------------------------
CSV "Item" field is usually an opaque part code (e.g. "G10-24/30-01V"), not a
brand name.  The script resolves brand through a layered priority system:

  1. No item_number + service keyword in CSV name         → WSF Services
  2. Vendor map: exact item_number match
  3. Vendor map: prefix item_number match
  4. Vendor map: contains text match (Supabase name/desc/CSV item)
  5. Built-in brand map on Supabase item name             (primary)
  6. Built-in brand map on Supabase item description      (secondary)
  7. Built-in brand map on CSV Item text                  (last resort)
  8. No item_number and no match                          → WSF Services
  9. Has item_number but no match                         → Unknown

Run order  (MUST follow this sequence)
-----------------------------------------
1. Supabase SQL  — run once if not already applied:
       ALTER TABLE items ADD COLUMN IF NOT EXISTS item_number text;
       CREATE INDEX IF NOT EXISTS idx_items_item_number ON items(item_number);

2. sync_items.py — populates items.item_number from Striven:
       python sync_items.py

3. Dry-run this script — reads Supabase items (read-only), prints summary, no writes:
       python import_kb_product_index.py --csv "<path>" [--vendor-map kb_vendor_map.csv] --dry-run

4. Review dry-run output and generated audit files:
       kb_import_unknowns.csv        — Unknown items sorted by revenue; fill blank_vendor_to_fill
       kb_import_vendor_summary.csv  — All vendors with revenue/margin totals

5. Add rules to kb_vendor_map.csv from the audit file, re-run dry-run to verify.

6. Live run — only after approving the dry-run summary:
       python import_kb_product_index.py --csv "<path>" [--vendor-map kb_vendor_map.csv]

Source
------
Export from Striven → Reports → Item Profitability.
CSV columns (required):
    Item, Item Number, Qty, Amount, Avg. Price, % of Sales,
    Cost, Avg. Cost, Profit, Profit %

Supabase target: kb_product_index
----------------------------------
Preserved fields (not overwritten):  coverage_status, kb_doc_count, has_install_manual
Written fields:                       vendor, total_unique_skus, total_revenue,
                                      total_cost, total_profit, avg_margin_pct,
                                      updated_at

Vendor map format  (kb_vendor_map.csv)
---------------------------------------
Columns: match_type, pattern, vendor, notes

  match_type  | pattern matches against      | example
  ------------|------------------------------|---------------------------
  exact       | CSV Item Number (exact)      | G10-24/30-01V → Real Fyre
  prefix      | CSV Item Number (startswith) | G10- → Real Fyre
  contains    | Supabase name/desc/CSV item  | Heatilator → Heatilator

Rules are applied in priority order: exact → prefix → contains.
Case-insensitive. If no rule matches, falls through to built-in brand map.

Usage
-----
    python import_kb_product_index.py --csv "C:\\path\\to\\ItemProfitability.csv"
    python import_kb_product_index.py --csv "C:\\path\\to\\ItemProfitability.csv" --dry-run
    python import_kb_product_index.py --csv "C:\\path\\to\\ItemProfitability.csv" \\
        --vendor-map kb_vendor_map.csv --dry-run

Required env vars
-----------------
    SUPABASE_URL
    SUPABASE_KEY          (anon key — fallback)
    SUPABASE_SERVICE_KEY  (service role — preferred; bypasses RLS for writes)
"""

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Built-in conservative brand map
# Applied to item name text (lowercased).  Order matters: check more-specific
# strings before shorter ones that might match inside them.
# ---------------------------------------------------------------------------
BRAND_MAP: list[tuple[str, str]] = [
    # Fireplace / hearth units
    ("heatilator",           "Heatilator"),
    ("heat & glo",           "Heat & Glo"),
    ("heat n glo",           "Heat & Glo"),
    ("heat and glo",         "Heat & Glo"),
    ("napoleon",             "Napoleon"),
    ("majestic",             "Majestic"),
    ("monessen",             "Monessen"),
    ("ihp",                  "IHP"),
    ("superior",             "Superior"),
    ("empire",               "Empire"),
    ("comfort flame",        "Comfort Flame"),
    ("continental",          "Continental"),
    ("security",             "Security Fireplaces"),
    ("vanguard",             "Vanguard"),
    ("valor",                "Valor"),
    ("regency",              "Regency"),
    ("quadra-fire",          "Quadra-Fire"),
    ("quadrafire",           "Quadra-Fire"),
    ("fireplace xtrordinair","Fireplace Xtrordinair"),
    ("fp xtrordinair",       "Fireplace Xtrordinair"),
    ("lennox",               "Lennox"),
    ("temco",                "Temco"),
    ("desa",                 "Desa"),
    ("hearth craft",         "Hearth Craft"),
    ("hearthcraft",          "Hearth Craft"),
    ("astria",               "Astria"),
    ("biltmore",             "Biltmore"),
    ("kingsman",             "Kingsman"),
    ("mendota",              "Mendota"),
    ("enviro",               "Enviro"),
    ("lopi",                 "Lopi"),
    ("kozy heat",            "Kozy Heat"),
    ("kozyheat",             "Kozy Heat"),
    ("osburn",               "Osburn"),
    ("pacific energy",       "Pacific Energy"),
    ("harman",               "Harman"),
    ("whitfield",            "Whitfield"),
    ("american hearth",      "American Hearth"),
    ("american gas",         "American Gas"),
    # Gas logs / burners
    ("rasmussen",            "Rasmussen"),
    ("real fyre",            "Real Fyre"),
    ("realfyre",             "Real Fyre"),
    ("r.h. peterson",        "Real Fyre"),
    ("rh peterson",          "Real Fyre"),
    ("peterson",             "Real Fyre"),
    ("golden blount",        "Golden Blount"),
    ("blount",               "Golden Blount"),
    ("sierra flame",         "Sierra Flame"),
    ("ceramic wool",         "Ceramic Wool"),
    ("rocky mountain",       "Rocky Mountain"),
    ("fire up",              "Fire Up"),
    # Mantels / surrounds
    ("stoll",                "Stoll"),
    ("veneered",             "Veneered"),
    ("pearl mantels",        "Pearl Mantels"),
    ("pearl",                "Pearl Mantels"),
    ("verona",               "Verona"),
    ("dimplex",              "Dimplex"),
    ("classicflame",         "ClassicFlame"),
    ("classic flame",        "ClassicFlame"),
    ("touchstone",           "Touchstone"),
    # Venting / chimney
    ("selkirk",              "Selkirk"),
    ("duravent",             "DuraVent"),
    ("hart & cooley",        "Hart & Cooley"),
    ("hart and cooley",      "Hart & Cooley"),
    ("metalbestos",          "Metalbestos"),
    ("security b vent",      "Security Fireplaces"),
    ("dura tech",            "DuraVent"),
    ("duratech",             "DuraVent"),
    ("pro-flex",             "Pro-Flex"),
    ("proflex",              "Pro-Flex"),
    ("flex-l",               "Flex-L"),
    ("rockwool",             "Rockwool"),
    # Accessories / screens / tools
    ("minute grate",         "Minute Grate"),
    ("pilgrim",              "Pilgrim"),
    ("napa forge",           "Napa Forge"),
    ("uniflame",             "Uniflame"),
    ("plow & hearth",        "Plow & Hearth"),
    ("woodfield",            "Woodfield"),
    ("spitfire",             "Spitfire"),
    ("btu",                  "BTU"),          # keep last — short substring
]

# Service-item keyword signals (no SKU + keyword in name → WSF Services)
SERVICE_KEYWORDS: tuple[str, ...] = (
    "install",
    "service",
    "repair",
    "diagnostic",
    "inspection",
    "cleaning",
    "preview",
    "permit",
    "trip charge",
    "return trip",
    "travel",
    "labor",
    "labour",
    "quote adjustment",
    "estimate",
    "consultation",
    "warranty call",
    "commission",
    "discount",
    "freight",
    "delivery",
    "haul",
    "disposal",
    "misc",
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_numeric(value) -> float:
    """Strip %, $, commas, whitespace; return float (0 on error or None)."""
    if value is None:
        return 0.0
    v = str(value).strip().lstrip("$").rstrip("%").replace(",", "").strip()
    if not v:
        return 0.0
    try:
        return float(v)
    except ValueError:
        return 0.0


def _brand_from_text(text: str) -> str | None:
    """Apply built-in BRAND_MAP to a string. Returns first matched brand or None."""
    lower = text.lower()
    for substring, brand in BRAND_MAP:
        if substring in lower:
            return brand
    return None


def _load_vendor_map(path: str) -> tuple[dict, int]:
    """
    Load vendor map rules from CSV file.
    CSV columns: match_type, pattern, vendor, notes

    Returns:
        rules — dict with keys "exact", "prefix", "contains"
        count — number of valid rules loaded
    """
    rules: dict = {"exact": {}, "prefix": [], "contains": []}
    count = 0

    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            mt      = (row.get("match_type") or "").strip().lower()
            pattern = (row.get("pattern") or "").strip()
            vendor  = (row.get("vendor") or "").strip()

            if not mt or not pattern or not vendor:
                continue  # skip blank or incomplete rows

            if mt == "exact":
                rules["exact"][pattern.lower()] = vendor
            elif mt == "prefix":
                rules["prefix"].append((pattern.lower(), vendor))
            elif mt == "contains":
                rules["contains"].append((pattern.lower(), vendor))
            else:
                print(
                    f"[vendor-map] Unknown match_type '{mt}' for '{pattern}' — skipped.",
                    file=sys.stderr,
                )
                continue
            count += 1

    # Longest prefix wins (sort descending by pattern length)
    rules["prefix"].sort(key=lambda x: -len(x[0]))
    return rules, count


def _apply_vendor_rules(
    item_number: str,
    sb_name: str,
    sb_desc: str,
    csv_item_name: str,
    rules: dict,
) -> str | None:
    """
    Apply vendor map rules in priority order: exact → prefix → contains.
    Returns matched vendor string or None if no rule fires.
    """
    item_lower = item_number.lower() if item_number else ""

    # 1. Exact match on item_number
    if item_lower and item_lower in rules["exact"]:
        return rules["exact"][item_lower]

    # 2. Prefix match on item_number (longest-prefix first)
    if item_lower:
        for pattern, vendor in rules["prefix"]:
            if item_lower.startswith(pattern):
                return vendor

    # 3. Contains match: checked against sb_name, sb_desc, csv_item_name
    search_texts = [
        sb_name.lower()       if sb_name       else "",
        sb_desc.lower()       if sb_desc       else "",
        csv_item_name.lower() if csv_item_name else "",
    ]
    for pattern, vendor in rules["contains"]:
        for text in search_texts:
            if text and pattern in text:
                return vendor

    return None


def _assign_vendor(
    csv_item_name: str,
    item_number: str,
    sb_name: str = "",
    sb_desc: str = "",
    vendor_rules: dict | None = None,
) -> str:
    """
    Return a canonical vendor name for this row.

    Priority:
      1. No item_number + service keyword in csv_item_name  → WSF Services
      2. Vendor map: exact item_number match
      3. Vendor map: prefix item_number match
      4. Vendor map: contains text match
      5. Built-in brand map on sb_name
      6. Built-in brand map on sb_desc
      7. Built-in brand map on csv_item_name
      8. No item_number and no match                        → WSF Services
      9. Has item_number but no match                       → Unknown

    Note: preferredVendor is intentionally excluded — it is a distributor, not a brand.
    """
    # 1. Service item detection (no SKU + service keyword)
    if not item_number:
        csv_lower = csv_item_name.lower()
        for kw in SERVICE_KEYWORDS:
            if kw in csv_lower:
                return "WSF Services"

    # 2–4. Vendor map rules (exact → prefix → contains)
    if vendor_rules:
        matched = _apply_vendor_rules(item_number, sb_name, sb_desc, csv_item_name, vendor_rules)
        if matched:
            return matched

    # 5. Built-in brand map on Supabase item name (primary)
    if sb_name:
        brand = _brand_from_text(sb_name)
        if brand:
            return brand

    # 6. Built-in brand map on Supabase description (secondary)
    if sb_desc:
        brand = _brand_from_text(sb_desc)
        if brand:
            return brand

    # 7. Built-in brand map on CSV item text (last resort — usually a part code)
    brand = _brand_from_text(csv_item_name)
    if brand:
        return brand

    # 8. No SKU → service / misc catch-all
    if not item_number:
        return "WSF Services"

    # 9. Has SKU but brand could not be identified
    return "Unknown"


def _read_csv(path: str) -> list[dict]:
    """
    Read Striven Item Profitability CSV.
    Returns list of dicts with cleaned numeric fields.
    """
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            item_name   = (raw.get("Item") or "").strip()
            item_number = (raw.get("Item Number") or "").strip()

            if not item_name:
                continue

            rows.append({
                "item_name":    item_name,
                "item_number":  item_number,
                "qty":          _clean_numeric(raw.get("Qty")),
                "amount":       _clean_numeric(raw.get("Amount")),
                "avg_price":    _clean_numeric(raw.get("Avg. Price")),
                "pct_of_sales": _clean_numeric(raw.get("% of Sales")),
                "cost":         _clean_numeric(raw.get("Cost")),
                "avg_cost":     _clean_numeric(raw.get("Avg. Cost")),
                "profit":       _clean_numeric(raw.get("Profit")),
                "profit_pct":   _clean_numeric(raw.get("Profit %")),
            })
    return rows


def _fetch_item_lookup(sb) -> dict[str, dict]:
    """
    Fetch all Supabase items that have item_number set.
    Paginates in batches of 1,000.
    Returns dict: item_number → {name, description, category}
    """
    lookup: dict[str, dict] = {}
    page_size = 1000
    offset    = 0

    while True:
        resp = (
            sb.table("items")
            .select("item_number, name, description, category")
            .not_.is_("item_number", "null")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        for r in batch:
            if r.get("item_number"):
                lookup[r["item_number"]] = r
        if len(batch) < page_size:
            break
        offset += page_size

    return lookup


def _fetch_existing_kb(sb) -> dict[str, dict]:
    """
    Pull existing kb_product_index rows to preserve KB-curated fields.
    Returns dict keyed by vendor name.
    """
    resp = sb.table("kb_product_index").select(
        "vendor, coverage_status, kb_doc_count, has_install_manual"
    ).execute()
    return {r["vendor"]: r for r in (resp.data or [])}


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------

def _aggregate(
    rows: list[dict],
    item_lookup: dict,
    vendor_rules: dict | None = None,
) -> tuple[dict, dict]:
    """
    Aggregate CSV rows by vendor using Supabase item names + vendor map for brand matching.

    Returns:
        agg   — dict: vendor → {skus, total_revenue, total_cost, total_profit, row_count}
        stats — dict: match counts and per-row Unknown details for audit output
    """
    agg: dict[str, dict] = defaultdict(lambda: {
        "skus":          set(),
        "total_revenue": 0.0,
        "total_cost":    0.0,
        "total_profit":  0.0,
        "row_count":     0,
    })

    stats: dict = {
        "rows_with_item_number": 0,
        "rows_no_item_number":   0,
        "matched_to_supabase":   0,
        "unmatched_to_supabase": 0,
        "vendor_from_map":       0,
        "vendor_from_sb":        0,
        "unknown_rows":          [],   # full row data for audit CSV
    }

    for row in rows:
        item_number = row["item_number"]
        sb_item     = item_lookup.get(item_number) if item_number else None
        sb_name     = (sb_item.get("name") or "")        if sb_item else ""
        sb_desc     = (sb_item.get("description") or "") if sb_item else ""

        # Match statistics
        if item_number:
            stats["rows_with_item_number"] += 1
            if sb_item:
                stats["matched_to_supabase"] += 1
            else:
                stats["unmatched_to_supabase"] += 1
        else:
            stats["rows_no_item_number"] += 1

        vendor = _assign_vendor(row["item_name"], item_number, sb_name, sb_desc, vendor_rules)

        # Source attribution tracking
        if vendor not in ("WSF Services", "Unknown"):
            if vendor_rules:
                # Check whether vendor map fired (re-run just for tracking)
                vm_result = _apply_vendor_rules(item_number, sb_name, sb_desc, row["item_name"], vendor_rules)
                if vm_result == vendor:
                    stats["vendor_from_map"] += 1
                elif sb_name or sb_desc:
                    stats["vendor_from_sb"] += 1
            elif sb_name or sb_desc:
                stats["vendor_from_sb"] += 1

        if vendor == "Unknown":
            reason = (
                "Item number not in Supabase items table — re-run sync_items.py"
                if item_number and not sb_item
                else "Supabase item name is opaque part code — add rule to vendor map"
            )
            stats["unknown_rows"].append({
                "item_number":         item_number,
                "csv_item":            row["item_name"],
                "supabase_name":       sb_name,
                "supabase_description":sb_desc,
                "qty":                 row["qty"],
                "amount":              row["amount"],
                "profit":              row["profit"],
                "suggested_reason":    reason,
                "blank_vendor_to_fill":"",
            })

        bucket = agg[vendor]
        if item_number:
            bucket["skus"].add(item_number)
        bucket["total_revenue"] += row["amount"]
        bucket["total_cost"]    += row["cost"]
        bucket["total_profit"]  += row["profit"]
        bucket["row_count"]     += 1

    return dict(agg), stats


# ---------------------------------------------------------------------------
# CSV output helpers
# ---------------------------------------------------------------------------

def _write_unknowns_csv(unknown_rows: list[dict], out_path: str) -> None:
    """Write Unknown-vendor rows to CSV, sorted by amount descending."""
    sorted_rows = sorted(unknown_rows, key=lambda r: -abs(r["amount"]))
    fieldnames = [
        "item_number", "csv_item", "supabase_name", "supabase_description",
        "qty", "amount", "profit", "suggested_reason", "blank_vendor_to_fill",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in sorted_rows:
            writer.writerow({k: r.get(k, "") for k in fieldnames})


def _write_vendor_summary_csv(upsert_records: list[dict], row_counts: dict[str, int], out_path: str) -> None:
    """Write vendor summary to CSV, sorted by total_revenue descending."""
    fieldnames = [
        "vendor", "total_unique_skus", "total_revenue",
        "total_profit", "avg_margin_pct", "row_count",
    ]
    with open(out_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for rec in upsert_records:
            writer.writerow({
                "vendor":            rec["vendor"],
                "total_unique_skus": rec["total_unique_skus"],
                "total_revenue":     rec["total_revenue"],
                "total_profit":      rec["total_profit"],
                "avg_margin_pct":    rec["avg_margin_pct"],
                "row_count":         row_counts.get(rec["vendor"], 0),
            })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import Item Profitability CSV → kb_product_index"
    )
    parser.add_argument("--csv",        required=True,  help="Path to ItemProfitability.csv")
    parser.add_argument("--vendor-map", default=None,   help="Optional path to kb_vendor_map.csv")
    parser.add_argument("--dry-run",    action="store_true",
                        help="Read-only: print summary and write audit CSVs, no Supabase writes")
    args = parser.parse_args()

    # ── Validate CSV path ──────────────────────────────────────────────────
    csv_path = args.csv
    if not os.path.isfile(csv_path):
        print(f"[error] CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    # ── Load vendor map (optional) ─────────────────────────────────────────
    vendor_rules: dict | None = None
    if args.vendor_map:
        if not os.path.isfile(args.vendor_map):
            print(f"[error] Vendor map not found: {args.vendor_map}", file=sys.stderr)
            sys.exit(1)
        vendor_rules, rule_count = _load_vendor_map(args.vendor_map)
        print(f"[map]  Vendor map loaded: {args.vendor_map}  ({rule_count} rules)")
    else:
        print("[map]  No vendor map supplied — using built-in brand map only.")

    # ── Read CSV ───────────────────────────────────────────────────────────
    print(f"[csv]  Reading: {csv_path}")
    rows = _read_csv(csv_path)
    print(f"[csv]  Rows read: {len(rows):,}")

    if not rows:
        print("[error] CSV is empty or could not be parsed.", file=sys.stderr)
        sys.exit(1)

    # ── Supabase client — needed for item lookup even in dry-run ───────────
    sb_key = os.getenv("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_KEY"]
    sb     = create_client(os.environ["SUPABASE_URL"], sb_key)

    # ── Fetch item lookup (read-only in both dry-run and live) ─────────────
    print("[sb]   Fetching item lookup from Supabase items table …")
    item_lookup = _fetch_item_lookup(sb)
    print(f"[sb]   Items with item_number in Supabase: {len(item_lookup):,}")

    # ── Aggregate by vendor ────────────────────────────────────────────────
    agg, stats = _aggregate(rows, item_lookup, vendor_rules)
    vendors_found = len(agg)
    print(f"[agg]  Vendors identified: {vendors_found}")

    # ── Fetch existing KB fields (live run only) ───────────────────────────
    if not args.dry_run:
        existing_kb = _fetch_existing_kb(sb)
        print(f"[kb]   Existing kb_product_index rows: {len(existing_kb)}")
    else:
        existing_kb = {}
        print("[dry-run] Skipping kb_product_index reads — no writes in dry-run.")

    # ── Build upsert records ───────────────────────────────────────────────
    now_iso    = datetime.now(timezone.utc).isoformat()
    upsert_records: list[dict] = []
    row_counts: dict[str, int] = {}

    for vendor, bucket in sorted(agg.items(), key=lambda kv: -kv[1]["total_revenue"]):
        total_revenue      = bucket["total_revenue"]
        total_cost         = bucket["total_cost"]
        total_profit       = bucket["total_profit"]
        unique_vendor_skus = len(bucket["skus"])

        avg_margin_pct = (
            round((total_profit / total_revenue) * 100, 2) if total_revenue != 0 else 0.0
        )

        existing = existing_kb.get(vendor, {})
        upsert_records.append({
            "vendor":             vendor,
            "total_unique_skus":  unique_vendor_skus,
            "total_revenue":      round(total_revenue, 2),
            "total_cost":         round(total_cost, 2),
            "total_profit":       round(total_profit, 2),
            "avg_margin_pct":     avg_margin_pct,
            "updated_at":         now_iso,
            "coverage_status":    existing.get("coverage_status"),
            "kb_doc_count":       existing.get("kb_doc_count", 0),
            "has_install_manual": existing.get("has_install_manual", False),
        })
        row_counts[vendor] = bucket["row_count"]

    # ── Print summary ──────────────────────────────────────────────────────
    unique_skus   = len({r["item_number"] for r in rows if r["item_number"]})
    unknown_rev   = sum(r["amount"] for r in stats["unknown_rows"])

    print()
    print("=" * 68)
    print("  IMPORT SUMMARY")
    print("=" * 68)
    print(f"  CSV rows read                  : {len(rows):,}")
    print(f"  Rows with Item Number          : {stats['rows_with_item_number']:,}")
    print(f"  Rows without Item Number       : {stats['rows_no_item_number']:,}  (service / misc)")
    print(f"  Unique SKUs in CSV             : {unique_skus:,}")
    print(f"  Matched to Supabase item_number: {stats['matched_to_supabase']:,}")
    print(f"  Item # not in items table      : {stats['unmatched_to_supabase']:,}")
    print(f"  Brand from vendor map          : {stats['vendor_from_map']:,}")
    print(f"  Brand from Supabase name       : {stats['vendor_from_sb']:,}")
    print(f"  Still Unknown after all layers : {len(stats['unknown_rows']):,}")
    print(f"  Unknown vendor revenue         : ${unknown_rev:,.2f}")
    print(f"  Vendors to upsert              : {vendors_found}")
    print()
    print(f"  {'Vendor':<32}  {'SKUs':>5}  {'Revenue':>12}  {'Profit':>10}  {'Margin':>7}  {'Rows':>5}")
    print(f"  {'-'*32}  {'-'*5}  {'-'*12}  {'-'*10}  {'-'*7}  {'-'*5}")
    for rec in upsert_records:
        print(
            f"  {rec['vendor']:<32}  "
            f"{rec['total_unique_skus']:>5}  "
            f"${rec['total_revenue']:>11,.2f}  "
            f"${rec['total_profit']:>9,.2f}  "
            f"{rec['avg_margin_pct']:>6.1f}%  "
            f"{row_counts.get(rec['vendor'], 0):>5}"
        )
    print("=" * 68)

    # ── Top 25 Unknown rows ────────────────────────────────────────────────
    unknown_sorted = sorted(stats["unknown_rows"], key=lambda r: -abs(r["amount"]))
    if unknown_sorted:
        n = min(25, len(unknown_sorted))
        print()
        print(f"  TOP {n} Unknown rows by |revenue|")
        print(f"  {'Item #':<18}  {'Supabase Name':<30}  {'Amount':>10}")
        print(f"  {'-'*18}  {'-'*30}  {'-'*10}")
        for r in unknown_sorted[:n]:
            print(
                f"  {r['item_number'][:18]:<18}  "
                f"{(r['supabase_name'] or r['csv_item'])[:30]:<30}  "
                f"${r['amount']:>9,.2f}"
            )

    # ── Dry-run: write audit CSVs ──────────────────────────────────────────
    if args.dry_run:
        repo_dir       = os.path.dirname(os.path.abspath(__file__))
        unknowns_path  = os.path.join(repo_dir, "kb_import_unknowns.csv")
        summary_path   = os.path.join(repo_dir, "kb_import_vendor_summary.csv")

        _write_unknowns_csv(stats["unknown_rows"], unknowns_path)
        _write_vendor_summary_csv(upsert_records, row_counts, summary_path)

        print()
        print(f"[dry-run] Audit files written:")
        print(f"          {unknowns_path}")
        print(f"          {summary_path}")
        print()
        print("[dry-run] No kb_product_index writes performed.")
        print()
        print("  Next steps:")
        print("  1. Open kb_import_unknowns.csv, fill blank_vendor_to_fill for top revenue rows.")
        print("  2. Add rules to kb_vendor_map.csv (exact/prefix/contains).")
        print("  3. Re-run dry-run with --vendor-map kb_vendor_map.csv to verify.")
        print("  4. Approve, then run live:")
        print(f'     python import_kb_product_index.py --csv "{csv_path}" --vendor-map kb_vendor_map.csv')
        return

    # ── Live upsert ────────────────────────────────────────────────────────
    print(f"\n[upsert] Writing {len(upsert_records)} vendor rows to kb_product_index …")
    BATCH = 100
    for i in range(0, len(upsert_records), BATCH):
        batch = upsert_records[i : i + BATCH]
        sb.table("kb_product_index").upsert(batch, on_conflict="vendor").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.")

    print(f"\n[done]  {len(upsert_records)} vendor rows upserted to kb_product_index.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
