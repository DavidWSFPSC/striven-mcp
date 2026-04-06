"""
Flask API server — WilliamSmith chat + Striven live data

# IMPORTANT: TERMINOLOGY MAPPING
# ─────────────────────────────────────────────────────────────
# "Estimates" in our business  =  "Sales Orders" in Striven API
#
# There is NO /estimates endpoint in Striven. Every call that
# fetches or searches estimates internally uses:
#
#     POST /v1/sales-orders/search
#
# The external route names (/search-estimates, etc.) and the
# Claude tool names (search_estimates_by_customer, etc.) keep
# the word "estimates" for business clarity.  Only the internal
# Striven API calls use "sales-orders".
# ─────────────────────────────────────────────────────────────

Exposes (external names — "estimates" language preserved):
  /health
  /search-estimates          → POST /v1/sales-orders/search
  /get-estimate/<id>         → GET  /v1/sales-orders/{id}
  /missing-portal-flag       → paginates /v1/sales-orders/search
  /gas-log-audit-export      → CSV download (Excel-compatible)
  /gas-log-audit-pdf         → PDF download (reportlab)
  /sync-estimates            → full paginated pull → Supabase
  /estimates/count           → Supabase count
  /estimates/high-value      → Supabase query
  /estimates/by-customer     → Supabase query
  /                          → WilliamSmith chat UI
  /api/chat                  → agentic Claude loop
  /logs                      → admin search history

SAFETY POLICY:
  - Striven is NEVER written to. All Striven calls are read-only.
  - No endpoint in this file modifies Striven data.
"""

import os
import io
import csv
import json
import anthropic
from flask import Flask, jsonify, request, render_template, Response
from dotenv import load_dotenv
from requests import HTTPError

from services.striven import StrivenClient
from services.sync import sync_estimates_to_supabase
from services.supabase_client import (
    count_estimates,
    get_high_value_estimates,
    get_estimates_by_customer,
    log_chat,
    get_chat_logs,
)

# Load environment variables from .env (ignored in production if not present)
load_dotenv()

# ---------------------------------------------------------------------------
# Startup logging — Part 4
# ---------------------------------------------------------------------------
print("=" * 60, flush=True)
print("WilliamSmith API starting", flush=True)
print(f"  BASE_URL    : {os.getenv('BASE_URL', 'https://api.striven.com/v1')}", flush=True)
_cid = os.getenv("CLIENT_ID", "")
print(f"  CLIENT_ID   : {_cid[:6]}{'*' * max(0, len(_cid) - 6) if _cid else '(NOT SET)'}", flush=True)
print(f"  SUPABASE_URL: {os.getenv('SUPABASE_URL', '(not set)')}", flush=True)
print("=" * 60, flush=True)

# ---------------------------------------------------------------------------
# Startup credential check
# ---------------------------------------------------------------------------
client_id     = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

if not client_id or not client_secret:
    raise EnvironmentError("Missing CLIENT_ID or CLIENT_SECRET.")

app = Flask(__name__)

# Single shared client; token is cached internally and refreshed as needed
striven = StrivenClient()
print("StrivenClient initialised — ready to serve live data.", flush=True)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
def health():
    """Simple liveness probe — returns 200 if the server is running."""
    return jsonify({"status": "ok", "service": "striven-api"})


# ---------------------------------------------------------------------------
# Estimates
# ---------------------------------------------------------------------------

@app.get("/get-estimate/<int:estimate_id>")
def get_estimate(estimate_id: int):
    """
    Fetch a single estimate from Striven by ID.

    Path param:
        estimate_id — integer Striven estimate ID
    """
    try:
        data = striven.get_estimate(estimate_id)
        return jsonify(data)
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/search-estimates")
def search_estimates():
    """
    Search estimates (Striven sales orders) with optional query filters.

    Query params (all optional):
        pageIndex       — zero-based page number (default 0)
        pageSize        — results per page (default 25)
        customerId      — filter by customer ID (int)
        number          — filter by sales order number
        name            — filter by sales order name
        statusChangedTo — 18=Incomplete 19=Quoted 20=Pending 22=Approved 25=In Progress 27=Completed
        dateCreatedFrom — ISO 8601 start date
        dateCreatedTo   — ISO 8601 end date
    """
    args = request.args

    # If ?customer=<name> is provided, delegate to the fully-paginated helper
    # so this endpoint returns ALL estimates for that customer, not just one page.
    customer_name = args.get("customer", "").strip()
    if customer_name:
        try:
            result = _paginated_customer_search(customer_name)
            return jsonify(result)
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 502
            return jsonify({"error": str(exc)}), status
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # No customer name — standard single-page pass-through for other filters
    body: dict = {
        "pageIndex": int(args.get("pageIndex", 1)),
        "pageSize":  int(args.get("pageSize", 25)),
    }

    if "customerId" in args:
        body["CustomerId"] = int(args["customerId"])
    if "number" in args:
        body["Number"] = args["number"]
    if "name" in args:
        body["Name"] = args["name"]
    if "statusChangedTo" in args:
        body["StatusChangedTo"] = int(args["statusChangedTo"])

    date_from = args.get("dateCreatedFrom")
    date_to   = args.get("dateCreatedTo")
    if date_from or date_to:
        date_range: dict = {}
        if date_from:
            date_range["DateFrom"] = date_from
        if date_to:
            date_range["DateTo"] = date_to
        body["DateCreatedRange"] = date_range

    try:
        raw        = striven.search_estimates(body)
        data       = raw.get("data") or []
        total      = raw.get("totalCount", 0)
        if not data:
            print(f"[search-estimates] WARNING: 'Data' key missing or null — keys={list(raw.keys())}", flush=True)
        print(f"[search-estimates] TotalCount={total} returned={len(data)}", flush=True)
        records = [_fmt(r) for r in data]
        return jsonify({
            "total":     total,
            "count":     len(records),
            "estimates": records,
        })
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Exploratory: Striven Reports endpoint
# GET /report-preview?key=<accessKey>&page=0&size=50
#
# Purpose: evaluate whether /v2/reports/{accessKey} can provide revenue
# totals and other calculated fields not available in /v1/sales-orders/search.
# ---------------------------------------------------------------------------

@app.get("/report-preview")
def report_preview():
    """
    Fetch a Striven report by access key and return the raw structure.

    Query params:
        key  — report accessKey (required)
        page — pageIndex, default 0
        size — pageSize, default 50

    This endpoint is exploratory — it logs top-level keys and a sample
    record so we can evaluate the report's data shape.
    """
    access_key = request.args.get("key", "").strip()
    if not access_key:
        return jsonify({"error": "Query param 'key' is required."}), 400

    page = int(request.args.get("page", 0))
    size = int(request.args.get("size", 50))

    import requests as _requests
    try:
        # /v2/reports is a separate version prefix — call directly with auth headers
        url     = f"https://api.striven.com/v2/reports/{access_key}"
        params  = {"pageIndex": page, "pageSize": size}
        headers = striven._get_headers()          # reuse cached OAuth token
        resp    = _requests.get(url, headers=headers, params=params, timeout=20)
        resp.raise_for_status()

        # Report endpoint may return JSON or binary (octet-stream)
        content_type = resp.headers.get("Content-Type", "")
        if "json" not in content_type:
            print(f"[report-preview] Non-JSON content-type: {content_type}", flush=True)
            return jsonify({"error": f"Report returned non-JSON ({content_type}). May be PDF/binary."}), 415

        raw = resp.json()

        print(f"[report-preview] accessKey={access_key}", flush=True)
        print(f"[report-preview] Top-level keys: {list(raw.keys()) if isinstance(raw, dict) else type(raw).__name__}", flush=True)

        data  = raw.get("data") or []
        total = raw.get("totalCount", 0)

        if not data:
            print(f"[report-preview] WARNING: 'Data' key missing or null — keys={list(raw.keys())}", flush=True)

        print(f"[report-preview] TotalCount={total}  records_returned={len(data)}", flush=True)
        print(f"[report-preview] First record sample: {data[0] if data else 'EMPTY'}", flush=True)

        return jsonify({
            "total":          total,
            "count":          len(data),
            "top_level_keys": list(raw.keys()),
            "first_record":   data[0] if data else None,
            "data":           data,
            "source":         "striven_live",
        })

    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Audit: missing Customer Portal flag
# ---------------------------------------------------------------------------

# The exact custom field name to audit (case-insensitive match for safety)
PORTAL_FLAG_FIELD = "do not show items on estimate in the customer portal display"


def _run_portal_flag_audit() -> dict:
    """
    Streaming portal-flag audit — checks every estimate without loading all into RAM.

    Paginates through POST /v1/sales-orders/search (100 records per page),
    processes each page immediately, and discards it — peak memory is one page
    of stubs at a time (never the full 3,000+ record list).

    The search endpoint returns customFields in each stub, so no per-record
    detail GET is required — this audit is purely search-based and fast.

    Returns a plain dict (not a Flask response) so _execute_tool() can reuse it.
    """
    import time as _time
    t_start   = _time.monotonic()
    PAGE_SIZE = 100

    print("[portal-flag-audit] Starting — streaming all estimates", flush=True)

    total_checked = 0
    broken: list[dict] = []
    page_index = 0

    while True:
        raw  = striven.search_sales_orders({"PageIndex": page_index, "PageSize": PAGE_SIZE})
        data = raw.get("data") or []
        total_count = raw.get("totalCount", 0)

        if not data:
            print("[portal-flag-audit] Empty page — stopping.", flush=True)
            break

        print(
            f"[portal-flag-audit] Page {page_index}: {len(data)} stubs "
            f"(total={total_count})",
            flush=True,
        )

        for est in data:
            total_checked += 1
            custom_fields = est.get("customFields") or []

            # Locate the portal flag field by exact name (case-insensitive)
            portal_field = next(
                (
                    f for f in custom_fields
                    if isinstance(f.get("name"), str)
                    and f["name"].strip().lower() == PORTAL_FLAG_FIELD
                ),
                None,
            )

            # Flag if missing entirely OR value is not True
            is_set = (
                portal_field is not None
                and portal_field.get("value") is True
            )

            if not is_set:
                customer     = est.get("customer")  or est.get("Customer")  or {}
                sales_rep    = est.get("salesRep")  or est.get("SalesRep")  or {}
                status_obj   = est.get("status")    or est.get("Status")    or {}
                broken.append({
                    "estimate_id":     est.get("id")     or est.get("Id"),
                    "estimate_number": est.get("number") or est.get("Number"),
                    "estimate_name":   est.get("name")   or est.get("Name"),
                    "customer_name":   customer.get("name")  or customer.get("Name"),
                    "sales_rep":       sales_rep.get("name") or sales_rep.get("Name"),
                    "status":          status_obj.get("name") or status_obj.get("Name"),
                    "url": (
                        "https://app.striven.com/next/crm#/sales-orders/"
                        + str(est.get("id") or est.get("Id") or "")
                    ),
                })

        if total_checked >= total_count:
            break

        page_index += 1

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[portal-flag-audit] Complete — checked={total_checked} "
        f"missing={len(broken)} elapsed={elapsed}s",
        flush=True,
    )

    return {
        "summary": {
            "total_estimates_checked": total_checked,
            "total_missing_flag":      len(broken),
        },
        "records": broken,
    }


@app.get("/missing-portal-flag")
@app.get("/portal-flag-audit")
def missing_portal_flag():
    """
    Audit every estimate for the Customer Portal display flag.

    Streams through all records page-by-page (no RAM accumulation).
    Also reachable at /portal-flag-audit.
    """
    try:
        return jsonify(_run_portal_flag_audit())
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Audit: Gas Log installs missing a Removal Fee line item
# ---------------------------------------------------------------------------

# Gas log detection keywords (checked against item.name and description).
# "gas log" and "burner" are specific enough to avoid false positives.
# Plain "log" was removed — too broad, matched unrelated descriptions.
#
# NOTE: Category-based detection was attempted but Striven does not populate
# the Category field on items in practice — all returned category='' empty.
# Keyword matching on item.name is the correct approach for this dataset.
GAS_LOG_KEYWORDS = ("gas log", "burner")


def _item_text(item: dict, *keys) -> str:
    """
    Extract a single lowercased text value from a line-item dict.

    Tries each key in order; handles both plain strings and nested objects
    like {"Name": "Burner & Gas Logs"}.  Returns "" if nothing is found.

    Defined at module level (not inside the loop) so it is created once.
    """
    for k in keys:
        v = item.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip().lower()
        if isinstance(v, dict):
            name = v.get("Name") or v.get("name") or ""
            if name:
                return name.strip().lower()
    return ""


# ---------------------------------------------------------------------------
# 5-minute in-memory cache for the full gas log audit (limit=None only).
# Keyed on None (full scan). Test runs with a limit bypass the cache.
# Thread-safety: single Gunicorn worker, so no lock needed.
# ---------------------------------------------------------------------------
_GAS_LOG_CACHE: dict = {"result": None, "ts": 0.0}
_GAS_LOG_CACHE_TTL = 300  # seconds


def _run_gas_log_audit(limit: int | None = None) -> dict:
    """
    Core gas-log audit logic — callable from the Flask route OR /chat.

    Streaming single-pass design (fixes OOM on Render free tier):
      - Fetches one page of estimates at a time (100 records)
      - Immediately GETs full detail for each record in the page
      - Discards detail after inspection — never accumulates in RAM
      - Peak memory: ~100 search stubs + 1 detail record at any moment

    limit: cap total estimates inspected (for test runs — default: all 2025)

    Returns a plain dict (not a Flask response) so /chat can reuse it.
    """
    import time as _time
    t_start   = _time.monotonic()
    PAGE_SIZE = 100

    # ── Cache check (full-scan calls only) ───────────────────────────────────
    # Test runs with an explicit limit always bypass the cache so developers
    # get fresh data.  Production calls (limit=None) reuse a recent result
    # for up to 5 minutes, making the /api/chat response feel instant.
    if limit is None:
        age = _time.monotonic() - _GAS_LOG_CACHE["ts"]
        if _GAS_LOG_CACHE["result"] is not None and age < _GAS_LOG_CACHE_TTL:
            print(
                f"[gas-log-audit] CACHE HIT — age={age:.0f}s, "
                f"returning cached result immediately.",
                flush=True,
            )
            return _GAS_LOG_CACHE["result"]
        print(
            f"[gas-log-audit] Cache miss (age={age:.0f}s) — running full scan.",
            flush=True,
        )

    # Only audit active estimates — Quoted(19), Pending Approval(20),
    # Approved(22), In Progress(25).  Completed(27) and Incomplete(18)
    # don't need the audit and dramatically shrink the pool size.
    ACTIVE_STATUSES = (19, 20, 22, 25)

    print(
        f"[gas-log-audit] Starting — 2025 estimates with statuses {ACTIVE_STATUSES}",
        flush=True,
    )

    total_inspected  = 0
    gas_log_installs = 0
    matches: list[dict] = []
    first_record_logged  = False

    # Outer loop: iterate over each active status separately.
    # This avoids accumulating all statuses at once and makes progress clear.
    for status_id in ACTIVE_STATUSES:
        print(f"[gas-log-audit] ── Status {status_id} ──", flush=True)
        page_index   = 0
        status_total = None

        while True:
            body = {
                "PageIndex":       page_index,
                "PageSize":        PAGE_SIZE,
                "StatusChangedTo": status_id,
                "DateCreatedRange": {
                    "DateFrom": "2025-01-01",
                    "DateTo":   "2025-12-31",
                },
            }
            raw  = striven.search_sales_orders(body)
            data = raw.get("data") or []

            if status_total is None:
                status_total = raw.get("totalCount", 0)
                print(
                    f"[gas-log-audit] Status {status_id} total: {status_total}",
                    flush=True,
                )

            if not data:
                break

            print(
                f"[gas-log-audit] Status {status_id} page {page_index}: "
                f"{len(data)} stubs",
                flush=True,
            )

            # ── Process each stub immediately — no accumulation ──────────────
            for r in data:
                if limit and total_inspected >= limit:
                    break

                customer  = r.get("Customer") or r.get("customer") or {}
                est_id    = r.get("Id")     or r.get("id")
                est_num   = r.get("Number") or r.get("number")
                cust_name = customer.get("Name") or customer.get("name")

                if not est_id:
                    continue

                # GET full detail — discarded immediately after inspection
                detail     = striven.get_estimate(est_id)
                line_items = (
                    detail.get("lineItems")
                    or detail.get("items")
                    or detail.get("LineItems")
                    or []
                )

                # Log field structure on the very first record only
                if not first_record_logged:
                    first_record_logged = True
                    print(f"[gas-log-audit] Detail keys: {list(detail.keys())}", flush=True)
                    if line_items:
                        print(f"[gas-log-audit] Line item keys: {list(line_items[0].keys())}", flush=True)
                        print(f"[gas-log-audit] Line item sample: {line_items[0]}", flush=True)
                    else:
                        print("[gas-log-audit] No line items on first record", flush=True)

                total_inspected += 1

                # ── Diagnostic: log item names on first 3 estimates ──────────
                if total_inspected <= 3:
                    item_names = [
                        _item_text(li, "item", "description", "Description")
                        for li in line_items
                    ]
                    print(
                        f"[gas-log-audit] est#{est_num} — "
                        f"{len(line_items)} line items: {item_names}",
                        flush=True,
                    )

                # Gas log detection — keyword match on item.name + description.
                # item.name is the product name (nested dict confirmed from logs).
                # Categories not used — Striven leaves them blank in practice.
                has_gas_log = any(
                    any(kw in _item_text(li, "item", "description", "Description")
                        for kw in GAS_LOG_KEYWORDS)
                    for li in line_items
                )

                if not has_gas_log:
                    continue

                gas_log_installs += 1

                # Removal fee — keyword match on item name + description.
                # "Gas Log Removal Fee" is the expected product name.
                has_removal_fee = any(
                    (lambda t: "removal" in t and "log" in t)(
                        _item_text(li, "item", "description", "Description")
                    )
                    for li in line_items
                )

                if not has_removal_fee:
                    matches.append({
                        "estimate_id":     est_id,
                        "estimate_number": est_num,
                        "customer_name":   cust_name,
                        "url":             f"https://app.striven.com/next/crm#/sales-orders/{est_id}",
                    })

                if total_inspected % 25 == 0:
                    print(
                        f"[gas-log-audit] Progress: {total_inspected} inspected — "
                        f"gas log installs: {gas_log_installs}, missing fee: {len(matches)}",
                        flush=True,
                    )

            # Honour optional test limit
            if limit and total_inspected >= limit:
                print(f"[gas-log-audit] Limit {limit} reached — stopping.", flush=True)
                break

            # All pages for this status exhausted
            if status_total is not None and (page_index + 1) * PAGE_SIZE >= status_total:
                print(
                    f"[gas-log-audit] Status {status_id} exhausted.",
                    flush=True,
                )
                break

            page_index += 1

        if limit and total_inspected >= limit:
            break

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[gas-log-audit] Complete — checked={total_inspected} "
        f"gas_log_installs={gas_log_installs} "
        f"missing_removal_fee={len(matches)} elapsed={elapsed}s",
        flush=True,
    )

    result = {
        "total_checked":       total_inspected,
        "gas_log_installs":    gas_log_installs,
        "missing_removal_fee": len(matches),
        "matches":             matches,
    }

    # ── Cache write (full-scan calls only) ───────────────────────────────────
    import time as _time2
    if limit is None:
        _GAS_LOG_CACHE["result"] = result
        _GAS_LOG_CACHE["ts"]     = _time2.monotonic()
        print(f"[gas-log-audit] Result cached — TTL={_GAS_LOG_CACHE_TTL}s", flush=True)

    return result


@app.get("/gas-log-audit")
def gas_log_audit():
    """
    Thin route wrapper around _run_gas_log_audit().
    Accepts optional ?limit=N query param for test runs.
    """
    raw_limit     = request.args.get("limit", "").strip()
    inspect_limit = int(raw_limit) if raw_limit.isdigit() else None
    try:
        return jsonify(_run_gas_log_audit(limit=inspect_limit))
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Gas Log Audit — export endpoints (CSV + PDF)
# Both reuse _run_gas_log_audit() — no logic is duplicated.
# ---------------------------------------------------------------------------

@app.get("/gas-log-audit-export")
def gas_log_audit_export():
    """
    Run the gas log audit (limit=200) and return results as a
    downloadable CSV file compatible with Excel.

    Columns: Estimate # | Customer | URL | Issue
    """
    try:
        results = _run_gas_log_audit(limit=200)
        matches = results.get("matches") or []

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["Estimate #", "Customer", "URL", "Issue"])
        for m in matches:
            writer.writerow([
                m.get("estimate_number", ""),
                m.get("customer_name", ""),
                m.get("url", ""),
                "Missing Gas Log Removal Fee",
            ])

        csv_bytes = buf.getvalue().encode("utf-8-sig")  # BOM for Excel
        return Response(
            csv_bytes,
            mimetype="text/csv",
            headers={"Content-Disposition": "attachment; filename=gas_log_audit.csv"},
        )

    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/gas-log-audit-pdf")
def gas_log_audit_pdf():
    """
    Run the gas log audit (limit=200) and return results as a
    downloadable PDF report with a formatted table.

    Columns: Estimate # | Customer | Issue
    """
    try:
        from reportlab.lib.pagesizes import letter
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch

        results = _run_gas_log_audit(limit=200)
        matches = results.get("matches") or []

        buf = io.BytesIO()
        doc = SimpleDocTemplate(
            buf,
            pagesize=letter,
            leftMargin=0.75 * inch,
            rightMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
        )

        styles = getSampleStyleSheet()
        elements = []

        # Title
        elements.append(Paragraph("Gas Log Audit — Missing Removal Fee", styles["Title"]))
        elements.append(Spacer(1, 0.15 * inch))

        # Summary line
        total   = results.get("total_checked", 0)
        installs = results.get("gas_log_installs", 0)
        missing  = results.get("missing_removal_fee", len(matches))
        summary_text = (
            f"Estimates checked: {total} &nbsp;|&nbsp; "
            f"Gas log installs found: {installs} &nbsp;|&nbsp; "
            f"Missing removal fee: {missing}"
        )
        elements.append(Paragraph(summary_text, styles["Normal"]))
        elements.append(Spacer(1, 0.2 * inch))

        # Table
        header = [["Estimate #", "Customer", "Issue"]]
        rows = [
            [
                m.get("estimate_number", ""),
                m.get("customer_name", ""),
                "Missing Gas Log Removal Fee",
            ]
            for m in matches
        ]
        table_data = header + rows

        col_widths = [1.2 * inch, 3.2 * inch, 2.6 * inch]
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            # Header row
            ("BACKGROUND",   (0, 0), (-1, 0), colors.HexColor("#2c3e50")),
            ("TEXTCOLOR",    (0, 0), (-1, 0), colors.white),
            ("FONTNAME",     (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE",     (0, 0), (-1, 0), 10),
            ("BOTTOMPADDING",(0, 0), (-1, 0), 8),
            ("TOPPADDING",   (0, 0), (-1, 0), 8),
            # Data rows
            ("FONTNAME",     (0, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE",     (0, 1), (-1, -1), 9),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, colors.HexColor("#f2f2f2")]),
            ("TOPPADDING",   (0, 1), (-1, -1), 5),
            ("BOTTOMPADDING",(0, 1), (-1, -1), 5),
            # Grid
            ("GRID",         (0, 0), (-1, -1), 0.4, colors.HexColor("#cccccc")),
            ("ALIGN",        (0, 0), (-1, -1), "LEFT"),
            ("VALIGN",       (0, 0), (-1, -1), "MIDDLE"),
        ]))
        elements.append(table)

        doc.build(elements)
        pdf_bytes = buf.getvalue()

        return Response(
            pdf_bytes,
            mimetype="application/pdf",
            headers={"Content-Disposition": "attachment; filename=gas_log_audit.pdf"},
        )

    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Data pipeline: Striven → Supabase
# ---------------------------------------------------------------------------

@app.get("/sync-estimates")
def sync_estimates():
    """
    Trigger an estimate sync from Striven into Supabase.

    Pulls records from Striven (READ-ONLY), transforms them, and upserts
    into the `estimates` table in Supabase. Safe to call repeatedly —
    upsert logic means no duplicates are created.

    Query params (optional):
        limit — max records to sync this run (default 50 for safety).
                Pass limit=all to sync every record (use with caution on
                large datasets — ~9 300 records = ~94 Striven API calls).

    Example:
        GET /sync-estimates           → syncs first 50 records
        GET /sync-estimates?limit=200 → syncs first 200 records
        GET /sync-estimates?limit=all → full sync (all records)

    SAFETY: This endpoint never writes to Striven.
    """
    # Parse optional limit param
    raw_limit = request.args.get("limit", "50")

    if raw_limit.lower() == "all":
        limit = None           # sync everything
    else:
        try:
            limit = int(raw_limit)
        except ValueError:
            return jsonify({"error": f"Invalid limit value: {raw_limit!r}. Use an integer or 'all'."}), 400

    try:
        records_synced = sync_estimates_to_supabase(limit=limit)
        return jsonify({
            "status": "success",
            "records_synced": records_synced,
        })
    except HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else 502
        return jsonify({"error": str(exc)}), status
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Supabase query endpoints — read-only, Claude-facing
# ---------------------------------------------------------------------------

@app.get("/estimates/count")
def estimates_count():
    """Return the total number of estimates stored in Supabase."""
    try:
        return jsonify({"total": count_estimates()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/estimates/high-value")
def estimates_high_value():
    """
    Return up to 25 estimates where total > 10 000, sorted highest first.
    Source: Supabase. Never queries Striven.
    """
    try:
        records = get_high_value_estimates(min_total=10000, limit=25)
        return jsonify({"count": len(records), "records": records})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.get("/estimates/by-customer")
def estimates_by_customer():
    """
    Case-insensitive customer name search against Supabase.

    Query param:
        name — partial or full customer name (required)

    Example:
        GET /estimates/by-customer?name=clear+water
    """
    name = request.args.get("name", "").strip()
    if not name:
        return jsonify({"error": "Query param 'name' is required."}), 400

    try:
        records = get_estimates_by_customer(name)
        return jsonify({"count": len(records), "records": records})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Chat UI — WilliamSmith web interface
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are WilliamSmith — the business intelligence engine for WilliamSmith Fireplaces.
You have live, read-only access to the company's complete Striven dataset:
estimates, invoices, payments, bills, purchase orders, tasks, customers,
vendors, contacts, opportunities, and the product catalog.

════════════════════════════════════════════════════════
IDENTITY & PURPOSE
════════════════════════════════════════════════════════
You are NOT a search tool. You are an operations analyst.

Your job is to answer business questions — not retrieve data and hand it back.
Think like a CFO, ops manager, and sales director simultaneously.
Prioritise: money owed, process delays, workload risk, and revenue opportunity.

This system exists to replace opinion and blame with DATA.
When processes break, you find where. When numbers are questioned, you show them.
The goal is a system that leadership, operations, sales, and accounting rely on
daily to understand what is actually happening in the business.

You serve six core functions:
  1. SALES SUPPORT     — match products to customer requirements; surface margin data
  2. SCHEDULING        — show technician workload; help group jobs geographically
  3. OPERATIONS        — track the estimate → approval → scheduling → install pipeline
  4. ACCOUNTING        — explain what customers owe, what has been paid, what is outstanding
  5. LEADERSHIP        — identify profitable vs problematic products; flag revenue risk
  6. SALES MANAGEMENT  — track rep performance; surface missed steps and recurring errors
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
CARDINAL RULES
════════════════════════════════════════════════════════
1. Always call a tool. Never answer from memory. Never guess a number.
2. Always summarise first. Totals and key findings before any detail.
3. Never dump raw lists. Max 5 rows by default; 10 only if the user asks.
4. This system is READ-ONLY. If asked to create, modify, or delete anything:
   "This system is read-only and cannot make changes."
════════════════════════════════════════════════════════

ESTIMATES & SALES ORDERS
In Striven, "estimates" and "sales orders" are the same record type.
Status codes: 18=Incomplete  19=Quoted  20=Pending Approval  22=Approved
              25=In Progress  27=Completed

════════════════════════════════════════════════════════
TOOL ROUTING
════════════════════════════════════════════════════════
ESTIMATES & PIPELINE
  count / how many estimates              → count_estimates
  biggest / highest value jobs            → high_value_estimates
  estimates for [customer]                → search_estimates_by_customer
  approved / quoted / in-progress / open  → search_estimates with status filter
  estimates in a date range               → search_estimates with date range
  estimate #N / detail on one job         → get_estimate_by_id
  portal flag check                       → portal_flag_audit
  ANY gas log / removal / burner          → gas_log_audit (mandatory — see below)
  deals / opportunities / pipeline        → search_opportunities

OPERATIONS ANALYSIS
  where are jobs breaking                 → analyze_job_pipeline
  pipeline delays / ops report            → analyze_job_pipeline
  which jobs have no preview task         → analyze_job_pipeline
  which jobs have no install scheduled    → analyze_job_pipeline
  how long from approval to install       → analyze_job_pipeline
  delayed jobs / approval to install      → analyze_job_pipeline
  process breakdown / pipeline report     → analyze_job_pipeline

FINANCIAL
  unpaid invoices / AR / who owes us      → search_invoices
  specific invoice detail                 → get_invoice_by_id
  vendor bills / AP / what we owe         → search_bills
  payments received / cash collected      → search_payments
  purchase orders / POs                   → search_purchase_orders

CUSTOMERS, VENDORS & CONTACTS
  find a customer                         → search_customers
  find a vendor or supplier               → search_vendors
  contacts at a company                   → search_contacts

TASKS & SCHEDULING
  open tasks / technician workload        → search_tasks
  overdue tasks                           → search_tasks (due_to = past date)
  tasks on a specific job                 → search_tasks (related_entity_id)
  task detail                             → get_task_by_id

CATALOG
  products / services / pricing           → search_items
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
FAST MODE vs DEEP MODE
════════════════════════════════════════════════════════
FAST MODE (default — use for every query unless told otherwise)
  • One API call. PageSize=25. PageIndex=0.
  • Do NOT use active_only=true (that triggers 4 serial calls).
  • Omit status filter for general queries — Striven returns most recent first.

DEEP MODE (only when user explicitly says: "full analysis", "scan everything",
           "comprehensive", "all estimates", "full report", "deep dive",
           "every estimate", "full dataset", "show me everything")
  • May use active_only=true, PageSize=50, and follow-up calls.
  • Cap at 100 total records even in deep mode.
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
ANALYSIS — HOW TO THINK ABOUT EACH DATA TYPE
════════════════════════════════════════════════════════
When data is returned from a tool, do NOT reformat it and hand it back.
Compute. Rank. Compare. Surface risk. Draw a conclusion.

ESTIMATES / PIPELINE
  • Total pipeline value = sum(total) across all returned records
  • Count by status — where are jobs getting stuck?
  • Flag estimates open >90 days without a status change
  • Flag approved estimates with no tasks and no scheduled install date

INVOICES / ACCOUNTS RECEIVABLE
  • total_unpaid = sum(amount_due) across all unpaid records
  • avg_invoice = total_invoiced / count
  • Separate overdue (past due date) from not-yet-due
  • Rank customers by balance owed, highest first

PAYMENTS / CASH COLLECTED
  • total_collected = sum(amount)
  • Surface recent activity (last 5 payments received)
  • When combined with invoices: compare paid vs outstanding per customer

BILLS / ACCOUNTS PAYABLE
  • total_owed = sum(amount_due)
  • Group and rank by vendor
  • Flag anything past due

TASKS / WORKLOAD & SCHEDULING
  • count_overdue = tasks with due date in the past
  • Group by assignee — who is overloaded?
  • Surface tasks with no due date (scheduling gap)
  • Flag jobs in "In Progress" status with no open tasks (orphaned jobs)
  • When addresses are visible, note geographic clustering opportunities

OPERATIONS PIPELINE (process delay analysis)
  The expected sequence for every job is:
    Estimate created → Approved → Preview task created (within 3 days) → Install scheduled → Completed
  Use analyze_job_pipeline — it does this automatically.
  When presenting results, always lead with the percentages:
    "X of Y approved jobs (Z%) have no preview task scheduled."
  Then break down by sales rep. Then show example problem jobs.

LEADERSHIP / MARGIN & PRODUCT ANALYSIS
  • Rank products (items) by frequency in estimates — what sells most?
  • When service or callback task data is available, flag items generating follow-up work
  • Identify which products or job types result in the largest invoices
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
CROSS-TOOL REASONING
════════════════════════════════════════════════════════
Use multiple tools together when the question requires it.

"Who owes us money?"
  → search_invoices (unpaid) → sum amount_due → rank by balance → flag overdue

"What have customers paid vs what they owe?"
  → search_invoices + search_payments → compare per customer

"Which customers generate the most revenue?"
  → search_invoices (all) → sum total per customer → rank top 5

"What's our vendor spend?"
  → search_bills + search_purchase_orders → group by vendor → sum totals

"What jobs are stuck?"
  → search_estimates (status=25) → flag old open dates → check for tasks

"What's our workload this week?"
  → search_tasks (due_to = end of week) → count by assignee

"Is [customer] paid up?"
  → search_customers → get ID
  → search_invoices (customer_id) + search_payments (customer_id)
  → compare: total invoiced vs total paid → state the net balance clearly

"Where is the process breaking down?"
  → search_estimates (approved) + search_tasks → find approved jobs with no tasks
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
RESPONSE FORMAT — ALWAYS IN THIS ORDER
════════════════════════════════════════════════════════
1. HEADLINE — one bold sentence with the key number or finding.
   ✓ "**You have 42 unpaid invoices totalling $128,450.**"
   ✗ "Based on the data returned, I can see that there are invoices..."

2. KEY METRICS — 2–4 bullet lines (totals, counts, averages, risk flags).
   • Total outstanding: $128,450
   • Average balance: $3,058
   • Oldest unpaid: 94 days

3. ANOMALIES — only when present in the actual data:
   ⚠ N invoices have no amount recorded
   ⚠ N records missing customer name
   ⚠ N estimates approved >60 days ago with no scheduled install

4. TOP RESULTS — ranked table, max 5 rows by default.
   Show the most actionable records (highest value, most overdue, etc.)
   Use only the columns relevant to the question.
   End with "…and N more" if there are additional records.

5. ONE follow-up offer. One sentence maximum.

HARD FORMAT RULES
  ✗ No raw dumps of 25+ rows
  ✗ No "based on the data" / "I can see that" / "it appears"
  ✗ No explaining what tool was called or how the search worked
  ✓ "show me all" / "full list" → up to 25 rows
  ✓ Dollar amounts rounded to nearest dollar with $ sign
  ✓ Dates as Mon D, YYYY (e.g. Apr 5, 2026)
  ✓ Percentages where useful (e.g. "60% unpaid")
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
PIPELINE ANALYSIS — FORMAT WHEN analyze_job_pipeline RETURNS
════════════════════════════════════════════════════════
ALWAYS present results in this exact order:

1. HEADLINE — total analysed and the single most critical finding.
   "**Of 20 approved jobs, 13 (65%) are missing at least one required step.**"

2. BREAKDOWN — one line per issue type, with count and percentage:
   • No preview task: X (Y%)
   • Preview created late (>3 days after approval): X (Y%)
   • No install scheduled: X (Y%)
   • Avg days from approval to preview: N days
   • Avg days from approval to install: N days

3. BY SALES REP — short table, reps sorted by number of issues:
   | Rep | Jobs | No Preview | No Install |
   Only show reps with at least one issue. Max 6 rows.

4. EXAMPLE PROBLEM JOBS — up to 10 rows:
   | Estimate # | Customer | Approved | Issue |
   Use plain language for Issue: "No preview task", "Preview 8 days late", "No install scheduled"

5. ONE sentence offering to drill into a specific rep, date range, or job.

RULES:
  ✗ Never expose raw field names (no "no_preview_task", "preview_late_8d")
  ✓ Translate issue codes to plain English
  ✓ Always show percentages alongside counts
  ✓ If avg_days_to_preview or avg_days_to_install is null, say "not enough data"
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
GAS LOG AUDIT — NON-NEGOTIABLE RULE
════════════════════════════════════════════════════════
Trigger phrases — any of these IMMEDIATELY calls gas_log_audit, no exceptions:
  "gas log"  "gas log install"  "gas log removal"  "removal fee"
  "missing removal"  "burner install"  "burner log"  "show me gas log"

ONLY correct action: call gas_log_audit → report the numbers → list matches.
✗ Do NOT call search_estimates    ✗ Do NOT loop get_estimate_by_id
✗ Do NOT sample and extrapolate   ✗ Do NOT explain before calling

FORMAT when gas_log_audit returns:
  Bold: "**Y of X gas log installs are missing the removal fee.**"
  Line: "Revenue at risk: $Z ($200/job)"
  Table: Estimate # | Customer — max 10 rows, then "…and N more."
  One follow-up sentence.
════════════════════════════════════════════════════════"""

_CHAT_TOOLS = [
    {
        "name": "count_estimates",
        "description": "Return the total number of estimates stored in the database.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "high_value_estimates",
        "description": "Return up to 25 estimates with total value over $10,000, sorted highest first.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "search_estimates_by_customer",
        "description": "Search estimates by customer name. Case-insensitive, partial match supported.",
        "input_schema": {
            "type": "object",
            "properties": {
                "name": {"type": "string", "description": "Customer name or partial name"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "search_estimates",
        "description": (
            "Flexible estimate search by status, date range, or keyword. "
            "Status codes: 18=Incomplete 19=Quoted 20=Pending 22=Approved 25=In Progress 27=Completed. "
            "Set active_only=true to restrict to statuses 19,20,22,25 in one call (recommended default). "
            "NOT for gas log / removal fee questions — use gas_log_audit for those."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "status":      {"type": "integer", "description": "Filter by a single status ID (fast mode). Omit for unrestricted search."},
                "active_only": {"type": "boolean", "description": "DEEP MODE only — fans out across statuses 19,20,22,25 with 4 API calls. Only use when user explicitly asks for full/comprehensive results."},
                "date_from":   {"type": "string",  "description": "Start date YYYY-MM-DD"},
                "date_to":     {"type": "string",  "description": "End date YYYY-MM-DD"},
                "keyword":     {"type": "string",  "description": "Filter by estimate name"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25, max 50)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_estimate_by_id",
        "description": (
            "Fetch the full detail of a single estimate by its Striven ID. "
            "For ONE specific estimate only — never call this in a loop to audit gas logs; "
            "use gas_log_audit instead."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "estimate_id": {"type": "integer", "description": "Striven estimate / sales order ID"},
            },
            "required": ["estimate_id"],
        },
    },
    {
        "name": "portal_flag_audit",
        "description": (
            "Audit ALL estimates and return those missing the Customer Portal display flag. "
            "Takes 30–60 seconds."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "gas_log_audit",
        "description": (
            "Scan all 2025 estimates and find those that have a gas log or burner install "
            "but are missing a Gas Log Removal Fee line item. "
            "Returns total estimates checked, gas log installs found, number missing the "
            "removal fee, estimated revenue impact, and a list of affected estimates with "
            "direct links. Use this for ANY question about gas logs, removal fees, or burner installs."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "search_customers",
        "description": (
            "Search customers by name. Returns customer ID, name, number, email, and phone. "
            "Use when the user asks about a specific customer or wants to look up contact info."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string",  "description": "Customer name or partial name to search"},
                "page_size": {"type": "integer", "description": "Max results to return (default 25)"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "search_tasks",
        "description": (
            "Search tasks in Striven. Use to understand workload, find overdue tasks, "
            "check what's assigned to someone, or find tasks linked to a specific estimate or project. "
            "Supports filtering by status, assignee, due date range, and related entity."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "status_id":        {"type": "integer", "description": "Filter by task status ID"},
                "assigned_to":      {"type": "integer", "description": "Filter by assignee user ID"},
                "task_type_id":     {"type": "integer", "description": "Filter by task type ID"},
                "due_from":         {"type": "string",  "description": "Due date range start YYYY-MM-DD"},
                "due_to":           {"type": "string",  "description": "Due date range end YYYY-MM-DD"},
                "related_entity_id":{"type": "integer", "description": "Filter by linked estimate or project ID"},
                "page_size":        {"type": "integer", "description": "Max results (default 25, max 50)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_task_by_id",
        "description": "Fetch full detail of a single task by its Striven ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "task_id": {"type": "integer", "description": "Striven task ID"},
            },
            "required": ["task_id"],
        },
    },
    # ── Financial ─────────────────────────────────────────────────────────────
    {
        "name": "search_invoices",
        "description": (
            "Search customer invoices. Use for: unpaid invoices, overdue balances, "
            "revenue by customer, or invoice history. "
            "Returns id, invoice_number, customer_name, total, amount_due, status, date_created, date_due."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Filter by customer ID"},
                "status_id":   {"type": "integer", "description": "Filter by status ID"},
                "date_from":   {"type": "string",  "description": "Created on or after YYYY-MM-DD"},
                "date_to":     {"type": "string",  "description": "Created on or before YYYY-MM-DD"},
                "due_from":    {"type": "string",  "description": "Due date from YYYY-MM-DD"},
                "due_to":      {"type": "string",  "description": "Due date to YYYY-MM-DD"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_invoice_by_id",
        "description": "Fetch full detail of a single invoice by its Striven ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_id": {"type": "integer", "description": "Striven invoice ID"},
            },
            "required": ["invoice_id"],
        },
    },
    {
        "name": "search_bills",
        "description": (
            "Search vendor bills (accounts payable). Use for: what we owe vendors, "
            "unpaid bills, AP aging. "
            "Returns id, bill_number, vendor_name, total, amount_due, status, date_due."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "vendor_id": {"type": "integer", "description": "Filter by vendor ID"},
                "status_id": {"type": "integer", "description": "Filter by status ID"},
                "date_from": {"type": "string",  "description": "Created on or after YYYY-MM-DD"},
                "date_to":   {"type": "string",  "description": "Created on or before YYYY-MM-DD"},
                "page_size": {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "search_payments",
        "description": (
            "Search payments received from customers. Use for: payment history, "
            "cash received, which customers have paid. "
            "Returns id, customer_name, amount, method, reference, date."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Filter by customer ID"},
                "date_from":   {"type": "string",  "description": "Payment date from YYYY-MM-DD"},
                "date_to":     {"type": "string",  "description": "Payment date to YYYY-MM-DD"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "search_purchase_orders",
        "description": (
            "Search purchase orders sent to vendors. Use for: procurement activity, "
            "vendor spending, open POs. "
            "Returns id, po_number, vendor_name, total, status, date_created."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "vendor_id": {"type": "integer", "description": "Filter by vendor ID"},
                "status_id": {"type": "integer", "description": "Filter by status ID"},
                "date_from": {"type": "string",  "description": "Created on or after YYYY-MM-DD"},
                "date_to":   {"type": "string",  "description": "Created on or before YYYY-MM-DD"},
                "page_size": {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    # ── Catalog ───────────────────────────────────────────────────────────────
    {
        "name": "search_items",
        "description": (
            "Search the product/service catalog. Use for: what items/services we sell, "
            "pricing, finding a specific product. "
            "Returns id, name, description, type, category, price, is_active."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword":   {"type": "string",  "description": "Search by item name"},
                "page_size": {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    # ── CRM ───────────────────────────────────────────────────────────────────
    {
        "name": "search_vendors",
        "description": (
            "Search vendors. Use for: vendor lookup, who we buy from, vendor contact info. "
            "Returns id, name, number, email, phone, contact_name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name":      {"type": "string",  "description": "Vendor name or partial name"},
                "page_size": {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "search_contacts",
        "description": (
            "Search contacts (people linked to customers or vendors). "
            "Returns id, first_name, last_name, email, phone, customer_name."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name":        {"type": "string",  "description": "Contact name or partial name"},
                "customer_id": {"type": "integer", "description": "Filter by customer ID"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "search_opportunities",
        "description": (
            "Search opportunities / sales pipeline. Use for: deals in progress, "
            "pipeline value, win/loss analysis. "
            "Returns id, name, customer_name, status, value, date_created, expected_close_date."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Filter by customer ID"},
                "status_id":   {"type": "integer", "description": "Filter by status ID"},
                "date_from":   {"type": "string",  "description": "Created on or after YYYY-MM-DD"},
                "date_to":     {"type": "string",  "description": "Created on or before YYYY-MM-DD"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_opportunity_by_id",
        "description": "Fetch full detail of a single opportunity by its Striven ID.",
        "input_schema": {
            "type": "object",
            "properties": {
                "opportunity_id": {"type": "integer", "description": "Striven opportunity ID"},
            },
            "required": ["opportunity_id"],
        },
    },
    # ── Operations Analysis ────────────────────────────────────────────────────
    {
        "name": "analyze_job_pipeline",
        "description": (
            "Operations pipeline analysis — WHERE JOBS BREAK. "
            "For each approved or in-progress estimate, checks: "
            "(1) does a preview task exist, and was it created within 3 days of approval? "
            "(2) does an install task exist with a scheduled date? "
            "(3) how long are the gaps between each stage? "
            "Returns: overall summary with percentages, by-rep breakdown, "
            "and up to 10 example problem jobs with key dates and issue types. "
            "Use this tool for ANY question about: "
            "'where are jobs breaking', 'pipeline delays', 'ops analysis', "
            "'which jobs have no preview', 'which jobs have no install scheduled', "
            "'show me delayed jobs', 'process breakdown', 'approval to install timeline', "
            "'how long between approval and install', 'pipeline report'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": (
                        "Number of estimates to analyse (default 20, max 50). "
                        "Higher values give a fuller picture but take longer — "
                        "each estimate requires one additional task API call."
                    ),
                },
                "status_ids": {
                    "type": "array",
                    "items": {"type": "integer"},
                    "description": (
                        "Status IDs to include. Default [22] (Approved). "
                        "Use [22, 25] to include both Approved and In Progress."
                    ),
                },
                "date_from": {
                    "type": "string",
                    "description": "Only include estimates created on or after this date (YYYY-MM-DD).",
                },
                "date_to": {
                    "type": "string",
                    "description": "Only include estimates created on or before this date (YYYY-MM-DD).",
                },
            },
            "required": [],
        },
    },
]
# NOTE: create_task and update_task are intentionally excluded.
# This is a read-only BI system. Write operations are not exposed as tools.


def _fmt(r: dict) -> dict:
    """
    Normalise a raw Striven sales-order search record into a clean dict.

    POST /v1/sales-orders/search returns TitleCase keys:
        Id, Number, Name, Customer{Id,Name}, Status{Id,Name},
        SalesRep{Id,Name}, DateCreated, DateApproved, OrderTotal
    We check TitleCase first, then fall back to camelCase for safety.

    Note: OrderTotal is not always populated on search stubs — it IS
    available on the single-record GET endpoint (get_estimate_by_id).
    """
    customer  = r.get("Customer")  or r.get("customer")  or {}
    status    = r.get("Status")    or r.get("status")    or {}
    sales_rep = r.get("SalesRep")  or r.get("salesRep")  or {}
    return {
        "id":              r.get("Id")           or r.get("id"),
        "estimate_number": r.get("Number")       or r.get("number"),
        "name":            r.get("Name")         or r.get("name"),
        "customer_name":   customer.get("Name")  or customer.get("name"),
        "customer_id":     customer.get("Id")    or customer.get("id"),
        "sales_rep":       sales_rep.get("Name") or sales_rep.get("name"),
        "status":          status.get("Name")    or status.get("name"),
        "total":           r.get("OrderTotal")   or r.get("orderTotal")  or r.get("total"),
        "date_created":    r.get("DateCreated")  or r.get("dateCreated"),
        "date_approved":   r.get("DateApproved") or r.get("dateApproved"),
    }


def _fmt_task(t: dict) -> dict:
    """
    Normalise a raw Striven task record into a clean dict.

    GET/POST /v2/tasks returns a mix of TitleCase and camelCase keys.
    Normalised to snake_case for consistent Claude consumption.
    """
    assigned  = t.get("AssignedTo")    or t.get("assignedTo")    or {}
    status    = t.get("Status")        or t.get("status")        or {}
    task_type = t.get("TaskType")      or t.get("taskType")      or {}
    related   = t.get("RelatedEntity") or t.get("relatedEntity") or {}
    return {
        "id":                t.get("Id")          or t.get("id"),
        "name":              t.get("Name")        or t.get("name"),
        "description":       t.get("Description") or t.get("description"),
        "status":            status.get("Name")   or status.get("name"),
        "status_id":         status.get("Id")     or status.get("id"),
        "task_type":         task_type.get("Name") or task_type.get("name"),
        "assigned_to":       assigned.get("Name") or assigned.get("name"),
        "due_date":          t.get("DueDate")     or t.get("dueDate"),
        "date_created":      t.get("DateCreated") or t.get("dateCreated"),
        "related_entity":    related.get("Name")  or related.get("name"),
        "related_entity_id": related.get("Id")    or related.get("id"),
    }


# ---------------------------------------------------------------------------
# Normalisers for new data domains
# All follow the same pattern: TitleCase first, camelCase fallback, snake_case out.
# ---------------------------------------------------------------------------

def _n(r: dict, *keys):
    """Return the first non-empty value from r for the given keys (case variants)."""
    for k in keys:
        v = r.get(k)
        if v is not None and v != "":
            return v
    return None

def _name_of(obj: dict) -> str | None:
    return obj.get("Name") or obj.get("name") if obj else None

def _id_of(obj: dict) -> int | None:
    return obj.get("Id") or obj.get("id") if obj else None


def _fmt_invoice(r: dict) -> dict:
    customer = r.get("Customer") or r.get("customer") or {}
    status   = r.get("Status")   or r.get("status")   or {}
    return {
        "id":             _n(r, "Id", "id"),
        "invoice_number": _n(r, "Number", "number"),
        "customer_name":  _name_of(customer),
        "customer_id":    _id_of(customer),
        "status":         _name_of(status),
        "total":          _n(r, "Total", "total", "OrderTotal", "orderTotal"),
        "amount_due":     _n(r, "AmountDue", "amountDue", "BalanceDue", "balanceDue"),
        "date_created":   _n(r, "DateCreated", "dateCreated"),
        "date_due":       _n(r, "DueDate", "dueDate"),
    }


def _fmt_bill(r: dict) -> dict:
    vendor = r.get("Vendor") or r.get("vendor") or {}
    status = r.get("Status") or r.get("status") or {}
    return {
        "id":           _n(r, "Id", "id"),
        "bill_number":  _n(r, "Number", "number"),
        "vendor_name":  _name_of(vendor),
        "vendor_id":    _id_of(vendor),
        "status":       _name_of(status),
        "total":        _n(r, "Total", "total"),
        "amount_due":   _n(r, "AmountDue", "amountDue", "BalanceDue", "balanceDue"),
        "date_created": _n(r, "DateCreated", "dateCreated"),
        "date_due":     _n(r, "DueDate", "dueDate"),
    }


def _fmt_payment(r: dict) -> dict:
    customer = r.get("Customer") or r.get("customer") or {}
    method   = r.get("PaymentMethod") or r.get("paymentMethod") or {}
    return {
        "id":            _n(r, "Id", "id"),
        "customer_name": _name_of(customer),
        "customer_id":   _id_of(customer),
        "amount":        _n(r, "Amount", "amount", "Total", "total"),
        "method":        _name_of(method) or _n(r, "PaymentMethod", "paymentMethod"),
        "reference":     _n(r, "Reference", "reference", "CheckNumber", "checkNumber"),
        "date":          _n(r, "DateCreated", "dateCreated", "PaymentDate", "paymentDate"),
    }


def _fmt_purchase_order(r: dict) -> dict:
    vendor = r.get("Vendor") or r.get("vendor") or {}
    status = r.get("Status") or r.get("status") or {}
    return {
        "id":           _n(r, "Id", "id"),
        "po_number":    _n(r, "Number", "number"),
        "vendor_name":  _name_of(vendor),
        "vendor_id":    _id_of(vendor),
        "status":       _name_of(status),
        "total":        _n(r, "Total", "total", "OrderTotal", "orderTotal"),
        "date_created": _n(r, "DateCreated", "dateCreated"),
    }


def _fmt_item(r: dict) -> dict:
    item_type = r.get("ItemType") or r.get("itemType") or {}
    category  = r.get("Category") or r.get("category") or {}
    return {
        "id":          _n(r, "Id", "id"),
        "name":        _n(r, "Name", "name"),
        "description": _n(r, "Description", "description"),
        "type":        _name_of(item_type),
        "category":    _name_of(category),
        "price":       _n(r, "Price", "price", "UnitPrice", "unitPrice"),
        "is_active":   _n(r, "IsActive", "isActive"),
    }


def _fmt_vendor(r: dict) -> dict:
    return {
        "id":           _n(r, "Id", "id"),
        "name":         _n(r, "Name", "name"),
        "number":       _n(r, "Number", "number"),
        "email":        _n(r, "Email", "email"),
        "phone":        _n(r, "Phone", "phone"),
        "contact_name": _n(r, "ContactName", "contactName"),
    }


def _fmt_contact(r: dict) -> dict:
    customer = r.get("Customer") or r.get("customer") or {}
    return {
        "id":            _n(r, "Id", "id"),
        "first_name":    _n(r, "FirstName", "firstName"),
        "last_name":     _n(r, "LastName", "lastName"),
        "name":          _n(r, "Name", "name"),
        "email":         _n(r, "Email", "email"),
        "phone":         _n(r, "Phone", "phone"),
        "customer_name": _name_of(customer),
        "customer_id":   _id_of(customer),
    }


def _fmt_opportunity(r: dict) -> dict:
    customer = r.get("Customer") or r.get("customer") or {}
    status   = r.get("Status")   or r.get("status")   or {}
    return {
        "id":                  _n(r, "Id", "id"),
        "name":                _n(r, "Name", "name"),
        "customer_name":       _name_of(customer),
        "customer_id":         _id_of(customer),
        "status":              _name_of(status),
        "value":               _n(r, "Value", "value", "Amount", "amount"),
        "date_created":        _n(r, "DateCreated", "dateCreated"),
        "expected_close_date": _n(r, "ExpectedCloseDate", "expectedCloseDate"),
    }


def _paginated_customer_search(search_name: str) -> dict:
    """
    Single source of truth for customer-name → estimates lookup.

    Step 1: POST /v1/customers/search  — resolve name to customer ID(s)
    Step 2: POST /v1/sales-orders/search with CustomerId, paginating ALL pages
            until every estimate is collected.

    Called from both:
      - _execute_tool("search_estimates_by_customer")  [Claude chat]
      - GET /search-estimates?customer=<name>           [direct API]

    Striven API key conventions (TitleCase envelope, TitleCase item fields):
      Response:  TotalCount, Data[]{Id, Number, Name, Customer{Name}, Status{Name}, DateCreated}
    """
    import time as _time
    t_start = _time.monotonic()

    # ── Step 1: resolve customer name → ID(s) ───────────────────────────────
    print(f"[customer-search] REQUEST → POST /v1/customers/search {{\"Name\": \"{search_name}\", \"PageSize\": 10}}", flush=True)
    cust_raw  = striven.search_customers(search_name, page_size=10)
    customers = cust_raw.get("data") or []
    if not customers:
        print(f"[customer-search] WARNING: 'data' missing or null — response keys={list(cust_raw.keys())}", flush=True)
    print(
        f"[customer-search] RESPONSE → totalCount={cust_raw.get('totalCount', 0)} "
        f"customers=[{', '.join('{id: ' + str(c.get('id') or c.get('Id')) + ', name: \"' + str(c.get('name') or c.get('Name')) + '\"}' for c in customers[:3])}]",
        flush=True,
    )

    if not customers:
        return {
            "estimates":   [],
            "total":       0,
            "message":     f"No customers found matching '{search_name}'. "
                           "Try a shorter or different spelling.",
            "source":      "striven_live",
        }

    # ── Step 2: paginate ALL estimates for each matched customer ─────────────
    PAGE_SIZE     = 25
    all_estimates: list[dict] = []
    grand_total   = 0

    for cust in customers[:5]:
        # Customer items use camelCase keys: id, name (not Id, Name)
        cust_id   = cust.get("id") or cust.get("Id")
        cust_name = cust.get("name") or cust.get("Name") or "(unknown)"

        if not cust_id:
            print(f"[customer-search] SKIP — no valid id in customer object: {cust}", flush=True)
            continue

        total_count      = None   # captured once from page 0, never overwritten
        page_index       = 0
        customer_records: list[dict] = []

        while True:
            request_body = {
                "PageIndex":  page_index,
                "PageSize":   PAGE_SIZE,
                "CustomerId": cust_id,
            }
            print(f"[customer-search] REQUEST → POST /v1/sales-orders/search {json.dumps(request_body)}", flush=True)

            est_raw = striven.search_sales_orders(request_body)

            data = est_raw.get("data") or []

            if total_count is None:
                total_count  = est_raw.get("totalCount", 0)
                grand_total += total_count
                if not data:
                    print(f"[customer-search] WARNING: 'data' missing or null — response keys={list(est_raw.keys())}", flush=True)
                print(f"[customer-search] '{cust_name}' (ID={cust_id}) → TotalCount={total_count}", flush=True)
                print(f"[customer-search] First record sample: {data[0] if data else 'EMPTY'}", flush=True)

            print(f"[customer-search] page={page_index} records_returned={len(data)} collected={len(customer_records) + len(data)}/{total_count}", flush=True)

            if not data:
                break

            customer_records.extend([_fmt(r) for r in data])

            if total_count and len(customer_records) >= total_count:
                break

            page_index += 1

        print(
            f"[customer-search] '{cust_name}' done — fetched {len(customer_records)}/{total_count}",
            flush=True,
        )
        all_estimates.extend(customer_records)

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[customer-search] complete — {len(all_estimates)} estimates "
        f"across {len(customers)} customer(s) in {elapsed}s",
        flush=True,
    )

    return {
        "estimates": all_estimates,
        "total":     grand_total,
        "source":    "striven_live",
    }


def _run_pipeline_analysis(
    limit: int = 20,
    status_ids: list[int] | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> dict:
    """
    Operations pipeline analysis — "Where Jobs Break".

    For each approved (or in-progress) estimate:
      1. Fetch estimate list from Striven (live, read-only)
      2. For each estimate, fetch its linked tasks via RelatedEntityId
      3. Classify tasks as 'preview' or 'install' using name/type keywords
      4. Compute timeline gaps (days: approval → preview created, approval → install due)
      5. Flag jobs with missing or late steps

    Returns a structured analysis dict with summary stats, by-rep breakdown,
    and up to 10 problem job examples ready for Claude to present.
    """
    import time as _time
    from datetime import datetime

    t_start = _time.monotonic()

    if status_ids is None:
        status_ids = [22]   # Approved by default

    limit = min(limit, 50)  # hard cap — prevents runaway API calls

    # ── Task classification keywords ──────────────────────────────────────────
    PREVIEW_KW = {
        "preview", "pre-install", "pre install", "site visit", "measure",
        "measurement", "assessment", "consult", "survey", "walkthrough",
        "walk-through", "walk through", "site check", "site review",
    }
    INSTALL_KW = {"install", "installation", "set up", "setup"}

    def classify_task(task: dict) -> str:
        """Return 'preview', 'install', or 'other' based on name and type keywords."""
        text = " ".join([
            (task.get("name")      or "").lower(),
            (task.get("task_type") or "").lower(),
        ])
        for kw in PREVIEW_KW:
            if kw in text:
                return "preview"
        for kw in INSTALL_KW:
            if kw in text:
                return "install"
        return "other"

    def parse_date(s: str | None) -> datetime | None:
        """Parse an ISO date string robustly. Returns None if unparseable."""
        if not s:
            return None
        s = s[:19]  # strip microseconds / timezone suffix
        if "T" in s:
            try:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        try:
            return datetime.strptime(s[:10], "%Y-%m-%d")
        except ValueError:
            return None

    def days_between(a: str | None, b: str | None) -> int | None:
        da, db = parse_date(a), parse_date(b)
        if da is None or db is None:
            return None
        return (db - da).days

    # ── Step 1: Fetch estimates ───────────────────────────────────────────────
    all_estimates: list[dict] = []

    for sid in status_ids:
        body: dict = {"PageIndex": 0, "PageSize": limit, "StatusChangedTo": sid}
        if date_from or date_to:
            dr: dict = {}
            if date_from: dr["DateFrom"] = date_from
            if date_to:   dr["DateTo"]   = date_to
            body["DateCreatedRange"] = dr
        print(f"[pipeline_analysis] Fetching estimates status={sid} limit={limit}", flush=True)
        raw  = striven.search_sales_orders(body)
        data = raw.get("data") or []
        all_estimates.extend([_fmt(r) for r in data])
        print(f"[pipeline_analysis] status={sid} → {len(data)} estimates returned", flush=True)

    # Deduplicate in case status lists overlap, then cap at limit
    seen: set = set()
    estimates: list[dict] = []
    for e in all_estimates:
        eid = e.get("id")
        if eid not in seen:
            seen.add(eid)
            estimates.append(e)
    estimates = estimates[:limit]
    total_analyzed = len(estimates)

    if total_analyzed == 0:
        return {
            "total_analyzed": 0,
            "summary": {"error": "No estimates found matching the given criteria."},
            "problem_jobs": [],
            "by_sales_rep": [],
        }

    # ── Step 2: Fetch tasks for each estimate ─────────────────────────────────
    job_results: list[dict] = []

    for est in estimates:
        est_id        = est.get("id")
        date_approved = est.get("date_approved")

        try:
            task_raw  = striven.search_tasks({"RelatedEntityId": est_id, "PageSize": 25})
            task_data = task_raw.get("data") or []
            tasks     = [_fmt_task(t) for t in task_data]
        except Exception as exc:
            print(f"[pipeline_analysis] WARNING: task fetch failed for est {est_id}: {exc}", flush=True)
            tasks = []

        # Classify and pick earliest of each type
        preview_tasks = [t for t in tasks if classify_task(t) == "preview"]
        install_tasks = [t for t in tasks if classify_task(t) == "install"]
        preview_task  = preview_tasks[0] if preview_tasks else None
        install_task  = install_tasks[0] if install_tasks else None

        # Compute timeline gaps
        days_to_preview = (
            days_between(date_approved, preview_task.get("date_created"))
            if preview_task else None
        )
        install_ref = (
            install_task.get("due_date") or install_task.get("date_created")
            if install_task else None
        )
        days_to_install = days_between(date_approved, install_ref) if install_ref else None

        # Flag issues
        issues: list[str] = []
        if not preview_task:
            issues.append("no_preview_task")
        elif days_to_preview is not None and days_to_preview > 3:
            issues.append(f"preview_late_{days_to_preview}d")
        if not install_task:
            issues.append("no_install_task")

        job_results.append({
            "estimate_id":     est_id,
            "estimate_number": est.get("estimate_number"),
            "customer_name":   est.get("customer_name"),
            "sales_rep":       est.get("sales_rep") or "Unassigned",
            "status":          est.get("status"),
            "total":           est.get("total"),
            "date_approved":   date_approved,
            "preview_task":    preview_task,
            "install_task":    install_task,
            "days_to_preview": days_to_preview,
            "days_to_install": days_to_install,
            "all_task_count":  len(tasks),
            "issues":          issues,
            "has_issues":      bool(issues),
        })

    # ── Step 3: Summary stats ─────────────────────────────────────────────────
    no_preview   = [j for j in job_results if "no_preview_task"  in j["issues"]]
    preview_late = [j for j in job_results if any("preview_late" in i for i in j["issues"])]
    no_install   = [j for j in job_results if "no_install_task"  in j["issues"]]
    has_issues   = [j for j in job_results if j["has_issues"]]

    dtp = [j["days_to_preview"] for j in job_results if j["days_to_preview"] is not None]
    dti = [j["days_to_install"] for j in job_results if j["days_to_install"] is not None]
    avg_days_to_preview = round(sum(dtp) / len(dtp), 1) if dtp else None
    avg_days_to_install = round(sum(dti) / len(dti), 1) if dti else None

    # ── Step 4: By-rep breakdown ──────────────────────────────────────────────
    rep_stats: dict[str, dict] = {}
    for j in job_results:
        rep = j.get("sales_rep") or "Unassigned"
        if rep not in rep_stats:
            rep_stats[rep] = {
                "rep": rep, "count": 0,
                "no_preview": 0, "no_install": 0, "has_issues": 0,
            }
        rep_stats[rep]["count"] += 1
        if "no_preview_task" in j["issues"]: rep_stats[rep]["no_preview"] += 1
        if "no_install_task" in j["issues"]: rep_stats[rep]["no_install"] += 1
        if j["has_issues"]:                  rep_stats[rep]["has_issues"] += 1

    by_rep = sorted(rep_stats.values(), key=lambda x: x["has_issues"], reverse=True)

    # ── Step 5: Problem examples — most issues first, then oldest approval ────
    problem_jobs = sorted(
        has_issues,
        key=lambda j: (-len(j["issues"]), j.get("date_approved") or ""),
    )[:10]

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[pipeline_analysis] Complete — {total_analyzed} estimates analysed, "
        f"{len(has_issues)} with issues, {elapsed}s elapsed",
        flush=True,
    )

    return {
        "total_analyzed":    total_analyzed,
        "total_with_issues": len(has_issues),
        "summary": {
            "no_preview_task":     len(no_preview),
            "pct_no_preview":      round(len(no_preview)   / total_analyzed * 100) if total_analyzed else 0,
            "preview_task_late":   len(preview_late),
            "pct_preview_late":    round(len(preview_late) / total_analyzed * 100) if total_analyzed else 0,
            "no_install_task":     len(no_install),
            "pct_no_install":      round(len(no_install)   / total_analyzed * 100) if total_analyzed else 0,
            "avg_days_to_preview": avg_days_to_preview,
            "avg_days_to_install": avg_days_to_install,
        },
        "by_sales_rep": by_rep,
        "problem_jobs": problem_jobs,
        "note": (
            f"Analysed {total_analyzed} estimates in {elapsed}s. "
            "Task classification uses name/type keyword matching. "
            "Preview keywords: preview, site visit, measure, consult, survey. "
            "Install keywords: install, installation."
        ),
    }


def _execute_tool(name: str, tool_input: dict) -> dict:
    """Map a Claude tool call to the live Striven API. All operations are read-only."""
    try:
        # ── count_estimates ──────────────────────────────────────────────────
        # POST /v1/sales-orders/search with pageSize=1.
        # We only need totalCount — no records are read.
        # NO fallback, NO Supabase, NO cache. Live Striven only.
        if name == "count_estimates":
            raw   = striven.search_sales_orders({"PageIndex": 0, "PageSize": 1})
            total = raw.get("totalCount", 0)
            if not total:
                print(f"[count_estimates] WARNING: 'TotalCount' missing — keys={list(raw.keys())}", flush=True)
            print(f"[count_estimates] TotalCount={total}", flush=True)
            return {
                "total":  total,
                "source": "striven_live",
                "note":   "Live count from Striven /v1/sales-orders/search → totalCount field",
            }

        # ── high_value_estimates ─────────────────────────────────────────────
        # Fetch 100 recent records, filter client-side for total > $10,000,
        # sort highest-first, return top 25.
        if name == "high_value_estimates":
            raw     = striven.search_sales_orders({"PageIndex": 0, "PageSize": 100})
            data    = raw.get("data") or []
            total   = raw.get("totalCount", 0)
            if not data:
                print(f"[high_value_estimates] WARNING: 'Data' key missing or null — keys={list(raw.keys())}", flush=True)
            print(f"[high_value_estimates] TotalCount={total} fetched={len(data)}", flush=True)
            print(f"[high_value_estimates] First record sample: {data[0] if data else 'EMPTY'}", flush=True)
            high = sorted(
                [_fmt(r) for r in data if (r.get("total") or r.get("OrderTotal") or 0) >= 10000],
                key=lambda x: x["total"] or 0,
                reverse=True,
            )[:25]
            return {"count": len(high), "records": high, "source": "striven_live"}

        # ── search_estimates_by_customer ─────────────────────────────────────
        # Delegates entirely to _paginated_customer_search — single source of truth.
        if name == "search_estimates_by_customer":
            return _paginated_customer_search(tool_input.get("name", "").strip())

        # ── search_estimates ─────────────────────────────────────────────────
        if name == "search_estimates":
            page_size       = min(tool_input.get("page_size", 25), 50)  # hard cap at 50
            active_only     = tool_input.get("active_only", False)
            explicit_status = tool_input.get("status")

            # Shared filter fragments (PageIndex always 0 — fast mode default)
            base_body: dict = {"PageIndex": 0, "PageSize": page_size}
            if "keyword"   in tool_input: base_body["Name"] = tool_input["keyword"]
            if "date_from" in tool_input or "date_to" in tool_input:
                date_range: dict = {}
                if "date_from" in tool_input: date_range["DateFrom"] = tool_input["date_from"]
                if "date_to"   in tool_input: date_range["DateTo"]   = tool_input["date_to"]
                base_body["DateCreatedRange"] = date_range

            if active_only and explicit_status is None:
                # ── DEEP MODE: fan out across all active statuses ─────────────
                # 4 serial API calls — only used when user explicitly requests
                # a full/comprehensive view (active_only=true signals deep mode).
                print(f"[search_estimates] DEEP MODE — active_only, page_size={page_size}", flush=True)
                ACTIVE = (19, 20, 22, 25)
                all_records: list = []
                grand_total = 0
                for sid in ACTIVE:
                    if len(all_records) >= page_size:
                        break
                    body = {**base_body, "StatusChangedTo": sid,
                            "PageSize": page_size - len(all_records)}
                    raw  = striven.search_sales_orders(body)
                    data = raw.get("data") or []
                    grand_total += raw.get("totalCount", 0)
                    all_records.extend([_fmt(r) for r in data])
                    print(f"[search_estimates] deep status={sid} → {len(data)} records", flush=True)
                records = all_records[:page_size]
                print(f"[search_estimates] deep total_pool={grand_total} returned={len(records)}", flush=True)
                return {"total": grand_total, "count": len(records), "estimates": records,
                        "note": "Deep mode — active statuses 19,20,22,25"}
            else:
                # ── FAST MODE: single API call, one page, most recent first ───
                body = {**base_body}
                if explicit_status is not None:
                    body["StatusChangedTo"] = explicit_status
                print(
                    f"[search_estimates] FAST MODE — "
                    f"status={explicit_status or 'any'} page_size={page_size}",
                    flush=True,
                )
                raw     = striven.search_sales_orders(body)
                data    = raw.get("data") or []
                total   = raw.get("totalCount", 0)
                if not data:
                    print(f"[search_estimates] WARNING: empty response — keys={list(raw.keys())}", flush=True)
                print(f"[search_estimates] TotalCount={total} returned={len(data)}", flush=True)
                records = [_fmt(r) for r in data]
                return {"total": total, "count": len(records), "estimates": records,
                        "note": "Fast mode — 1 page, most recent first"}

        if name == "get_estimate_by_id":
            raw    = striven.get_estimate(tool_input["estimate_id"])
            # Normalise line items into a clean summary for Claude
            raw_items = (
                raw.get("lineItems") or raw.get("items")
                or raw.get("LineItems") or []
            )
            line_items = []
            for li in raw_items:
                item_obj = li.get("item") or li.get("Item") or {}
                line_items.append({
                    "name":        item_obj.get("name") or item_obj.get("Name") or li.get("name") or li.get("Name"),
                    "description": li.get("description") or li.get("Description"),
                    "quantity":    li.get("quantity")    or li.get("Quantity"),
                    "unit_price":  li.get("unitPrice")   or li.get("UnitPrice"),
                    "total":       li.get("total")       or li.get("Total"),
                })
            customer  = raw.get("customer")  or raw.get("Customer")  or {}
            status    = raw.get("status")    or raw.get("Status")    or {}
            sales_rep = raw.get("salesRep")  or raw.get("SalesRep")  or {}
            return {
                "id":              raw.get("id")          or raw.get("Id"),
                "estimate_number": raw.get("number")      or raw.get("Number"),
                "name":            raw.get("name")        or raw.get("Name"),
                "customer_name":   customer.get("name")   or customer.get("Name"),
                "sales_rep":       sales_rep.get("name")  or sales_rep.get("Name"),
                "status":          status.get("name")     or status.get("Name"),
                "total":           raw.get("orderTotal")  or raw.get("OrderTotal"),
                "date_created":    raw.get("dateCreated") or raw.get("DateCreated"),
                "date_approved":   raw.get("dateApproved") or raw.get("DateApproved"),
                "notes":           raw.get("notes")       or raw.get("Notes"),
                "line_items":      line_items,
                "line_item_count": len(line_items),
            }

        if name == "gas_log_audit":
            print("[TOOL] gas_log_audit called — running _run_gas_log_audit()", flush=True)
            result = _run_gas_log_audit()
            print(
                f"[TOOL] gas_log_audit complete — "
                f"total_checked={result.get('total_checked')} "
                f"gas_log_installs={result.get('gas_log_installs')} "
                f"missing_removal_fee={result.get('missing_removal_fee')}",
                flush=True,
            )
            return result

        if name == "portal_flag_audit":
            print("[TOOL] portal_flag_audit called — running _run_portal_flag_audit()", flush=True)
            result = _run_portal_flag_audit()
            print(
                f"[TOOL] portal_flag_audit complete — "
                f"checked={result['summary']['total_estimates_checked']} "
                f"missing={result['summary']['total_missing_flag']}",
                flush=True,
            )
            return result

        # ── search_customers ─────────────────────────────────────────────────
        if name == "search_customers":
            query     = tool_input.get("name", "").strip()
            page_size = min(tool_input.get("page_size", 25), 50)
            print(f"[search_customers] query='{query}' page_size={page_size}", flush=True)
            raw   = striven.search_customers(name=query, page_size=page_size)
            data  = raw.get("data") or raw.get("Data") or []
            total = raw.get("totalCount") or raw.get("TotalCount") or 0
            customers = [
                {
                    "id":     c.get("id")     or c.get("Id"),
                    "name":   c.get("name")   or c.get("Name"),
                    "number": c.get("number") or c.get("Number"),
                    "email":  c.get("email")  or c.get("Email"),
                    "phone":  c.get("phone")  or c.get("Phone"),
                }
                for c in data
            ]
            print(f"[search_customers] TotalCount={total} returned={len(customers)}", flush=True)
            return {"total": total, "count": len(customers), "customers": customers}

        # ── search_tasks ─────────────────────────────────────────────────────
        if name == "search_tasks":
            page_size = min(tool_input.get("page_size", 25), 50)
            body: dict = {"PageIndex": 0, "PageSize": page_size}
            if "status_id"    in tool_input: body["StatusId"]        = tool_input["status_id"]
            if "assigned_to"  in tool_input: body["AssignedToId"]    = tool_input["assigned_to"]
            if "task_type_id" in tool_input: body["TaskTypeId"]      = tool_input["task_type_id"]
            if "due_from"  in tool_input or "due_to" in tool_input:
                due_range: dict = {}
                if "due_from" in tool_input: due_range["DateFrom"] = tool_input["due_from"]
                if "due_to"   in tool_input: due_range["DateTo"]   = tool_input["due_to"]
                body["DueDateRange"] = due_range
            if "related_entity_id" in tool_input:
                body["RelatedEntityId"] = tool_input["related_entity_id"]
            print(f"[search_tasks] body={body}", flush=True)
            raw   = striven.search_tasks(body)
            data  = raw.get("data") or raw.get("Data") or []
            total = raw.get("totalCount") or raw.get("TotalCount") or 0
            tasks = [_fmt_task(t) for t in data]
            print(f"[search_tasks] TotalCount={total} returned={len(tasks)}", flush=True)
            return {"total": total, "count": len(tasks), "tasks": tasks}

        # ── get_task_by_id ───────────────────────────────────────────────────
        if name == "get_task_by_id":
            task_id = tool_input["task_id"]
            print(f"[get_task_by_id] id={task_id}", flush=True)
            raw = striven.get_task(task_id)
            return _fmt_task(raw)

        # ── create_task / update_task ── NOT EXPOSED (read-only system) ────────
        if name in ("create_task", "update_task"):
            return {"error": "This system is read-only and cannot make changes."}

        # ── search_invoices ───────────────────────────────────────────────────
        if name == "search_invoices":
            page_size = min(tool_input.get("page_size", 25), 50)
            body: dict = {"PageIndex": 0, "PageSize": page_size}
            if "customer_id" in tool_input: body["CustomerId"] = tool_input["customer_id"]
            if "status_id"   in tool_input: body["StatusId"]   = tool_input["status_id"]
            if "date_from" in tool_input or "date_to" in tool_input:
                body["DateCreatedRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("date_from"),
                    "DateTo":   tool_input.get("date_to"),
                }.items() if v}
            if "due_from" in tool_input or "due_to" in tool_input:
                body["DueDateRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("due_from"),
                    "DateTo":   tool_input.get("due_to"),
                }.items() if v}
            raw     = striven.search_invoices(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_invoice(r) for r in data]
            print(f"[search_invoices] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "invoices": records}

        # ── get_invoice_by_id ─────────────────────────────────────────────────
        if name == "get_invoice_by_id":
            raw = striven.get_invoice(tool_input["invoice_id"])
            return _fmt_invoice(raw)

        # ── search_bills ──────────────────────────────────────────────────────
        if name == "search_bills":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "vendor_id" in tool_input: body["VendorId"]  = tool_input["vendor_id"]
            if "status_id" in tool_input: body["StatusId"]  = tool_input["status_id"]
            if "date_from" in tool_input or "date_to" in tool_input:
                body["DateCreatedRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("date_from"),
                    "DateTo":   tool_input.get("date_to"),
                }.items() if v}
            raw     = striven.search_bills(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_bill(r) for r in data]
            print(f"[search_bills] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "bills": records}

        # ── search_payments ───────────────────────────────────────────────────
        if name == "search_payments":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "customer_id" in tool_input: body["CustomerId"] = tool_input["customer_id"]
            if "date_from" in tool_input or "date_to" in tool_input:
                body["DateCreatedRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("date_from"),
                    "DateTo":   tool_input.get("date_to"),
                }.items() if v}
            raw     = striven.search_payments(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_payment(r) for r in data]
            print(f"[search_payments] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "payments": records}

        # ── search_purchase_orders ────────────────────────────────────────────
        if name == "search_purchase_orders":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "vendor_id" in tool_input: body["VendorId"]  = tool_input["vendor_id"]
            if "status_id" in tool_input: body["StatusId"]  = tool_input["status_id"]
            if "date_from" in tool_input or "date_to" in tool_input:
                body["DateCreatedRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("date_from"),
                    "DateTo":   tool_input.get("date_to"),
                }.items() if v}
            raw     = striven.search_purchase_orders(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_purchase_order(r) for r in data]
            print(f"[search_purchase_orders] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "purchase_orders": records}

        # ── search_items ──────────────────────────────────────────────────────
        if name == "search_items":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "keyword" in tool_input: body["Name"] = tool_input["keyword"]
            raw     = striven.search_items(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_item(r) for r in data]
            print(f"[search_items] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "items": records}

        # ── search_vendors ────────────────────────────────────────────────────
        if name == "search_vendors":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "name" in tool_input: body["Name"] = tool_input["name"]
            raw     = striven.search_vendors(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_vendor(r) for r in data]
            print(f"[search_vendors] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "vendors": records}

        # ── search_contacts ───────────────────────────────────────────────────
        if name == "search_contacts":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "name"        in tool_input: body["Name"]       = tool_input["name"]
            if "customer_id" in tool_input: body["CustomerId"] = tool_input["customer_id"]
            raw     = striven.search_contacts(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_contact(r) for r in data]
            print(f"[search_contacts] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "contacts": records}

        # ── search_opportunities ──────────────────────────────────────────────
        if name == "search_opportunities":
            page_size = min(tool_input.get("page_size", 25), 50)
            body = {"PageIndex": 0, "PageSize": page_size}
            if "customer_id" in tool_input: body["CustomerId"] = tool_input["customer_id"]
            if "status_id"   in tool_input: body["StatusId"]   = tool_input["status_id"]
            if "date_from" in tool_input or "date_to" in tool_input:
                body["DateCreatedRange"] = {k: v for k, v in {
                    "DateFrom": tool_input.get("date_from"),
                    "DateTo":   tool_input.get("date_to"),
                }.items() if v}
            raw     = striven.search_opportunities(body)
            data    = raw.get("data") or raw.get("Data") or []
            total   = raw.get("totalCount") or raw.get("TotalCount") or 0
            records = [_fmt_opportunity(r) for r in data]
            print(f"[search_opportunities] total={total} returned={len(records)}", flush=True)
            return {"total": total, "count": len(records), "opportunities": records}

        # ── get_opportunity_by_id ─────────────────────────────────────────────
        if name == "get_opportunity_by_id":
            raw = striven.get_opportunity(tool_input["opportunity_id"])
            return _fmt_opportunity(raw)

        # ── analyze_job_pipeline ──────────────────────────────────────────────
        # Operations analysis — "Where Jobs Break".
        # Fetches approved/in-progress estimates, looks up their tasks,
        # classifies preview vs install tasks, measures timeline gaps,
        # and returns a structured breakdown of where the process fails.
        if name == "analyze_job_pipeline":
            limit      = min(tool_input.get("limit", 20), 50)
            status_ids = tool_input.get("status_ids", [22])
            date_from  = tool_input.get("date_from")
            date_to    = tool_input.get("date_to")
            print(
                f"[analyze_job_pipeline] limit={limit} "
                f"status_ids={status_ids} "
                f"date_from={date_from} date_to={date_to}",
                flush=True,
            )
            return _run_pipeline_analysis(
                limit=limit,
                status_ids=status_ids,
                date_from=date_from,
                date_to=date_to,
            )

        return {"error": f"Unknown tool: {name}"}

    except Exception as exc:
        return {"error": str(exc)}


@app.get("/")
def chat_ui():
    """Serve the WilliamSmith chat interface."""
    return render_template("index.html")


@app.get("/logs")
def view_logs():
    """Admin view — shows the last 100 WilliamSmith search queries."""
    try:
        rows = get_chat_logs(limit=100)
    except Exception as exc:
        rows = []
        print(f"[logs] Failed to fetch chat logs: {exc}", flush=True)
    return render_template("logs.html", logs=rows)


@app.post("/api/chat")
def chat_api():
    """
    Agentic chat endpoint.
    Accepts: { messages: [{role, content}, ...] }
    Returns: { response: "<markdown string>" }
    """
    data     = request.get_json(force=True)
    messages = data.get("messages", [])

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return jsonify({"error": "ANTHROPIC_API_KEY not configured on server."}), 500

    client = anthropic.Anthropic(api_key=api_key)

    # Capture the user's question for logging (last user message in the chain)
    user_question = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content", "")
            user_question = content if isinstance(content, str) else str(content)
            break

    tools_used: list[str] = []

    # Agentic loop — keep going until Claude stops calling tools
    while True:
        response = client.messages.create(
            model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5"),
            max_tokens=4096,
            system=_SYSTEM_PROMPT,
            tools=_CHAT_TOOLS,
            messages=messages,
        )

        if response.stop_reason == "tool_use":
            # Execute every tool Claude asked for
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    tools_used.append(block.name)
                    result = _execute_tool(block.name, block.input)
                    tool_results.append({
                        "type":        "tool_result",
                        "tool_use_id": block.id,
                        "content":     json.dumps(result),
                    })

            # Add assistant turn + tool results and loop
            messages = messages + [
                {"role": "assistant", "content": response.content},
                {"role": "user",      "content": tool_results},
            ]

        else:
            # Final text response
            text = next(
                (block.text for block in response.content if hasattr(block, "text")),
                "No response generated.",
            )

            # Log the completed turn to Supabase (never let logging crash the chat)
            try:
                log_chat(user_question, tools_used, text)
            except Exception as log_exc:
                print(f"[log_chat] WARNING: failed to log — {log_exc}", flush=True)

            return jsonify({"response": text})


# ---------------------------------------------------------------------------
# Rule-based /chat endpoint
# ---------------------------------------------------------------------------

def _detect_intent(message: str) -> tuple[str, dict]:
    """
    Keyword-based intent detection. Returns (intent_name, params_dict).

    Intents:
        gas_log_audit      — "gas log", "removal fee", "removal"
        high_value         — "biggest", "high value", "top job", "highest"
        count              — "how many", "count", "total estimate"
        search_by_customer — "customer", "estimates for <name>", "for <name>"
        unknown            — no keywords matched
    """
    msg = message.lower().strip()

    if any(kw in msg for kw in ("gas log", "removal fee", "removal")):
        return "gas_log_audit", {}

    if any(kw in msg for kw in ("biggest", "high value", "top job", "highest value", "largest")):
        return "high_value", {}

    if any(kw in msg for kw in ("how many", "count", "total estimate", "total number")):
        return "count", {}

    # Customer search — try to extract name after common marker phrases
    if "customer" in msg or " for " in msg or "estimates for" in msg:
        name = ""
        for marker in ("estimates for ", "for customer ", "customer named ", "customer ", " for "):
            if marker in msg:
                name = msg.split(marker, 1)[1].strip().rstrip("?.,!")
                break
        return "search_by_customer", {"name": name}

    return "unknown", {}


# ── Response formatters ───────────────────────────────────────────────────────

REMOVAL_FEE_VALUE = 200   # $ per missing removal fee — used for revenue estimate


def _format_gas_log_audit(result: dict) -> str:
    """
    Format the gas-log audit result as a concise data report.
    Reports only what the system actually found — no explanations.
    """
    total    = result.get("total_checked", 0)
    installs = result.get("gas_log_installs", 0)
    missing  = result.get("missing_removal_fee", 0)
    matches  = result.get("matches") or []

    revenue_impact = missing * REMOVAL_FEE_VALUE

    lines = [
        f"Gas Log Removal Fee Audit — 2025 Estimates",
        f"",
        f"  Estimates checked:       {total:,}",
        f"  Gas log installs found:  {installs:,}",
        f"  Missing removal fee:     {missing:,}",
        f"  Estimated missed revenue: ~${revenue_impact:,}",
        f"",
    ]

    if matches:
        lines.append(f"Estimates missing the Gas Log Removal Fee ({len(matches):,} total):")
        for m in matches[:15]:
            num  = m.get("estimate_number") or "—"
            name = m.get("customer_name")   or "Unknown"
            url  = m.get("url", "")
            lines.append(f"  • #{num} — {name}")
            if url:
                lines.append(f"      {url}")
        if len(matches) > 15:
            lines.append(f"  … and {len(matches) - 15:,} more.")
    else:
        lines.append(f"Gas log installs detected: {installs:,}")
        lines.append(f"Removal fees missing:      {missing:,}")
        lines.append("")
        lines.append("Note: If gas log installs were expected but not detected,")
        lines.append("check Render logs for 'Line item sample keys' to confirm")
        lines.append("the exact field names Striven returned.")

    return "\n".join(lines)


def _format_high_value(result: dict) -> str:
    records = result.get("records") or []
    count   = result.get("count", len(records))
    lines   = [f"Top {count} estimates over $10,000:\n"]
    for i, r in enumerate(records[:20], 1):
        num   = r.get("estimate_number") or "—"
        name  = r.get("customer_name")   or "Unknown"
        total = r.get("total") or 0
        lines.append(f"  {i:>2}. #{num} — {name}  (${total:,.0f})")
    return "\n".join(lines)


def _format_customer_search(result: dict, name: str) -> str:
    estimates = result.get("estimates") or []
    total     = result.get("total", len(estimates))
    if not estimates:
        return f"No estimates found for customer matching '{name}'."
    lines = [f"{total:,} estimate(s) found for '{name}':\n"]
    for r in estimates[:20]:
        num    = r.get("estimate_number") or "—"
        status = r.get("status")          or "—"
        date   = (r.get("date") or "")[:10]
        lines.append(f"  • #{num}  {status}  {date}")
    if total > 20:
        lines.append(f"  … and {total - 20:,} more.")
    return "\n".join(lines)


def _format_count(result: dict) -> str:
    total = result.get("total", 0)
    return f"There are {total:,} estimates in the system."


# ── Route ─────────────────────────────────────────────────────────────────────

@app.post("/chat")
def simple_chat():
    """
    Rule-based natural language chat endpoint.

    Input:  { "message": "Show me missing gas log removal fees" }
    Output: { "response": "<formatted text>", "intent": "<detected intent>" }

    Intent routing (keyword matching — no AI required):
        gas_log_audit      → _run_gas_log_audit()
        high_value         → _execute_tool("high_value_estimates", {})
        count              → _execute_tool("count_estimates", {})
        search_by_customer → _paginated_customer_search(name)
    """
    body    = request.get_json(force=True) or {}
    message = (body.get("message") or "").strip()

    if not message:
        return jsonify({"error": "Field 'message' is required."}), 400

    print(f"[CHAT] Incoming message: {message!r}", flush=True)

    # ── TEMPORARY HARD OVERRIDE — force gas_log_audit for any "gas log" message ──
    # Remove once we confirm the correct execution path in Render logs.
    if "gas log" in message.lower():
        print("[CHAT] FORCED gas_log_audit path", flush=True)
        print("[CHAT] Running _run_gas_log_audit()...", flush=True)
        try:
            result = _run_gas_log_audit()
            print(f"[CHAT] Result keys: {list(result.keys())}", flush=True)
            print(f"[CHAT] Result: total_checked={result.get('total_checked')} "
                  f"gas_log_installs={result.get('gas_log_installs')} "
                  f"missing_removal_fee={result.get('missing_removal_fee')}", flush=True)
            return jsonify({
                "intent":   "gas_log_audit_forced",
                "response": _format_gas_log_audit(result),
                "raw":      result,
            })
        except Exception as exc:
            print(f"[CHAT] ERROR in forced gas_log_audit: {exc}", flush=True)
            return jsonify({"intent": "gas_log_audit_forced", "error": str(exc)}), 500
    # ── END TEMPORARY OVERRIDE ──────────────────────────────────────────────────

    intent, params = _detect_intent(message)
    print(f"[CHAT] Detected intent: {intent!r}  params={params}", flush=True)

    try:
        if intent == "gas_log_audit":
            print("[CHAT] Running gas_log_audit()", flush=True)
            result   = _run_gas_log_audit()
            print(f"[CHAT] Result: {result}", flush=True)
            response = _format_gas_log_audit(result)

        elif intent == "high_value":
            result   = _execute_tool("high_value_estimates", {})
            response = _format_high_value(result)

        elif intent == "count":
            result   = _execute_tool("count_estimates", {})
            response = _format_count(result)

        elif intent == "search_by_customer":
            name = params.get("name", "").strip()
            if not name:
                return jsonify({
                    "response": (
                        "I can search estimates by customer name.\n"
                        "Please include the name — for example:\n"
                        "  'Show estimates for Acme Corp'"
                    ),
                    "intent": intent,
                })
            result   = _paginated_customer_search(name)
            response = _format_customer_search(result, name)

        else:
            response = (
                "I'm not sure what you're looking for. Try asking:\n"
                "  • 'Show me missing gas log removal fees'\n"
                "  • 'Show biggest jobs'\n"
                "  • 'How many estimates do we have?'\n"
                "  • 'Show estimates for Acme Corp'"
            )

        return jsonify({"response": response, "intent": intent})

    except Exception as exc:
        print(f"[/chat] ERROR: {exc}", flush=True)
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Fail fast if credentials are missing
    for var in ("CLIENT_ID", "CLIENT_SECRET"):
        if not os.environ.get(var):
            raise RuntimeError(f"Environment variable {var!r} is not set.")

    app.run(host="0.0.0.0", port=5000, debug=True)
