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
import time as _time_mod
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
    get_gas_log_audit as _sb_get_gas_log_audit,
    upsert_gas_log_audit as _sb_upsert_gas_log_audit,
    query_gas_log_missing,
    query_unassigned_reps,
    query_no_line_items,
    query_jobs_by_location,
    query_jobs_past_install_date,
    query_sales_rep_backlog,
    query_time_to_target,
    _get_client as _sb_client,
)
from services import knowledge as _knowledge

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

# Load knowledge base into memory (runs once at startup)
_knowledge.load_all()
print("Knowledge base loaded.", flush=True)

# ---------------------------------------------------------------------------
# Anthropic client singleton — created once, reused across all requests.
# Creating a new client per request was safe but wasted ~5ms and a TLS
# handshake on every chat call.
# ---------------------------------------------------------------------------
_anthropic_client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY", ""))

# ---------------------------------------------------------------------------
# Simple in-memory response cache
# Keyed on a short string (tool name + stable arguments).
# TTL = 60 seconds. Only used for cheap, stable queries.
#
# This is intentionally minimal — a dict, not Redis.
# The goal is to avoid hitting Striven 3 times in 10 seconds for the
# same count query when a user asks the same question twice.
# ---------------------------------------------------------------------------
# Server-side request throttle — prevents Claude API rate-limit bursts
# ---------------------------------------------------------------------------
import threading as _threading

_REQUEST_GATE_LOCK  = _threading.Lock()
_LAST_CLAUDE_TS: float = 0.0
_MIN_CLAUDE_INTERVAL   = 3.0  # seconds — max ~20 Claude calls/min


def _check_and_claim_request() -> bool:
    """
    Atomically checks whether enough time has passed since the last Claude call.
    If yes, records the current timestamp and returns True (caller may proceed).
    If no, returns False (caller must reject with 429).
    Thread-safe via lock.
    """
    global _LAST_CLAUDE_TS
    now = _time_mod.time()
    with _REQUEST_GATE_LOCK:
        if now - _LAST_CLAUDE_TS < _MIN_CLAUDE_INTERVAL:
            return False
        _LAST_CLAUDE_TS = now
        return True


# ---------------------------------------------------------------------------
_RESP_CACHE: dict[str, tuple[object, float]] = {}
_CACHE_TTL   = 60  # seconds


def _cache_get(key: str) -> object | None:
    entry = _RESP_CACHE.get(key)
    if entry is None:
        return None
    data, ts = entry
    if _time_mod.monotonic() - ts < _CACHE_TTL:
        print(f"[cache] HIT  key={key!r}", flush=True)
        return data
    del _RESP_CACHE[key]
    return None


def _cache_set(key: str, data: object) -> None:
    _RESP_CACHE[key] = (data, _time_mod.monotonic())
    print(f"[cache] SET  key={key!r}", flush=True)


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    """Simple liveness probe — returns 200 if the server is running."""
    return jsonify({"status": "ok", "service": "striven-api"})


# ---------------------------------------------------------------------------
# Estimates
# ---------------------------------------------------------------------------

@app.route("/get-estimate/<int:estimate_id>", methods=["GET"])
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


@app.route("/search-estimates", methods=["GET"])
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

@app.route("/report-preview", methods=["GET"])
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


@app.route("/missing-portal-flag", methods=["GET"])
@app.route("/portal-flag-audit", methods=["GET"])
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

    # TEMP filtered + limited scan to avoid Render timeout; replace with full background job later
    #
    # Only audit active estimates — Quoted(19), Pending Approval(20),
    # Approved(22), In Progress(25).
    # Excluded intentionally: Incomplete(18), Completed(27), Cancelled, Lost.
    # These statuses are never queried so they never enter the count.
    ACTIVE_STATUSES = (19, 20, 22, 25)

    # Cap total estimates inspected per production run.
    # Explicit limit= arg (test runs) takes precedence; otherwise DEFAULT_SCAN_LIMIT applies.
    DEFAULT_SCAN_LIMIT = 400
    effective_limit    = limit if limit is not None else DEFAULT_SCAN_LIMIT

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
                if total_inspected >= effective_limit:
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

            # Honour scan limit (test runs use explicit limit; production uses DEFAULT_SCAN_LIMIT)
            if total_inspected >= effective_limit:
                print(f"[gas-log-audit] Limit {effective_limit} reached — stopping.", flush=True)
                break

            # All pages for this status exhausted
            if status_total is not None and (page_index + 1) * PAGE_SIZE >= status_total:
                print(
                    f"[gas-log-audit] Status {status_id} exhausted.",
                    flush=True,
                )
                break

            page_index += 1

        if total_inspected >= effective_limit:
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

        # ── Supabase persistence (full-scan calls only) ──────────────────────
        # Write the summary so the next server cold-start can serve stale data
        # instantly rather than forcing another 60-second full scan.
        try:
            pct = (
                round(len(matches) / gas_log_installs * 100, 1)
                if gas_log_installs
                else 0.0
            )
            _sb_upsert_gas_log_audit(
                total_checked=total_inspected,
                missing_count=len(matches),
                percent_missing=pct,
            )
            print(
                f"[gas-log-audit] Supabase upsert OK — "
                f"total_checked={total_inspected} missing={len(matches)} pct={pct}%",
                flush=True,
            )
        except Exception as _sb_err:
            # Supabase write failure must never crash the audit itself.
            print(f"[gas-log-audit] Supabase upsert FAILED (non-fatal): {_sb_err}", flush=True)

    return result


@app.route("/gas-log-audit", methods=["GET"])
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

@app.route("/gas-log-audit-export", methods=["GET"])
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


@app.route("/gas-log-audit-pdf", methods=["GET"])
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

@app.route("/sync-estimates", methods=["GET"])
def sync_estimates():
    """
    Trigger an estimate sync from Striven into Supabase.

    Pulls records from Striven (READ-ONLY), transforms them, and upserts
    into the `estimates` table in Supabase. Safe to call repeatedly —
    upsert logic means no duplicates are created.

    Returns 202 immediately and runs the sync in a background thread so
    Render's proxy timeout never kills a long-running full sync.
    Progress and errors are written to stdout (Render logs).

    Query params (optional):
        limit — max records to sync this run (default: all records).
                Pass limit=all or omit for a full sync.
                Pass limit=200 etc. for a partial sync.

    Example:
        GET /sync-estimates           → full sync (all records, background)
        GET /sync-estimates?limit=200 → sync first 200 records, background
        GET /sync-estimates?limit=all → full sync (all records, background)

    SAFETY: This endpoint never writes to Striven.
    """
    import threading

    # Parse optional limit param
    raw_limit = request.args.get("limit", "all")

    if raw_limit.lower() == "all":
        limit = None           # sync everything
    else:
        try:
            limit = int(raw_limit)
        except ValueError:
            return jsonify({"error": f"Invalid limit value: {raw_limit!r}. Use an integer or 'all'."}), 400

    def _run():
        try:
            n = sync_estimates_to_supabase(limit=limit)
            print(f"[sync-estimates] Background sync complete — {n} records.", flush=True)
        except Exception as exc:
            print(f"[sync-estimates] Background sync ERROR: {exc}", flush=True)

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    return jsonify({
        "status":  "started",
        "message": f"Sync running in background. limit={'all' if limit is None else limit}. Check Render logs for progress.",
    }), 202


# ---------------------------------------------------------------------------
# Supabase query endpoints — read-only, Claude-facing
# ---------------------------------------------------------------------------

@app.route("/estimates/count", methods=["GET"])
def estimates_count():
    """Return the total number of estimates stored in Supabase."""
    try:
        return jsonify({"total": count_estimates()})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/estimates/high-value", methods=["GET"])
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


@app.route("/estimates/by-customer", methods=["GET"])
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
# Fast direct-query endpoints — no LLM, deterministic, sub-second
#
# These routes bypass Claude entirely and execute pre-built Supabase queries
# for known, high-value business questions.  They MUST:
#   • Return in < 1 second (pure Supabase reads, no Striven calls)
#   • Return JSON with {"count": N, "records": [...]}
#   • Never call the Anthropic API or block on any external service
# ---------------------------------------------------------------------------

def _run_query(records: list[dict]) -> "Response":
    """
    Standard JSON wrapper for all direct-query results.

    Converts a list of record dicts (returned by supabase_client query_*
    functions) into a consistent {"count": N, "records": [...]} envelope.
    All /queries/* routes use this to guarantee a uniform response shape.

    Args:
        records: Row list from any query_* helper in supabase_client.

    Returns:
        Flask JSON response with count and records.
    """
    return jsonify({"count": len(records), "records": records})


def _enrich_sales_rep(orders: list[dict], limit: int = 20) -> list[dict]:
    """
    Attach sales_rep to a subset of search-stub orders by fetching full detail.

    salesRep is NOT present on POST /v1/sales-orders/search stubs.
    It only appears on GET /v1/sales-orders/{id}.  Each enrichment costs
    one Striven API call, so we cap at `limit` to keep response times
    acceptable.  Orders beyond the limit are returned with sales_rep=None.

    Args:
        orders: List of dicts from _fmt() (search stubs).  Must have an "id" key.
        limit:  Maximum number of orders to enrich (default 20).

    Returns:
        Same list with sales_rep field populated for the first `limit` items.
    """
    enriched = []
    for order in orders[:limit]:
        try:
            raw    = striven.get_estimate(order.get("id") or order.get("estimate_id"))
            detail = _fmt_detail(raw)
            order["sales_rep"] = detail.get("sales_rep_name") or "Unassigned"
        except Exception:
            order["sales_rep"] = None
        enriched.append(order)
    # Orders past the limit pass through unchanged (sales_rep remains None)
    enriched.extend(orders[limit:])
    return enriched


@app.route("/queries/gas-log-missing", methods=["GET"])
def gas_log_missing():
    """
    Gas log installs that are missing a removal fee line item.

    Returns estimates where has_gas_logs=true AND has_removal_fee=false,
    ordered newest-first. Limit defaults to 50; pass ?limit=N to override.

    No LLM. Reads directly from Supabase estimates table.
    Expected response time: < 300ms.
    """
    limit = min(int(request.args.get("limit", 50)), 200)
    try:
        return _run_query(query_gas_log_missing(limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/unassigned-reps", methods=["GET"])
def unassigned_reps():
    """
    Estimates with no sales rep assigned (sales_rep_name = 'Unassigned').

    Surfaces attribution gaps so they can be corrected in Striven.
    Limit defaults to 50; pass ?limit=N to override (max 200).

    No LLM. Reads directly from Supabase estimates table.
    Expected response time: < 300ms.
    """
    limit = min(int(request.args.get("limit", 50)), 200)
    try:
        return _run_query(query_unassigned_reps(limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/no-line-items", methods=["GET"])
def no_line_items():
    """
    Data integrity check: estimates that have zero line items in Supabase.

    Equivalent to:
        SELECT e.estimate_number, e.customer_name
        FROM estimates e
        LEFT JOIN estimate_line_items li ON e.estimate_id = li.estimate_id
        WHERE li.estimate_id IS NULL

    A non-zero result means the sync may have dropped line items for those
    estimates, or they genuinely have no line items in Striven.
    Limit defaults to 50; pass ?limit=N to override (max 200).

    No LLM. Two Supabase reads. Expected response time: < 1s.
    """
    limit = min(int(request.args.get("limit", 50)), 200)
    try:
        return _run_query(query_no_line_items(limit=limit))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Analytics endpoints — read-only, AI-consumption, no LLM
# ---------------------------------------------------------------------------

@app.route("/queries/jobs-by-location", methods=["GET"])
def jobs_by_location():
    """
    Live Striven pipeline: search customers by name → join their estimates.

    Accepts a location/name keyword, searches Striven for matching customers,
    then pulls every estimate for those customers from Supabase and aggregates.

    Query params:
        location  (required) — partial customer or city name to match
        year      (optional) — restrict to a calendar year (e.g. 2024)
        limit     (optional) — max customers to inspect (default 15, max 30)

    Example:
        GET /queries/jobs-by-location?location=charleston&year=2024
    """
    from collections import defaultdict

    location = (
        request.args.get("location")
        or request.args.get("search")
        or ""
    ).strip()
    if not location:
        return jsonify({"error": "Query param 'location' is required."}), 400

    year_raw = request.args.get("year")
    year = None
    if year_raw:
        try:
            year = int(year_raw)
        except ValueError:
            return jsonify({"error": f"Invalid year: {year_raw!r}"}), 400

    cust_limit = min(int(request.args.get("limit", 15)), 30)

    try:
        # ── Step 1: search Striven for customers whose name contains the keyword
        cust_resp = striven.search_customers(location, page_size=cust_limit)
        _, cust_data = _striven_page(cust_resp)
        if not cust_data:
            return jsonify({
                "count": 0,
                "filters": {"location": location, "year": year},
                "data": [],
                "note": "No matching customers found in Striven.",
            })

        customer_ids = [
            c.get("Id") or c.get("id")
            for c in cust_data
            if c.get("Id") or c.get("id")
        ]

        # ── Step 2: pull estimates for these customers from Supabase
        q = (
            _sb_client().table("estimates")
            .select(
                "estimate_id, estimate_number, customer_id, customer_name, "
                "status_normalized, total_amount, created_date, sales_rep_name, target_date"
            )
            .in_("customer_id", customer_ids)
        )
        if year:
            q = (
                q
                .gte("created_date", f"{year}-01-01T00:00:00+00:00")
                .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
            )
        res = q.order("created_date", desc=True).limit(500).execute()
        rows = res.data or []

        # ── Step 3: aggregate per customer, collect sample
        agg: dict[int, dict] = defaultdict(lambda: {
            "total_jobs": 0,
            "total_revenue": 0.0,
            "active_jobs": 0,
            "completed_jobs": 0,
        })
        for r in rows:
            cid = r["customer_id"]
            agg[cid]["customer_name"]  = r["customer_name"]
            agg[cid]["total_jobs"]    += 1
            agg[cid]["total_revenue"] += float(r["total_amount"] or 0)
            if r["status_normalized"] == "ACTIVE":
                agg[cid]["active_jobs"] += 1
            elif r["status_normalized"] == "COMPLETE":
                agg[cid]["completed_jobs"] += 1

        summary = sorted(
            [{"customer_id": cid, **vals} for cid, vals in agg.items()],
            key=lambda x: -x["total_revenue"],
        )

        # Sample: up to 25 most-recent individual estimates
        sample = [
            {
                "estimate_id":     r["estimate_id"],
                "estimate_number": r["estimate_number"],
                "customer":        r["customer_name"],
                "status":          r["status_normalized"],
                "amount":          r["total_amount"],
                "sales_rep":       r["sales_rep_name"],
                "created":         r["created_date"],
            }
            for r in rows[:25]
        ]

        return jsonify({
            "count":   len(rows),
            "filters": {"location": location, "year": year, "customers_searched": len(customer_ids)},
            "data":    summary,
            "sample":  sample,
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/jobs-past-install-date", methods=["GET"])
def jobs_past_install_date():
    """
    Active estimates where today's date has passed the target install date.

    'Active' covers Striven statuses: Quoted, Pending Approval, Approved,
    In Progress (status_normalized = 'ACTIVE').  Estimates with no target
    date set are excluded.  Results are ordered oldest-overdue first.

    Query params:
        limit (optional) — max rows (default 100, max 500)

    Example:
        GET /queries/jobs-past-install-date
        GET /queries/jobs-past-install-date?limit=200
    """
    limit = min(int(request.args.get("limit", 100)), 500)
    try:
        result = query_jobs_past_install_date(limit=limit)
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/sales-rep-backlog", methods=["GET"])
def sales_rep_backlog():
    """
    Active estimate counts grouped by sales rep with three dimensions:
        total_jobs       — all active estimates for this rep
        unscheduled_jobs — active estimates with no target_date set
        overdue_jobs     — active estimates where target_date < today

    Covers all reps with at least one active estimate.  Sorted descending
    by total_jobs so the most loaded reps appear first.

    No query params.  Aggregation happens in Python (PostgREST / supabase-py
    does not support GROUP BY server-side).

    Example:
        GET /queries/sales-rep-backlog
    """
    try:
        result = query_sales_rep_backlog()
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/backlog-by-rep", methods=["GET"])
def backlog_by_rep():
    """
    Live Striven backlog grouped by sales rep.

    Pulls active estimates (Approved=22, In Progress=25) from Striven,
    enriches the first 50 with real salesRep data (GET detail), then
    groups by rep returning total_jobs and total_revenue.

    Query params:
        limit  (optional) — max estimates to enrich with rep detail (default 50, max 100)

    Example:
        GET /queries/backlog-by-rep
        GET /queries/backlog-by-rep?limit=80
    """
    from collections import defaultdict

    enrich_limit = min(int(request.args.get("limit", 50)), 100)

    try:
        # ── Pull active orders: Approved (22) + In Progress (25)
        all_orders: list[dict] = []
        for status_id in (22, 25):
            page = 0
            while True:
                resp  = striven.search_sales_orders({
                    "PageIndex":       page,
                    "PageSize":        100,
                    "StatusChangedTo": status_id,
                })
                total, data = _striven_page(resp)
                if not data:
                    break
                all_orders.extend([_fmt(o) for o in data])
                if (page + 1) * 100 >= total:
                    break
                page += 1

        if not all_orders:
            return jsonify({
                "count":   0,
                "filters": {"statuses": [22, 25]},
                "data":    [],
            })

        # ── Enrich first `enrich_limit` stubs with real salesRep from GET detail
        enriched = _enrich_sales_rep(all_orders, limit=enrich_limit)

        # ── Group by rep
        backlog: dict[str, dict] = defaultdict(lambda: {
            "total_jobs":    0,
            "total_revenue": 0.0,
        })
        for o in enriched:
            rep = o.get("sales_rep") or o.get("sales_rep_name") or "Unassigned"
            backlog[rep]["total_jobs"]    += 1
            backlog[rep]["total_revenue"] += float(o.get("total") or 0)

        reps = [
            {
                "rep":           rep,
                "total_jobs":    vals["total_jobs"],
                "total_revenue": round(vals["total_revenue"], 2),
            }
            for rep, vals in sorted(
                backlog.items(), key=lambda x: -x[1]["total_jobs"]
            )
        ]

        return jsonify({
            "count":          len(all_orders),
            "enriched_count": min(enrich_limit, len(all_orders)),
            "filters":        {"statuses": ["Approved (22)", "In Progress (25)"]},
            "data":           reps,
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/queries/time-to-preview", methods=["GET"])
def time_to_preview():
    """
    Days from estimate creation to first preview/site-inspection task (task type 15).

    Uses live Striven task data (POST /v2/tasks/search, TaskTypeId=15) to find
    the date each "Site Inspections/Preview" task was created.  Then looks up the
    parent estimate's created_date from Supabase and computes the delta in days.

    Returns average, median, sample size, and up to 25 sample records
    sorted fastest-to-slowest.

    Query params:
        limit  (optional) — max tasks to sample (default 200, max 500)

    Example:
        GET /queries/time-to-preview
        GET /queries/time-to-preview?limit=300
    """
    import statistics
    from datetime import datetime

    task_limit = min(int(request.args.get("limit", 200)), 500)

    try:
        # ── Step 1: pull Site Inspection / Preview tasks (type 15) from Striven
        tasks_raw: list[dict] = []
        page = 0
        while len(tasks_raw) < task_limit:
            resp  = striven.search_tasks({
                "PageIndex":  page,
                "PageSize":   100,
                "TaskTypeId": 15,
            })
            total, data = _striven_page(resp)
            if not data:
                break
            tasks_raw.extend(data)
            if (page + 1) * 100 >= total or len(tasks_raw) >= task_limit:
                break
            page += 1

        tasks_raw = tasks_raw[:task_limit]

        if not tasks_raw:
            return jsonify({
                "average_days": None,
                "median_days":  None,
                "sample_size":  0,
                "data_note":    "No preview tasks (type 15) found in Striven.",
                "data":         [],
            })

        # ── Step 2: extract (estimate_id, task_created_date) pairs
        # RelatedEntity on task stubs contains the linked sales-order id
        task_pairs: list[tuple[int, str]] = []
        for t in tasks_raw:
            related = t.get("RelatedEntity") or t.get("relatedEntity") or {}
            est_id  = related.get("Id") or related.get("id")
            t_created = t.get("DateCreated") or t.get("dateCreated")
            if est_id and t_created:
                task_pairs.append((int(est_id), t_created))

        if not task_pairs:
            return jsonify({
                "average_days": None,
                "median_days":  None,
                "sample_size":  0,
                "data_note":    "Tasks found but none linked to an estimate with a created date.",
                "data":         [],
            })

        # ── Step 3: batch-lookup estimate created_dates from Supabase
        est_ids = list({p[0] for p in task_pairs})
        sb_res  = (
            _sb_client()
            .table("estimates")
            .select("estimate_id, estimate_number, customer_name, created_date")
            .in_("estimate_id", est_ids)
            .execute()
        )
        est_map = {
            r["estimate_id"]: r
            for r in (sb_res.data or [])
        }

        # ── Step 4: compute delta for each task
        deltas:  list[float] = []
        samples: list[dict]  = []

        for est_id, task_created_str in task_pairs:
            est = est_map.get(est_id)
            if not est or not est.get("created_date"):
                continue
            try:
                est_dt   = datetime.fromisoformat(
                    est["created_date"].replace("Z", "+00:00")
                )
                task_dt  = datetime.fromisoformat(
                    task_created_str.replace("Z", "+00:00")
                )
                days = (task_dt - est_dt).days
                if days < 0:
                    continue   # data anomaly: task predates estimate
                deltas.append(days)
                samples.append({
                    "estimate_id":     est_id,
                    "estimate_number": est.get("estimate_number"),
                    "customer":        est.get("customer_name"),
                    "days_to_preview": days,
                    "preview_task_date": task_created_str,
                })
            except Exception:
                continue

        if not deltas:
            return jsonify({
                "average_days": None,
                "median_days":  None,
                "sample_size":  0,
                "data_note":    "Could not calculate deltas for any tasks.",
                "data":         [],
            })

        samples.sort(key=lambda x: x["days_to_preview"])

        return jsonify({
            "average_days": round(statistics.mean(deltas), 1),
            "median_days":  round(statistics.median(deltas), 1),
            "sample_size":  len(deltas),
            "data_note":    (
                "Measures days from estimate creation to when the "
                "Site Inspections/Preview task (type 15) was created in Striven."
            ),
            "data":         samples[:25],
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# /ask — keyword router: routes known questions to direct endpoints, falls
#         through to the Claude agentic loop only for unknown questions.
# ---------------------------------------------------------------------------

def _route_question(question: str):
    """
    Pure keyword router — NO LLM involved.

    Matches known question patterns to their direct Supabase query.
    Returns a Flask Response immediately if matched, or None if the
    question should fall through to the Claude /api/chat pipeline.

    Matching is intentionally broad (substring) so natural phrasing works:
        "which gas log jobs are missing removal fees?"  → matched
        "show unassigned sales rep estimates"           → matched
        "estimates with no line items"                  → matched
        "what's the biggest job ever?"                  → None (Claude handles)
    """
    q = question.lower()

    # Gas log + removal fee missing
    if ("gas log" in q or "gas logs" in q) and ("removal" in q or "missing" in q):
        try:
            return _run_query(query_gas_log_missing())
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # Unassigned sales reps
    if "unassigned" in q and ("rep" in q or "sales" in q or "assign" in q):
        try:
            return _run_query(query_unassigned_reps())
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    # Missing line items (data integrity)
    if ("line item" in q or "line items" in q) and ("missing" in q or "no " in q or "without" in q):
        try:
            return _run_query(query_no_line_items())
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    return None   # no direct route matched


@app.route("/ask", methods=["POST"])
def ask():
    """
    Lightweight question endpoint.

    Tries the keyword router first (instant, no LLM). Falls back to a
    simple "no route found" message.  The full agentic Claude loop lives
    at /api/chat — use that for open-ended or exploratory questions.

    Request body (JSON):
        {"question": "which gas log jobs are missing removal fees?"}

    Response (always JSON):
        Matched:   {"count": N, "records": [...]}
        No match:  {"error": "No direct route found", "question": "..."}

    Expected response time: < 300ms for matched questions.
    """
    data     = request.get_json(silent=True) or {}
    question = (data.get("question") or "").strip()

    if not question:
        return jsonify({"error": "Request body must include a non-empty 'question' field."}), 400

    result = _route_question(question)
    if result is not None:
        return result

    return jsonify({
        "error":    "No direct route found for this question.",
        "question": question,
        "hint":     "Use POST /api/chat for open-ended questions handled by Claude.",
    }), 404


# ---------------------------------------------------------------------------
# Snapshot endpoints — lightweight, cached, NO Claude
#
# Purpose: feed the Intelligence Panel cards with instant counts + top-3
# previews so the UI feels live on page load.
#
# Design constraints:
#   • pageSize ≤ 25 per status — keeps each Striven call fast
#   • Parallel Striven calls via ThreadPoolExecutor — multiple statuses
#     fetched concurrently instead of serially
#   • 60-second in-memory cache (reuses existing _cache_get/_cache_set)
#   • Never calls Claude — pure data, no interpretation
#   • Returns 200 on Striven errors with count=0 so the UI degrades
#     gracefully rather than showing broken cards
#
# The "count" in install_gaps is an upper bound (approved + in-progress
# total) because task-level checking would exceed the latency budget.
# The full analyze_install_gaps tool gives the precise count.
# ---------------------------------------------------------------------------

@app.route("/debug-one-order", methods=["GET"])
def debug_one_order():
    """
    Fetch raw Striven sales orders and return every field for inspection.
    ?n=N  — number of records to return (default 5, max 20)
    ?id=N — also fetch the full GET detail for order id N

    Returns raw search stubs AND _fmt() normalised forms side by side.
    No Claude call — safe to hit repeatedly.
    """
    import json as _json
    try:
        n = min(int(request.args.get("n", 5)), 20)
        raw = striven.search_sales_orders({"PageIndex": 0, "PageSize": n})
        orders = raw.get("data") or []
        if not orders:
            print("[debug-one-order] NO ORDERS RETURNED", flush=True)
            return jsonify({"status": "no orders"})

        results = []
        for order in orders:
            normalised = _fmt(order)
            results.append({
                "raw":        order,
                "normalised": normalised,
            })
            print("----- RAW ORDER DEBUG -----", flush=True)
            print(_json.dumps(order, indent=2), flush=True)
            print("----- NORMALISED -----", flush=True)
            print(_json.dumps(normalised, indent=2), flush=True)

        # Optionally fetch the full GET detail for a specific order
        detail_result = None
        detail_id = request.args.get("id")
        if detail_id:
            detail_result = striven.get_estimate(int(detail_id))
            print("----- FULL GET DETAIL -----", flush=True)
            print(_json.dumps(detail_result, indent=2), flush=True)

        return jsonify({
            "status":        "ok",
            "count":         len(results),
            "search_stubs":  results,
            "full_detail":   detail_result,
        })

    except Exception as exc:
        print(f"[debug-one-order] ERROR: {exc}", flush=True)
        return jsonify({"error": str(exc)}), 500


@app.route("/assign-missing-reps", methods=["GET"])
def assign_missing_reps():
    """
    Find all Approved (22) jobs with no SalesRep and assign the fallback rep
    (CreatedBy). Returns a dry-run preview by default.

    Query params:
      dry_run=true   (default) — report only, no writes
      dry_run=false            — perform PATCH updates (requires confirm=yes)
      confirm=yes              — required safety gate when dry_run=false
      page_size=N              — how many Approved jobs to scan (default 50, max 100)

    Example:
      GET /assign-missing-reps                           → dry run, 50 jobs
      GET /assign-missing-reps?dry_run=false&confirm=yes → live update
    """
    import json as _json

    dry_run   = request.args.get("dry_run", "true").lower() != "false"
    confirmed = request.args.get("confirm", "").lower() == "yes"
    page_size = min(int(request.args.get("page_size", 50)), 100)

    # Extra safety gate — live mode requires explicit confirm=yes
    if not dry_run and not confirmed:
        return jsonify({
            "error": "Live mode requires ?dry_run=false&confirm=yes",
            "hint":  "Add &confirm=yes to the URL to proceed with updates.",
        }), 400

    print(
        f"[assign-missing-reps] dry_run={dry_run} page_size={page_size}",
        flush=True,
    )

    # ── 1. Fetch Approved jobs ────────────────────────────────────────────────
    try:
        raw  = striven.search_sales_orders({
            "PageIndex": 0, "PageSize": page_size, "StatusChangedTo": 22,
        })
        rows = raw.get("data") or []
    except Exception as exc:
        print(f"[assign-missing-reps] fetch error: {exc}", flush=True)
        return jsonify({"error": f"Failed to fetch approved jobs: {exc}"}), 500

    print(f"[assign-missing-reps] fetched {len(rows)} approved jobs", flush=True)

    # ── 2. Identify jobs with no SalesRep ─────────────────────────────────────
    to_fix = []
    skipped_no_fallback = []

    for r in rows:
        sr = (r.get("SalesRep") or r.get("salesRep") or {})
        if sr.get("Id") or sr.get("id"):
            continue  # already has a rep — skip

        # No SalesRep — try to find a fallback
        created_by = (r.get("CreatedBy") or r.get("createdBy") or {})
        assigned_to = (r.get("AssignedTo") or r.get("assignedTo") or {})

        fallback_id   = (created_by.get("Id")   or created_by.get("id")
                         or assigned_to.get("Id") or assigned_to.get("id"))
        fallback_name = (created_by.get("Name") or created_by.get("name")
                         or assigned_to.get("Name") or assigned_to.get("name")
                         or "Unknown")

        order_id  = r.get("Id") or r.get("id")
        order_num = r.get("Number") or r.get("number") or str(order_id)
        customer  = (r.get("Customer") or r.get("customer") or {})
        cust_name = customer.get("Name") or customer.get("name") or "—"

        if not fallback_id:
            skipped_no_fallback.append({
                "id": order_id, "number": order_num, "customer": cust_name,
                "reason": "No CreatedBy or AssignedTo available",
            })
            print(
                f"[assign-missing-reps] SKIP {order_num} — no fallback rep available",
                flush=True,
            )
            continue

        to_fix.append({
            "id":            order_id,
            "number":        order_num,
            "customer":      cust_name,
            "fallback_id":   fallback_id,
            "fallback_name": fallback_name,
            "_raw":          r,          # kept for update payload; stripped from response
        })

    print(
        f"[assign-missing-reps] missing_rep={len(to_fix)} "
        f"no_fallback={len(skipped_no_fallback)} "
        f"already_assigned={len(rows)-len(to_fix)-len(skipped_no_fallback)}",
        flush=True,
    )

    # ── 3. Dry run — report without writing ───────────────────────────────────
    if dry_run:
        preview = [
            {
                "id":           j["id"],
                "number":       j["number"],
                "customer":     j["customer"],
                "would_assign": j["fallback_name"],
                "fallback_id":  j["fallback_id"],
            }
            for j in to_fix
        ]
        return jsonify({
            "mode":                "DRY RUN — no changes made",
            "total_scanned":       len(rows),
            "already_assigned":    len(rows) - len(to_fix) - len(skipped_no_fallback),
            "will_be_updated":     len(to_fix),
            "skipped_no_fallback": len(skipped_no_fallback),
            "preview":             preview,
            "skipped_detail":      skipped_no_fallback,
            "next_step":           "Add ?dry_run=false&confirm=yes to apply",
        })

    # ── 4. Live mode — PATCH each job ─────────────────────────────────────────
    updated  = []
    errors   = []

    for j in to_fix:
        order_id = j["id"]
        try:
            # Minimal PATCH payload — only set the SalesRep field
            patch_body = {"SalesRep": {"Id": j["fallback_id"]}}

            print(
                f"[assign-missing-reps] PATCH {j['number']} "
                f"SalesRep → {j['fallback_name']} (id={j['fallback_id']})",
                flush=True,
            )
            striven.update_sales_order(order_id, patch_body)
            updated.append({
                "id":       order_id,
                "number":   j["number"],
                "customer": j["customer"],
                "assigned": j["fallback_name"],
            })

        except Exception as exc:
            print(
                f"[assign-missing-reps] ERROR patching {j['number']}: {exc}",
                flush=True,
            )
            errors.append({
                "id": order_id, "number": j["number"],
                "error": str(exc),
            })

    return jsonify({
        "mode":                "LIVE — updates applied",
        "total_scanned":       len(rows),
        "updated":             len(updated),
        "failed":              len(errors),
        "skipped_no_fallback": len(skipped_no_fallback),
        "results":             updated,
        "errors":              errors,
    })


@app.route("/snapshot/stuck_jobs", methods=["GET"])
def snapshot_stuck_jobs():
    """
    Return the count of stuck jobs and the top-3 worst offenders.

    Stuck definitions (no task checking — purely date-based):
      Quoted      > 7 days  since date_created
      Approved    > 5 days  since date_approved
      In Progress > 10 days since date_approved (proxy — no task API call)
    """
    cached = _cache_get("snap:stuck_jobs")
    if cached is not None:
        return jsonify(cached)

    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    today = datetime.utcnow()
    THRESH = {19: 7, 22: 5, 25: 10}
    ISSUE  = {
        19: "Quoted, no follow-up",
        22: "Approved, not scheduled",
        25: "In progress, no install confirmed",
    }

    def _fetch(sid):
        try:
            raw = striven.search_sales_orders({
                "PageIndex": 0, "PageSize": 25, "StatusChangedTo": sid,
            })
            return sid, raw.get("data") or []
        except Exception as exc:
            print(f"[snap:stuck] WARNING status={sid}: {exc}", flush=True)
            return sid, []

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            fetched = list(pool.map(_fetch, [19, 22, 25]))
    except Exception as exc:
        print(f"[snap:stuck] ERROR: {exc}", flush=True)
        return jsonify({"count": 0, "top": [], "error": str(exc)})

    stuck: list[dict] = []
    for sid, data in fetched:
        for r in data:
            est     = _fmt(r)
            ref_str = est.get("date_approved") if sid == 22 else est.get("date_created")
            ref     = _parse_date_str(ref_str)
            if ref is None:
                continue
            days = (today - ref).days
            if days > THRESH[sid]:
                stuck.append({
                    "id":       str(est.get("estimate_number") or est.get("id") or ""),
                    "customer": est.get("customer_name") or "Unknown",
                    "rep":      est.get("sales_rep") or "Unassigned",
                    "days":     days,
                    "issue":    ISSUE[sid],
                })

    stuck.sort(key=lambda x: x["days"], reverse=True)

    # Dollar impact — sum OrderTotal across all stuck estimates
    # The total field is populated when available on search stubs.
    total_value = 0
    for sid, data in fetched:
        for r in data:
            est = _fmt(r)
            ref_str = est.get("date_approved") if sid == 22 else est.get("date_created")
            ref     = _parse_date_str(ref_str)
            if ref is None:
                continue
            days = (today - ref).days
            if days > THRESH[sid] and est.get("total"):
                try:
                    total_value += float(est["total"])
                except (TypeError, ValueError):
                    pass

    result = {
        "count": len(stuck),
        "top":   stuck[:3],
        "impact": {"total_value": round(total_value), "currency": "USD"},
    }
    _cache_set("snap:stuck_jobs", result)
    print(f"[snap:stuck] count={result['count']} impact=${result['impact']['total_value']:,}", flush=True)
    return jsonify(result)


@app.route("/snapshot/install_gaps", methods=["GET"])
def snapshot_install_gaps():
    """
    Return the count of approved/in-progress jobs and the 3 oldest (by approval date).

    Note: count is an upper bound — task checking is skipped for speed.
    The full analyze_install_gaps tool gives the precise figure.
    """
    cached = _cache_get("snap:install_gaps")
    if cached is not None:
        return jsonify(cached)

    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    today = datetime.utcnow()

    def _fetch(sid):
        try:
            raw = striven.search_sales_orders({
                "PageIndex": 0, "PageSize": 25, "StatusChangedTo": sid,
            })
            return sid, raw.get("data") or []
        except Exception as exc:
            print(f"[snap:install] WARNING status={sid}: {exc}", flush=True)
            return sid, []

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            fetched = list(pool.map(_fetch, [22, 25]))
    except Exception as exc:
        print(f"[snap:install] ERROR: {exc}", flush=True)
        return jsonify({"count": 0, "top": [], "error": str(exc)})

    STATUS_LABEL = {22: "Approved", 25: "In Progress"}
    all_ests: list[dict] = []
    for sid, data in fetched:
        label = STATUS_LABEL.get(sid, "Active")
        for r in data:
            est     = _fmt(r)
            ref_str = est.get("date_approved") or est.get("date_created")
            ref     = _parse_date_str(ref_str)
            days    = (today - ref).days if ref else 0
            all_ests.append({
                "id":       str(est.get("estimate_number") or est.get("id") or ""),
                "customer": est.get("customer_name") or "Unknown",
                "rep":      est.get("sales_rep") or "Unassigned",
                "days":     days,
                "issue":    f"{label}, install not yet confirmed",
                "_total":   est.get("total"),   # internal — stripped before returning
            })

    all_ests.sort(key=lambda x: x["days"], reverse=True)

    # Dollar impact — sum totals; strip internal _total key from all items
    impact_value = 0
    for e in all_ests:
        raw_total = e.pop("_total", None)
        if raw_total:
            try:
                impact_value += float(raw_total)
            except (TypeError, ValueError):
                pass

    result = {
        "count": len(all_ests),
        "top":   all_ests[:3],
        "impact": {"total_value": round(impact_value), "currency": "USD"},
        "note":  "Upper bound — full analysis confirms which have no install task",
    }
    _cache_set("snap:install_gaps", result)
    print(f"[snap:install] count={result['count']} impact=${result['impact']['total_value']:,}", flush=True)
    return jsonify(result)


@app.route("/snapshot/rep_pipeline", methods=["GET"])
def snapshot_rep_pipeline():
    """
    Return the count of active sales reps and the top-3 by open-job volume.
    """
    cached = _cache_get("snap:rep_pipeline")
    if cached is not None:
        return jsonify(cached)

    from concurrent.futures import ThreadPoolExecutor

    def _fetch(sid):
        try:
            raw = striven.search_sales_orders({
                "PageIndex": 0, "PageSize": 25, "StatusChangedTo": sid,
            })
            return raw.get("data") or []
        except Exception as exc:
            print(f"[snap:rep] WARNING status={sid}: {exc}", flush=True)
            return []

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            batches = list(pool.map(_fetch, [22, 25]))
    except Exception as exc:
        print(f"[snap:rep] ERROR: {exc}", flush=True)
        return jsonify({"count": 0, "top": [], "error": str(exc)})

    rep_counts: dict[str, int] = {}
    for data in batches:
        for r in data:
            rep = _fmt(r).get("sales_rep") or "Unassigned"
            rep_counts[rep] = rep_counts.get(rep, 0) + 1

    reps_sorted = sorted(rep_counts.items(), key=lambda x: x[1], reverse=True)
    top3 = [
        {
            "id":       rep,     # rep name used as drill-down key
            "customer": rep,
            "days":     count,   # "days" field reused for job count
            "issue":    f"{count} open job{'s' if count != 1 else ''}",
        }
        for rep, count in reps_sorted[:3]
    ]

    result = {"count": len(rep_counts), "top": top3}
    _cache_set("snap:rep_pipeline", result)
    print(f"[snap:rep] reps={result['count']}", flush=True)
    return jsonify(result)


@app.route("/snapshot/pipeline", methods=["GET"])
def snapshot_pipeline():
    """
    Return a live summary of the full active sales pipeline.

    Fetches Quoted (19), Approved (22), and In Progress (25) in parallel.
    Returns total job count, per-stage counts, and the top-3 highest-risk
    jobs (oldest by days in stage) for the preview panel.

    60-second cache. No Claude. No task API calls.
    """
    cached = _cache_get("snap:pipeline")
    if cached is not None:
        return jsonify(cached)

    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    today = datetime.utcnow()
    THRESH      = {19: 7, 22: 5, 25: 10}
    STATUS_NAMES = {19: "Quoted", 22: "Approved", 25: "In Progress"}

    def _fetch(sid):
        try:
            raw = striven.search_sales_orders({
                "PageIndex": 0, "PageSize": 25, "StatusChangedTo": sid,
            })
            return sid, raw.get("data") or []
        except Exception as exc:
            print(f"[snap:pipeline] WARNING status={sid}: {exc}", flush=True)
            return sid, []

    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            fetched = list(pool.map(_fetch, [19, 22, 25]))
    except Exception as exc:
        print(f"[snap:pipeline] ERROR: {exc}", flush=True)
        return jsonify({"count": 0, "top": [], "totals": {}, "error": str(exc)})

    all_jobs: list[dict] = []
    stage_counts = {"quoted": 0, "approved": 0, "in_progress": 0}
    SKEY = {19: "quoted", 22: "approved", 25: "in_progress"}

    for sid, data in fetched:
        status_name = STATUS_NAMES[sid]
        stage_counts[SKEY[sid]] += len(data)
        for r in data:
            est     = _fmt(r)
            ref_str = est.get("date_approved") if sid == 22 else est.get("date_created")
            ref     = _parse_date_str(ref_str)
            days    = (today - ref).days if ref else 0
            all_jobs.append({
                "id":       str(est.get("estimate_number") or est.get("id") or ""),
                "customer": est.get("customer_name") or "Unknown",
                "days":     days,
                "is_stuck": days > THRESH[sid],
                "status":   status_name,
                "issue":    f"{status_name}, {days} days",
            })

    # Top 3: prioritise stuck jobs, then sort by days desc
    stuck = [j for j in all_jobs if j["is_stuck"]]
    stuck.sort(key=lambda x: -x["days"])
    top3  = [{"id": j["id"], "customer": j["customer"],
               "days": j["days"], "issue": j["issue"]}
              for j in stuck[:3]]

    result = {
        "count":  len(all_jobs),
        "top":    top3,
        "totals": {
            "quoted":      stage_counts["quoted"],
            "approved":    stage_counts["approved"],
            "in_progress": stage_counts["in_progress"],
            "stuck":       len(stuck),
        },
    }
    _cache_set("snap:pipeline", result)
    print(
        f"[snap:pipeline] total={result['count']} "
        f"stuck={result['totals']['stuck']}",
        flush=True,
    )
    return jsonify(result)


@app.route("/snapshot/gas_log_audit", methods=["GET"])
def snapshot_gas_log_audit():
    """
    Return a lightweight gas-log audit summary.

    Priority order (fastest → slowest):
      1. Supabase (persisted from last full scan — survives server restarts)
      2. In-memory _GAS_LOG_CACHE (5-min TTL, fastest on warm server)
      3. Full scan via _run_gas_log_audit() (60 s — cold start only)

    All computation is done in Python via _run_gas_log_audit().
    Claude is never involved in the data processing.

    Response shape:
      {
        "count":            N,   # jobs missing the removal fee
        "total_checked":    N,
        "gas_log_installs": N,
        "top":              [{number, customer, url}]  # top 3 for preview panel
      }
    """
    try:
        # ── Priority 1: Supabase (persists across server restarts) ───────────
        # The summary row (id=1) is written every time _run_gas_log_audit()
        # completes a full scan.  If it exists we return it immediately —
        # no Striven API call, no compute.
        sb_row = None
        try:
            sb_row = _sb_get_gas_log_audit()
        except Exception as _sb_read_err:
            print(
                f"[snap:gas_log_audit] Supabase read failed (non-fatal): {_sb_read_err}",
                flush=True,
            )

        if sb_row:
            print(
                f"[snap:gas_log_audit] Supabase HIT — "
                f"missing={sb_row.get('missing_count')} "
                f"total_checked={sb_row.get('total_checked')} "
                f"updated_at={sb_row.get('updated_at')}",
                flush=True,
            )
            snap = {
                "count":            sb_row.get("missing_count", 0),
                "total_checked":    sb_row.get("total_checked", 0),
                "gas_log_installs": None,   # not stored in summary row
                "percent_missing":  sb_row.get("percent_missing", 0.0),
                "top":              [],     # detail not persisted — fetch lazily if needed
                "source":           "supabase",
                "updated_at":       sb_row.get("updated_at"),
            }
            return jsonify(snap)

        # ── Priority 2 & 3: in-memory cache then full scan ───────────────────
        # _run_gas_log_audit() handles both: returns cache if warm, else scans.
        result  = _run_gas_log_audit()
        missing = result.get("missing_removal_fee", 0)
        top3 = [
            {
                "id":       m.get("estimate_number"),
                "customer": m.get("customer_name"),
                "days":     0,
                "issue":    "Missing gas log removal fee",
                "url":      m.get("url"),
            }
            for m in (result.get("matches") or [])[:3]
        ]
        snap = {
            "count":            missing,
            "total_checked":    result.get("total_checked"),
            "gas_log_installs": result.get("gas_log_installs"),
            "top":              top3,
            "source":           "computed",
        }
        print(
            f"[snap:gas_log_audit] computed — missing={missing} "
            f"total_checked={result.get('total_checked')}",
            flush=True,
        )
        return jsonify(snap)

    except Exception as exc:
        print(f"[snap:gas_log_audit] ERROR: {exc}", flush=True)
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

You have TWO knowledge sources:
  A. LIVE STRIVEN DATA — real-time via Striven tools (estimates, tasks, invoices, etc.)
  B. KNOWLEDGE BASE    — company documents via search_knowledge tool:
       audit_rules.md         — all audit patterns and business rules
       order_lifecycle.md     — status flow, stuck definitions, workflow stages
       product_categories.md  — job types, what each includes, audit triggers
       customer_types.md      — builder accounts, homeowner patterns, geography
       striven_fields.md      — field definitions, API conventions, status codes
       order_naming.md        — naming convention and common errors
       estimating_standards.md — what makes a complete estimate per job type
       roles.md               — how to tailor answers by who is asking
       company_overview.md    — company background, vendors, revenue drivers
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
CARDINAL RULES
════════════════════════════════════════════════════════
1. Always call a tool. Never answer from memory. Never guess a number.
2. Always summarise first. Totals and key findings before any detail.
3. Never dump raw lists. Max 5 rows by default; 10 only if the user asks.
4. This system is READ-ONLY. If asked to create, modify, or delete anything:
   "This system is read-only and cannot make changes."
5. Intelligence module results are PRE-SUMMARISED: you receive counts, averages,
   and top-N examples only — not the full dataset. Focus on identifying issues,
   patterns, and recommended actions from the summary provided.
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
  how long from approval to install       → analyze_job_pipeline
  process breakdown / pipeline report     → analyze_job_pipeline

INTELLIGENCE MODULES (structured analysis — Claude interprets, not computes)
  stuck / delayed / stalled / not moving  → analyze_stuck_jobs
  no install scheduled / install gaps     → analyze_install_gaps
  sales rep health / rep performance      → analyze_rep_pipeline
  which rep has most problems             → analyze_rep_pipeline
  what jobs need to be scheduled          → analyze_install_gaps
  weekly pipeline / pipeline review       → analyze_weekly_pipeline
  sales pipeline / pipeline report        → analyze_weekly_pipeline
  full pipeline / pipeline by rep         → analyze_weekly_pipeline

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

KNOWLEDGE BASE — call search_knowledge for:
  what is the process for [job type]      → search_knowledge
  what should a [job type] estimate include → search_knowledge + get_estimate_by_id
  what fees are required on [job]         → search_knowledge
  what does [term / status] mean          → search_knowledge
  what are our audit rules                → search_knowledge
  who are our builder customers           → search_knowledge
  does estimate #N include everything     → get_estimate_by_id + search_knowledge
  is this estimate correct / complete     → get_estimate_by_id + search_knowledge
  what job types require a preview        → search_knowledge
  naming convention / order name format   → search_knowledge
  how does [workflow step] work           → search_knowledge
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
ROLE DETECTION — TAILOR YOUR RESPONSE
════════════════════════════════════════════════════════
Infer who is asking from their question and adjust accordingly:

WILLIAM SMITH (CEO/Owner) — asks about revenue, risk, business health
  → Lead with the dollar number or headline risk. One-paragraph max.
  → Skip operational detail. Surface red flags.

OPERATIONS / PROJECT MANAGERS — asks about stuck jobs, scheduling, pipeline
  → Job-level tables: Estimate #, Customer, Status, Date, Issue
  → Group by problem type. Make it actionable: "These 7 need a preview task today."

SALES REPS — asks about specific customers or their own estimates
  → Filter to relevant data. Flag their own audit issues.
  → Tone: helpful, not accusatory.

SCHEDULERS — asks about what's ready to schedule, techs, geographic clusters
  → List approved jobs with city and job type. Note tech workload.
  → Mention geographic grouping where applicable (Charleston traffic).

ACCOUNTING (David) — asks about AR, missing fees, payments, revenue
  → Dollar amounts first. Sort by balance descending.
  → Separate overdue from not-yet-due. State exact revenue at risk.

SERVICE MANAGER — asks about open service calls, techs, callbacks
  → Task-level detail. Group by technician. Flag overdue.
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
PROACTIVE FLAG PROTOCOL
════════════════════════════════════════════════════════
Whenever you retrieve live data, scan for these and surface them
WITHOUT being asked — add an ANOMALIES section if any are found:

  ⚠ Estimates with OrderTotal = $0 or missing total
  ⚠ Incomplete (status 18) estimates older than 7 days
  ⚠ Quoted (status 19) estimates older than 30 days — follow-up needed
  ⚠ Builder customers with 3+ open estimates — flag for pipeline review
  ⚠ Approved estimates older than 14 days with no install task
  ⚠ Gas log / burner jobs — flag if you suspect removal fee may be missing
    (full audit requires gas_log_audit tool)

Do not list every anomaly if there are many — summarise: "⚠ 6 estimates are
over 30 days old in Quoted status — ask me to show them if needed."
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
HYBRID QUESTION ROUTING
════════════════════════════════════════════════════════
LIVE DATA ONLY — just Striven tools:
  "How many estimates do we have?" / "Show me stuck jobs" / "Who owes us money?"

KNOWLEDGE ONLY — just search_knowledge:
  "What's our chimney repair process?" / "What fees are required on a gas log install?"
  "What does 'Incomplete' mean?" / "Who are our builder customers?"

HYBRID — search_knowledge THEN Striven tool(s):
  "Does estimate 9264 include everything it should?"
    → search_knowledge("estimating standards gas log") + get_estimate_by_id(9264)
  "Is this job priced correctly?"
    → search_knowledge("pricing standards [job type]") + get_estimate_by_id(N)
  "Show me Scenic Custom Homes' open jobs and flag anything wrong"
    → search_estimates_by_customer("Scenic Custom Homes") + search_knowledge("audit rules")

AUDIT — dedicated audit tools:
  "Run the gas log fee audit" → gas_log_audit
  "Check portal flags" → portal_flag_audit
  "Where are jobs breaking?" → analyze_job_pipeline
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
INTELLIGENCE MODULES — OUTPUT FORMAT
════════════════════════════════════════════════════════
When analyze_stuck_jobs, analyze_install_gaps, or analyze_rep_pipeline
returns data, ALWAYS respond in this exact order:

1. SUMMARY — one bold sentence with the critical finding and count.
   ✓ "**23 jobs are stuck — 14 quoted with no follow-up, 9 approved with no action.**"
   ✗ "Based on the analysis data, I can see that there are jobs that are stuck..."

2. KEY FINDINGS — 3–5 bullet points with specific numbers and context.
   • Longest stuck: [Customer], [N] days in [Status]
   • Most affected rep: [Name] — [N] of their [X] jobs have issues
   • Revenue at risk: $[total] across [N] stuck jobs

3. EXAMPLES — table of the top 5–8 worst offenders (sorted by days, highest first).
   | Customer | Rep | Status | Days | Issue |
   Use plain-English issue text — no raw field names like "no_install_task".

4. ACTIONS — 2–3 specific, role-appropriate next steps.
   ✓ "Contact [Customer] today — their quote has been sitting 18 days."
   ✓ "Schedule install tasks for the 9 approved jobs before end of week."
   ✗ "You should follow up on stuck jobs." (too generic)

RULES:
  ✓ Always state the count in the SUMMARY (never vague)
  ✓ Dollar amounts with $ sign, rounded to nearest dollar
  ✓ Rep names from the data — never invent names
  ✓ Percentages where useful: "7 of 15 approved jobs (47%) have no install"
  ✗ Never show raw JSON field names in the response
  ✗ Never say "the data shows" or "based on the analysis"
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
WEEKLY PIPELINE — FORMAT WHEN analyze_weekly_pipeline RETURNS
════════════════════════════════════════════════════════
ALWAYS respond in this exact order when analyze_weekly_pipeline returns:

1. SUMMARY — one bold sentence: total pipeline + single biggest risk.
   "**83 jobs in pipeline ($1.2M) — 34 (41%) are stuck or delayed.**"
   Include total_pipeline_value if non-zero. Include stuck_pct.

2. KEY FINDINGS — 3–5 bullets covering each stage and the overall health.
   • Quoted: [N] jobs, [X] sitting >[threshold] days — needs follow-up
   • Approved: [N] jobs, [X] over threshold — scheduling not initiated
   • In Progress: [N] jobs, [X] without confirmed install task
   • Install task check: confirmed for [N] jobs (sample — full check via Install Gaps module)

3. REP BREAKDOWN — table sorted by Stuck (worst first), max 8 reps.
   | Rep | Total | Quoted | Approved | In Progress | Stuck | No Install |
   ✓ Skip reps with zero stuck and zero missing install
   ✓ Bold the worst rep's row mentally — call them out in ACTIONS

4. TOP RISKS — table of up to 10 jobs, sorted by days-in-stage desc.
   | Job # | Customer | Rep | Status | Days | Risk |
   Translate risk codes to plain English.

5. ACTIONS — 3–5 specific, role-appropriate next steps. Be direct.
   ✓ "Rep [Name] has [N] stuck jobs — schedule a pipeline review with them today."
   ✓ "Contact [Customer] — their quote has been sitting [N] days with no response."
   ✓ "Book install tasks for the [N] approved jobs before end of week."

RULES:
  ✓ Dollar amounts: $1,200,000 → "$1.2M"; $45,000 → "$45K"; $8,500 → "$8,500"
  ✓ Show total_pipeline_value in SUMMARY if > 0
  ✓ Stuck percentage always shown: "34 of 83 jobs (41%)"
  ✓ install_checked note: "Install confirmed for X of Y checked — run Install Gaps for full picture"
  ✗ Never show raw field names (no "over_threshold", "status_id", etc.)
  ✗ Do not repeat the same job in both KEY FINDINGS and TOP RISKS tables
════════════════════════════════════════════════════════

════════════════════════════════════════════════════════
PIPELINE ANALYSIS — FORMAT WHEN analyze_job_pipeline RETURNS
════════════════════════════════════════════════════════
ALWAYS present results in this exact order:

1. HEADLINE — total analysed and the single most critical finding.
   "**Of 20 approved jobs, 13 (65%) are missing at least one required step.**"

2. JOB TYPE BREAKDOWN — show the split first so context is clear:
   • Jobs requiring a preview task (remodel / new construction): X
   • Jobs exempt from preview requirement (enhancement / repair / service): X
   • Jobs not yet classifiable: X — note these are excluded from preview failure %

3. PREVIEW ISSUES — percentages are ALWAYS out of the required set, not total:
   • No preview task: X of Y required jobs (Z%)
   • Preview admin task only — no site preview confirmed: X (Z%) — only if > 0
   • Preview created late (>3 days after approval): X (Z%)
   • Avg days from approval to preview: N days (or "not enough data")

4. INSTALL ISSUES — applies to all jobs:
   • No install scheduled: X of Y total jobs (Z%)
   • Avg days from approval to install: N days

5. BY SALES REP — table sorted by issue count, max 6 rows, only reps with issues:
   | Rep | Jobs | Preview Required | No Preview | No Install |

6. EXAMPLE PROBLEM JOBS — up to 10 rows:
   | Estimate # | Customer | Job Type | Approved | Issue |
   Use plain language for Issue: "No preview task", "Preview 8 days late", "No install scheduled"

7. ONE sentence offering to drill into a specific rep, date range, or job.

RULES:
  ✗ Never expose raw field names (no "no_preview_task", "preview_late_8d", "exempt:chimney")
  ✓ Translate issue codes and reason codes to plain English
  ✓ Always show percentages alongside counts
  ✓ Make the required vs exempt split visible — it is the most important context
  ✓ If total_unclassified > 0, note it clearly: "N jobs could not be classified and are excluded"
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
    # ── Intelligence Modules ───────────────────────────────────────────────────
    {
        "name": "analyze_stuck_jobs",
        "description": (
            "Identify jobs that are stuck — not progressing through the pipeline. "
            "Checks Quoted (>7 days), Approved (>5 days), and In Progress (>10 days "
            "with no install task). Returns each stuck job with customer, rep, "
            "days stuck, and the specific issue. "
            "Use for: 'what jobs are stuck', 'delayed jobs', 'stalled estimates', "
            "'which jobs aren't moving', 'pipeline problems'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max estimates to check per status (default 50, max 50).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "analyze_install_gaps",
        "description": (
            "Find approved or in-progress jobs that have NO install task scheduled. "
            "Returns each job with customer, rep, status, and days since approval. "
            "Use for: 'what jobs have no install scheduled', 'install gaps', "
            "'what needs to be scheduled', 'missing install tasks', "
            "'what's not scheduled yet', 'scheduling gaps'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max estimates to check (default 40, max 50).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "analyze_rep_pipeline",
        "description": (
            "Sales rep pipeline health — groups job issues by rep. "
            "Shows each rep's total jobs, stuck jobs, jobs missing install, "
            "and average days from approval to install. Worst reps listed first. "
            "Use for: 'sales rep performance', 'rep pipeline', 'how are reps doing', "
            "'which rep has the most stuck jobs', 'rep accountability', "
            "'who has the most problems', 'rep health check'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max estimates to analyse across all reps (default 30, max 50).",
                },
            },
            "required": [],
        },
    },
    {
        "name": "analyze_weekly_pipeline",
        "description": (
            "Full weekly sales pipeline review — replaces the Excel pipeline report. "
            "Fetches Quoted, Approved, and In Progress estimates, computes days in stage, "
            "checks install tasks, groups by sales rep, and returns: "
            "(1) pipeline_summary — count/avg-days/over-threshold per stage, "
            "(2) rep_summary — per-rep breakdown with stuck and missing-install counts, "
            "(3) top_risks — up to 10 highest-urgency jobs, "
            "(4) totals — total pipeline jobs, value, and stuck percentage. "
            "Use for: 'weekly pipeline', 'pipeline review', 'sales pipeline report', "
            "'pipeline meeting', 'pipeline status', 'how is the pipeline', "
            "'weekly report', 'pipeline by rep', 'full pipeline review'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Max estimates per status group (default 40, max 75).",
                },
            },
            "required": [],
        },
    },
    # ── Knowledge Base ─────────────────────────────────────────────────────────
    {
        "name": "search_knowledge",
        "description": (
            "Search the WilliamSmith internal knowledge base. "
            "Use this tool to answer questions about: company procedures, "
            "audit rules, product categories, job types, order lifecycle, "
            "customer types, Striven field definitions, order naming conventions, "
            "and role-specific guidance. "
            "Always call this tool BEFORE answering questions about: "
            "'what is the correct process for...', "
            "'what are the rules for...', "
            "'what should a [job type] estimate include', "
            "'what does [term] mean', "
            "'how does [workflow step] work', "
            "'who are our builder customers', "
            "'what job types require a preview'. "
            "Do NOT call this for live Striven data — use Striven tools for that."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "The question or topic to search the knowledge base for.",
                },
                "top_k": {
                    "type": "integer",
                    "description": "Maximum number of knowledge sections to return (default 4, max 8).",
                },
            },
            "required": ["query"],
        },
    },
]
# NOTE: create_task and update_task are intentionally excluded.
# This is a read-only BI system. Write operations are not exposed as tools.


def _resolve_rep(r: dict, *, allow_created_by_fallback: bool = False) -> tuple[int | None, str]:
    """
    Extract sales rep from a raw Striven order dict.

    CONFIRMED field mapping (from live GET /v1/sales-orders/{id} responses):
        salesRep   → {"id": int, "name": string}   ← PRIMARY; only on full GET detail
        assignedTo → {"id": int, "name": string}   ← rare alternative
        employee   → {"id": int, "name": string}   ← very rare

    IMPORTANT — search stubs vs full GET detail:
        POST /v1/sales-orders/search does NOT return salesRep.
        salesRep is only present on GET /v1/sales-orders/{id}.
        Running _resolve_rep() on a search stub will return "Unassigned"
        unless allow_created_by_fallback=True is explicitly passed.

    createdBy fallback:
        Only used when allow_created_by_fallback=True (i.e., called from
        _fmt_detail() on a full GET record).  In this business, salesRep and
        createdBy are often the same person, so this is a reasonable fallback
        for the detail view.  It is NOT used on search stubs to avoid
        misattributing the data-entry clerk as the sales rep.

    Returns (rep_id, rep_name).
        rep_name is always a string — "Unassigned" if nothing resolves.
        rep_id   is int or None.

    Both camelCase and TitleCase keys checked for API resilience.
    """
    primary_keys = (
        ("salesRep",   "SalesRep"),
        ("assignedTo", "AssignedTo"),
        ("employee",   "Employee"),
    )
    fallback_keys = (
        ("createdBy",  "CreatedBy"),
    )

    for camel, title in primary_keys:
        obj = r.get(camel) or r.get(title)
        if isinstance(obj, dict):
            name = obj.get("name") or obj.get("Name")
            if name:
                return obj.get("id") or obj.get("Id"), name

    if allow_created_by_fallback:
        for camel, title in fallback_keys:
            obj = r.get(camel) or r.get(title)
            if isinstance(obj, dict):
                name = obj.get("name") or obj.get("Name")
                if name:
                    return obj.get("id") or obj.get("Id"), name

    return None, "Unassigned"


def _extract_custom_field(custom_fields: list, field_id: int) -> str | None:
    """
    Extract a value from the customFields array by field id.

    customFields is a list of dicts with shape:
        {"id": int, "name": string, "value": string|null, "valueText": string|null}

    Returns valueText if set, else value, else None.
    Handles multi-value fields (same id repeated) by returning the first match.
    """
    for cf in (custom_fields or []):
        if cf.get("id") == field_id:
            return cf.get("valueText") or cf.get("value") or None
    return None


def _fmt(r: dict) -> dict:
    """
    Normalise a raw Striven sales-order SEARCH STUB into a clean dict.

    Called on results from POST /v1/sales-orders/search.

    CONFIRMED fields present on search stubs (TitleCase):
        Id, Number, Name
        Customer  → {Id, Name}
        Status    → {Id, Name}
        DateCreated

    CONFIRMED fields NOT present on search stubs:
        salesRep    → always null here; use _fmt_detail() for rep data
        orderTotal  → always null here; use _fmt_detail() for totals
        dateApproved → does not exist in the Striven API at all

    sales_rep_name will be "Unassigned" for all search stub results.
    Use get_estimate_by_id + _fmt_detail() when rep/total data is required.
    """
    customer = r.get("Customer") or r.get("customer") or {}
    status   = r.get("Status")   or r.get("status")   or {}

    # salesRep is NOT in search stubs — _resolve_rep() will return "Unassigned"
    # unless the rare case where the stub happens to include it.
    rep_id, rep_name = _resolve_rep(r, allow_created_by_fallback=False)

    return {
        "id":              r.get("Id")           or r.get("id"),
        "estimate_number": r.get("Number")       or r.get("number"),
        "name":            r.get("Name")         or r.get("name"),
        "customer_name":   customer.get("Name")  or customer.get("name"),
        "customer_id":     customer.get("Id")    or customer.get("id"),
        "status":          status.get("Name")    or status.get("name"),
        "status_id":       status.get("Id")      or status.get("id"),
        # sales_rep_name will be "Unassigned" on search stubs — salesRep not returned by search API
        "sales_rep_name":  rep_name,
        "sales_rep_id":    rep_id,
        "sales_rep":       rep_name,             # backward-compat alias
        # total not available on search stubs — always null until full GET is fetched
        "total":           r.get("OrderTotal")   or r.get("orderTotal") or None,
        "date_created":    r.get("DateCreated")  or r.get("dateCreated"),
        # dateApproved does not exist in the Striven API — removed
    }


def _fmt_detail(r: dict) -> dict:
    """
    Normalise a raw Striven sales-order FULL DETAIL record into a clean dict.

    Called on results from GET /v1/sales-orders/{id}.

    CONFIRMED fields on full GET detail (camelCase):
        id, orderNumber, orderName
        customer    → {id, name, number}
        status      → {id, name}
        salesRep    → {id, name}         ← present and reliable
        orderTotal  → float              ← present and reliable
        orderDate   → ISO datetime string (when order was placed)
        dateCreated → ISO datetime string (when record was created in Striven)
        targetDate  → ISO datetime string or null
        contact     → {id, name} or null
        createdBy   → {id, name}
        lastUpdatedBy → {id, name} or null
        lastUpdatedDate → ISO datetime string or null
        invoiceStatus → {id, name}
        isChangeOrder → bool
        customFields  → list of {id, name, value, valueText}
        lineItems     → list (see _fmt_line_item)

    Custom field IDs confirmed from live data:
        1506 → Project Type        (e.g. "Residential New Construction", "Fireplace Enhancement")
        1507 → Product Type        (e.g. "Direct Vent", "Burner & Gas Logs")
        1508 → Project Components  (e.g. "Fireplace - Vertical Termination", "Burner & Logs")
        1515 → Target Install Date
        1516 → Target CO Date
        1517 → Permit Status       (e.g. "Needs Permit*", "No Permit Required For Job")
        1559 → Project Manager     (e.g. "Francisco Granados (Ops)", "Chris Bullock (Service)")
        1521 → Ops Install Status
        1522 → Gas Log Install Status
    """
    customer  = r.get("customer")  or r.get("Customer")  or {}
    status    = r.get("status")    or r.get("Status")    or {}
    contact   = r.get("contact")   or r.get("Contact")   or {}
    cf        = r.get("customFields") or []

    rep_id, rep_name = _resolve_rep(r, allow_created_by_fallback=True)

    return {
        # ── Identity ──────────────────────────────────────────────────────────
        "id":               r.get("id")          or r.get("Id"),
        "estimate_number":  r.get("orderNumber")  or r.get("OrderNumber"),
        "name":             r.get("orderName")    or r.get("OrderName"),
        # ── Customer ──────────────────────────────────────────────────────────
        "customer_id":      customer.get("id")   or customer.get("Id"),
        "customer_name":    customer.get("name")  or customer.get("Name"),
        # ── Status ────────────────────────────────────────────────────────────
        "status":           status.get("name")   or status.get("Name"),
        "status_id":        status.get("id")     or status.get("Id"),
        # ── Sales rep — reliable on full GET detail ───────────────────────────
        "sales_rep_name":   rep_name,            # "Unassigned" only if genuinely missing
        "sales_rep_id":     rep_id,
        "sales_rep":        rep_name,            # backward-compat alias
        # ── Financials ────────────────────────────────────────────────────────
        "total":            r.get("orderTotal")  or r.get("OrderTotal"),
        # ── Dates ────────────────────────────────────────────────────────────
        "date_created":     r.get("dateCreated") or r.get("DateCreated"),
        "order_date":       r.get("orderDate")   or r.get("OrderDate"),
        "target_date":      r.get("targetDate")  or r.get("TargetDate"),
        # ── People ───────────────────────────────────────────────────────────
        "contact_name":     contact.get("name")  or contact.get("Name"),
        # ── Workflow metadata from customFields ───────────────────────────────
        "project_type":     _extract_custom_field(cf, 1506),
        "product_type":     _extract_custom_field(cf, 1507),
        "project_components": _extract_custom_field(cf, 1508),
        "permit_status":    _extract_custom_field(cf, 1517),
        "project_manager":  _extract_custom_field(cf, 1559),
        # ── Flags ─────────────────────────────────────────────────────────────
        "is_change_order":  r.get("isChangeOrder", False),
        "invoice_status":   (r.get("invoiceStatus") or {}).get("name"),
        # ── Line items ────────────────────────────────────────────────────────
        "line_items":       [_fmt_line_item(li) for li in (r.get("lineItems") or [])],
        "line_item_count":  len(r.get("lineItems") or []),
        "total_computed":   round(sum(
            li.get("price", 0) * li.get("qty", 1)
            for li in (r.get("lineItems") or [])
        ), 2),
    }


def _fmt_line_item(li: dict) -> dict:
    """
    Normalise a single line item from a full GET detail response.

    Confirmed shape from live data:
        item        → {id, name}   (item.name is often "SKU - Description")
        description → string       (customer-facing description)
        price       → float        (unit price)
        qty         → float
        itemGroupLineItems → list | null  (sub-items if this is a group line)
    """
    item = li.get("item") or {}
    return {
        "line_item_id":   li.get("id"),
        "item_id":        item.get("id"),
        "item_name":      item.get("name"),
        "description":    li.get("description"),
        "price":          li.get("price"),
        "qty":            li.get("qty"),
        "line_total":     round((li.get("price") or 0) * (li.get("qty") or 1), 2),
        "is_group":       bool(li.get("itemGroupLineItems")),
    }


def extract_sales_rep(order: dict) -> str:
    """
    Best-effort extraction of the sales rep name from a RAW Striven order dict.
    Tries every known field pattern in priority order.
    NOT wired into the main pipeline yet — verify correct field via /debug-one-order first.
    """
    def _name(obj):
        if not obj:
            return None
        if isinstance(obj, dict):
            return obj.get("Name") or obj.get("name")
        return None

    return (
        _name(order.get("SalesRep")    or order.get("salesRep"))
        or _name(order.get("AssignedTo") or order.get("assignedTo"))
        or _name(order.get("Employee")   or order.get("employee"))
        or _name(order.get("CreatedBy")  or order.get("createdBy"))
        or "Unassigned"
    )


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


# ---------------------------------------------------------------------------
# Normaliser helpers for /striven/* endpoints
# ---------------------------------------------------------------------------

def _striven_page(resp: dict) -> tuple[int, list]:
    """
    Extract (total_count, data_list) from any Striven paginated search response.

    Striven uses both TitleCase and camelCase across different endpoints, so
    we check both.  Returns (0, []) on malformed responses rather than crashing.
    """
    total = resp.get("TotalCount") or resp.get("totalCount") or 0
    data  = resp.get("Data")       or resp.get("data")       or []
    return int(total), list(data)


def _fmt_customer(r: dict) -> dict:
    """
    Normalise a Striven customer record.

    Address fields may be nested under an 'Address' object or flat on the root —
    we check both.  This matches the shape returned by GET /v1/customers/{id}
    and POST /v1/customers/search.
    """
    addr = r.get("Address") or r.get("address") or {}
    return {
        "customer_id": _n(r, "Id",     "id"),
        "name":        _n(r, "Name",   "name"),
        "number":      _n(r, "Number", "number"),
        "phone":       _n(r, "Phone",  "phone"),
        "email":       _n(r, "Email",  "email"),
        "is_active":   _n(r, "IsActive", "isActive"),
        # Address — check nested object first, then flat keys on root
        "address_1": (
            _n(addr, "Line1", "line1", "AddressLine1", "addressLine1")
            or _n(r,   "AddressLine1", "addressLine1")
        ),
        "address_2": (
            _n(addr, "Line2", "line2", "AddressLine2", "addressLine2")
            or _n(r,   "AddressLine2", "addressLine2")
        ),
        "city":  _n(addr, "City",  "city")  or _n(r, "City",  "city"),
        "state": _n(addr, "State", "state") or _n(r, "State", "state"),
        "zip": (
            _n(addr, "Zip", "zip", "ZipCode", "zipCode", "PostalCode", "postalCode")
            or _n(r,  "Zip", "zip", "ZipCode", "zipCode", "PostalCode", "postalCode")
        ),
    }


def _fmt_employee(r: dict) -> dict:
    """
    Normalise a Striven employee record.

    Name may be a single 'Name' field or split as FirstName / LastName.
    We try the combined field first, then construct from parts.
    """
    first = _n(r, "FirstName", "firstName") or ""
    last  = _n(r, "LastName",  "lastName")  or ""
    full  = _n(r, "Name", "name") or " ".join(filter(None, [first, last])) or None
    return {
        "employee_id": _n(r, "Id",    "id"),
        "name":        full,
        "first_name":  first or None,
        "last_name":   last  or None,
        "email":       _n(r, "Email", "email"),
        "phone":       _n(r, "Phone", "phone"),
        "is_active":   _n(r, "IsActive", "isActive"),
    }


# ---------------------------------------------------------------------------
# /striven/* — Flexible read-only proxy endpoints to the Striven API
#
# DESIGN PRINCIPLES:
#   • Each endpoint maps one-to-one to a Striven API resource.
#   • Query params translate to the Striven POST/GET filter body.
#   • All responses are normalised to snake_case before returning.
#   • Pagination metadata (total_count, page, page_size) is always included.
#   • Read-only guard in before_request blocks non-GET on this prefix.
#
# IMPORTANT — salesRep on search stubs:
#   POST /v1/sales-orders/search does NOT return salesRep in its stubs.
#   Only GET /v1/sales-orders/{id} includes the full salesRep object.
#   The sales_rep field on /striven/sales-orders will always be "Unassigned".
#   Use /striven/sales-orders/{id} for accurate rep attribution.
# ---------------------------------------------------------------------------

@app.before_request
def read_only_guard():
    """
    Block all non-GET requests on the /striven/* prefix.

    Applies only to paths beginning with /striven/ so the rest of the app
    (/api/chat, /ask, /sync-estimates, etc.) is completely unaffected.
    """
    if request.path.startswith("/striven/") and request.method != "GET":
        return jsonify({"error": "Read-only API — /striven/* accepts GET requests only."}), 403


@app.route("/striven/sales-orders", methods=["GET"])
def striven_sales_orders():
    """
    Flexible search across all sales orders (estimates) in Striven.

    Internally: POST /v1/sales-orders/search

    Query params (all optional):
        page       int    — 0-based page index (default 0)
        pageSize   int    — records per page (default 25, max 100)
        status     int    — Striven status ID
                            18=Incomplete  19=Quoted  20=Pending Approval
                            22=Approved    25=In Progress  27=Completed
        date_from  str    — ISO date, filters DateCreatedRange.DateFrom
        date_to    str    — ISO date, filters DateCreatedRange.DateTo
        search     str    — partial match on order Name
        customer_id int   — filter by Striven customer ID

    NOTE: sales_rep filtering is not supported by the Striven search API.
          salesRep is also NOT present on search stubs — it only appears on
          GET /striven/sales-orders/{id}. The sales_rep field here is always null.

    Examples:
        GET /striven/sales-orders?status=22&pageSize=50
        GET /striven/sales-orders?date_from=2025-01-01&date_to=2025-12-31
        GET /striven/sales-orders?search=charleston&page=1
    """
    page      = max(0, int(request.args.get("page", 0)))
    page_size = min(int(request.args.get("pageSize", 25)), 100)

    body: dict = {"PageIndex": page, "PageSize": page_size}

    if request.args.get("status"):
        try:
            body["StatusChangedTo"] = int(request.args["status"])
        except ValueError:
            return jsonify({"error": "status must be an integer (e.g. 22 for Approved)."}), 400

    if request.args.get("customer_id"):
        try:
            body["CustomerId"] = int(request.args["customer_id"])
        except ValueError:
            return jsonify({"error": "customer_id must be an integer."}), 400

    if request.args.get("search"):
        body["Name"] = request.args["search"]

    if request.args.get("date_from") or request.args.get("date_to"):
        dr: dict = {}
        if request.args.get("date_from"):
            dr["DateFrom"] = request.args["date_from"]
        if request.args.get("date_to"):
            dr["DateTo"] = request.args["date_to"]
        body["DateCreatedRange"] = dr

    try:
        resp = striven.search_sales_orders(body)
        total, data = _striven_page(resp)
        return jsonify({
            "total_count": total,
            "page":        page,
            "page_size":   page_size,
            "count":       len(data),
            "note":        "sales_rep is null on search results — use /striven/sales-orders/{id} for full detail including rep.",
            "data": [_fmt(r) for r in data],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/sales-orders/<int:order_id>", methods=["GET"])
def striven_sales_order_detail(order_id: int):
    """
    Full detail for a single sales order including line items and custom fields.

    Internally: GET /v1/sales-orders/{id}

    This is the only endpoint that returns a reliable sales_rep value.
    Line items, product types, and all custom fields are included.

    Example:
        GET /striven/sales-orders/14843
    """
    try:
        raw    = striven.get_estimate(order_id)
        detail = _fmt_detail(raw)
        return jsonify(detail)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/tasks", methods=["GET"])
def striven_tasks():
    """
    Search tasks across the business.

    Internally: POST /v2/tasks/search

    Query params (all optional):
        page         int  — 0-based page (default 0)
        pageSize     int  — records per page (default 25, max 100)
        task_type    int  — TaskTypeId (use GET /striven/task-types for IDs)
        assigned_to  int  — AssignedToId (employee/user ID)
        status_id    int  — task status ID
        entity_id    int  — RelatedEntityId (linked sales order / estimate ID)
        date_from    str  — ISO date for DueDateRange.DateFrom
        date_to      str  — ISO date for DueDateRange.DateTo

    Examples:
        GET /striven/tasks?entity_id=14843          → all tasks for estimate 14843
        GET /striven/tasks?task_type=5&pageSize=50  → 50 tasks of a specific type
        GET /striven/tasks?assigned_to=12           → tasks assigned to employee 12
    """
    page      = max(0, int(request.args.get("page", 0)))
    page_size = min(int(request.args.get("pageSize", 25)), 100)

    body: dict = {"PageIndex": page, "PageSize": page_size}

    for param, key in (
        ("task_type",   "TaskTypeId"),
        ("assigned_to", "AssignedToId"),
        ("status_id",   "StatusId"),
        ("entity_id",   "RelatedEntityId"),
    ):
        if request.args.get(param):
            try:
                body[key] = int(request.args[param])
            except ValueError:
                return jsonify({"error": f"{param} must be an integer."}), 400

    if request.args.get("date_from") or request.args.get("date_to"):
        dr: dict = {}
        if request.args.get("date_from"):
            dr["DateFrom"] = request.args["date_from"]
        if request.args.get("date_to"):
            dr["DateTo"] = request.args["date_to"]
        body["DueDateRange"] = dr

    try:
        resp = striven.search_tasks(body)
        total, data = _striven_page(resp)
        return jsonify({
            "total_count": total,
            "page":        page,
            "page_size":   page_size,
            "count":       len(data),
            "data":        [_fmt_task(t) for t in data],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/invoices", methods=["GET"])
def striven_invoices():
    """
    Search customer invoices.

    Internally: POST /v1/invoices/search

    Query params (all optional):
        page         int  — 0-based page (default 0)
        pageSize     int  — records per page (default 25, max 100)
        status_id    int  — invoice status ID
        customer_id  int  — filter by Striven customer ID
        date_from    str  — ISO date for DateCreatedRange.DateFrom
        date_to      str  — ISO date for DateCreatedRange.DateTo
        due_from     str  — ISO date for DueDateRange.DateFrom
        due_to       str  — ISO date for DueDateRange.DateTo

    Examples:
        GET /striven/invoices?date_from=2025-01-01&date_to=2025-12-31
        GET /striven/invoices?customer_id=4521&pageSize=50
    """
    page      = max(0, int(request.args.get("page", 0)))
    page_size = min(int(request.args.get("pageSize", 25)), 100)

    body: dict = {"PageIndex": page, "PageSize": page_size}

    if request.args.get("status_id"):
        try:
            body["StatusId"] = int(request.args["status_id"])
        except ValueError:
            return jsonify({"error": "status_id must be an integer."}), 400

    if request.args.get("customer_id"):
        try:
            body["CustomerId"] = int(request.args["customer_id"])
        except ValueError:
            return jsonify({"error": "customer_id must be an integer."}), 400

    if request.args.get("date_from") or request.args.get("date_to"):
        dr: dict = {}
        if request.args.get("date_from"):
            dr["DateFrom"] = request.args["date_from"]
        if request.args.get("date_to"):
            dr["DateTo"] = request.args["date_to"]
        body["DateCreatedRange"] = dr

    if request.args.get("due_from") or request.args.get("due_to"):
        dr2: dict = {}
        if request.args.get("due_from"):
            dr2["DateFrom"] = request.args["due_from"]
        if request.args.get("due_to"):
            dr2["DateTo"] = request.args["due_to"]
        body["DueDateRange"] = dr2

    try:
        resp = striven.search_invoices(body)
        total, data = _striven_page(resp)
        return jsonify({
            "total_count": total,
            "page":        page,
            "page_size":   page_size,
            "count":       len(data),
            "data":        [_fmt_invoice(r) for r in data],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/customers", methods=["GET"])
def striven_customers():
    """
    Search customers with full address data.

    Internally: POST /v1/customers/search

    Query params (all optional):
        page      int   — 0-based page (default 0)
        pageSize  int   — records per page (default 25, max 100)
        search    str   — partial name match
        number    str   — exact customer number
        active    bool  — "true" / "false" to filter IsActive

    Examples:
        GET /striven/customers?search=charleston
        GET /striven/customers?search=smith&pageSize=50
    """
    page      = max(0, int(request.args.get("page", 0)))
    page_size = min(int(request.args.get("pageSize", 25)), 100)

    body: dict = {"PageIndex": page, "PageSize": page_size}

    if request.args.get("search"):
        body["Name"] = request.args["search"]
    if request.args.get("number"):
        body["Number"] = request.args["number"]
    if request.args.get("active") is not None:
        body["IsActive"] = request.args["active"].lower() == "true"

    try:
        resp = striven.search_customers_full(body)
        total, data = _striven_page(resp)
        return jsonify({
            "total_count": total,
            "page":        page,
            "page_size":   page_size,
            "count":       len(data),
            "data":        [_fmt_customer(r) for r in data],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/employees", methods=["GET"])
def striven_employees():
    """
    Return a paginated list of all employees.

    Internally: GET /v1/employees

    Query params (all optional):
        page      int  — 0-based page (default 0)
        pageSize  int  — records per page (default 100, max 200)

    Example:
        GET /striven/employees
        GET /striven/employees?page=1&pageSize=50
    """
    page      = max(0, int(request.args.get("page", 0)))
    page_size = min(int(request.args.get("pageSize", 100)), 200)

    try:
        resp = striven.get_employees(page_index=page, page_size=page_size)
        # GET /v1/employees returns a raw list, not the standard paginated dict
        if isinstance(resp, list):
            data  = resp
            total = len(data)
        else:
            total, data = _striven_page(resp)
        return jsonify({
            "total_count": total,
            "page":        page,
            "page_size":   page_size,
            "count":       len(data),
            "data":        [_fmt_employee(r) for r in data],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/striven/task-types", methods=["GET"])
def striven_task_types():
    """
    Return all task types — useful for looking up TaskTypeId values before
    querying /striven/tasks?task_type=<id>.

    Internally: GET /v2/tasks/types  (no pagination, small static list)

    Example:
        GET /striven/task-types
    """
    try:
        data = striven.get_task_types()
        # Normalize whatever shape the API returns
        items = data if isinstance(data, list) else (data.get("Data") or data.get("data") or [data])
        return jsonify({
            "count": len(items),
            "data": [
                {
                    "task_type_id": _n(t, "Id", "id"),
                    "name":         _n(t, "Name", "name"),
                }
                for t in items
                if isinstance(t, dict)
            ],
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


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


def _parse_date_str(s):
    """
    Parse an ISO date string to a naive datetime. Returns None if unparseable.
    Handles both '2025-01-15T14:30:00' and '2025-01-15' forms.
    Module-level helper shared by all intelligence modules.
    """
    from datetime import datetime
    if not s:
        return None
    s = str(s)[:19]
    if "T" in s:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except ValueError:
        return None


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

    # ── Task classification ───────────────────────────────────────────────────
    #
    # Preview detection uses Striven's actual task naming patterns.
    # Three tiers are recognised:
    #
    #   STRONG  — task_type contains "preview" or "site inspections/preview"
    #             OR task name starts with "preview", contains "[preview]",
    #             "preview-", or "site preview"
    #             → counts as the primary preview task for the job
    #
    #   ADMIN   — task name contains "update preview status"
    #             → administrative/support task; does NOT count as the primary
    #             site preview unless no strong match exists for this job
    #
    #   INSTALL — task name or type contains "install" / "installation"
    #
    # Valid preview statuses: Open, On Hold (both still count as existing)
    # A cancelled or deleted preview task is intentionally ignored.

    PREVIEW_VALID_STATUSES = {"open", "on hold"}

    def _is_preview_valid_status(task: dict) -> bool:
        status = (task.get("status") or "").strip().lower()
        # Treat unknown/blank status as valid — better to count than to drop
        return status == "" or status in PREVIEW_VALID_STATUSES

    def _classify_preview(task: dict) -> str:
        """
        Return 'strong', 'admin', or 'none' for this task's preview signal.

        Strong signals (any one match = strong):
          • task_type contains "preview" (covers "Site Inspections/Preview" etc.)
          • task_name starts with "preview"
          • task_name contains "[preview]"
          • task_name contains "preview-"
          • task_name contains "site preview"

        Weak / admin signal:
          • task_name contains "update preview status"
        """
        name = (task.get("name")      or "").strip().lower()
        ttype = (task.get("task_type") or "").strip().lower()

        # Admin signal — check first so it cannot accidentally match strong
        if "update preview status" in name:
            return "admin"

        # Strong signals
        if "preview" in ttype:
            return "strong"
        if name.startswith("preview"):
            return "strong"
        if "[preview]" in name:
            return "strong"
        if "preview-" in name:
            return "strong"
        if "site preview" in name:
            return "strong"

        return "none"

    def _classify_install(task: dict) -> bool:
        """Return True if this task is an install task."""
        text = " ".join([
            (task.get("name")      or "").lower(),
            (task.get("task_type") or "").lower(),
        ])
        return "install" in text or "installation" in text

    def _pick_preview_task(tasks: list[dict]) -> tuple[dict | None, str]:
        """
        Return (best_preview_task, tier) where tier is 'strong' or 'admin'.
        Prefers strong signals. Falls back to admin only if no strong match exists.
        Only considers tasks whose status is Open or On Hold.
        """
        strong_candidates = [
            t for t in tasks
            if _classify_preview(t) == "strong" and _is_preview_valid_status(t)
        ]
        if strong_candidates:
            # Pick earliest by date_created
            strong_candidates.sort(key=lambda t: t.get("date_created") or "")
            return strong_candidates[0], "strong"

        admin_candidates = [
            t for t in tasks
            if _classify_preview(t) == "admin" and _is_preview_valid_status(t)
        ]
        if admin_candidates:
            admin_candidates.sort(key=lambda t: t.get("date_created") or "")
            return admin_candidates[0], "admin"

        return None, "none"

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

    # ── Job type → preview expectation ────────────────────────────────────────
    #
    # Preview tasks are ONLY required for remodel and new construction jobs.
    # Enhancement, repair, service, warranty, and callback jobs do NOT need one.
    #
    # Matching priority:
    #   1. Structured job_type field (most reliable — direct Striven lookup field)
    #   2. Structured category field
    #   3. Keywords in estimate name (fallback when type fields are absent/null)
    #
    # Exempt keywords take priority over required keywords: a job whose signal
    # text contains both "remodel" and "service" is conservatively marked exempt.
    #
    # Unknown / unclassifiable jobs default to NOT required — we must never
    # inflate the failure rate by flagging jobs whose type we cannot determine.

    PREVIEW_REQUIRED_KW = {
        "residential remodel",
        "residential new construction",
        "commercial remodel",
        "commercial new construction",
        "remodel",
        "new construction",
        "new const",
    }
    PREVIEW_EXEMPT_KW = {
        "fireplace enhancement",
        "enhancement",
        "chimney repair",
        "chimney",
        "service",
        "warranty",
        "callback",
        "call back",
    }

    def _requires_preview(est: dict) -> tuple[bool, str]:
        """
        Return (preview_required: bool, reason: str).

        Inspects job_type → category → estimate name (in that order).
        Exempt keywords take priority over required keywords.
        Returns (False, "unknown:no_match") when the job type cannot be
        determined — so unclassifiable jobs are never counted as failures.
        """
        type_text = (est.get("job_type") or "").strip().lower()
        cat_text  = (est.get("category") or "").strip().lower()
        name_text = (est.get("name")     or "").strip().lower()

        # Use structured fields when available; fall back to estimate name
        signal = " | ".join(filter(None, [type_text, cat_text])) or name_text

        # Exempt check runs first — conservative, prevents over-flagging
        for kw in PREVIEW_EXEMPT_KW:
            if kw in signal:
                return False, f"exempt:{kw}"

        # Required check
        for kw in PREVIEW_REQUIRED_KW:
            if kw in signal:
                return True, f"required:{kw}"

        # Cannot classify — do NOT count as missing preview
        return False, "unknown:no_match"

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
        # On the first batch of the first status, dump all raw field keys so
        # we can verify which type/category field name Striven is using.
        if data and not all_estimates:
            sample = data[0]
            print(
                f"[pipeline_analysis] RAW ESTIMATE FIELD KEYS: {sorted(sample.keys())}",
                flush=True,
            )
            print(
                f"[pipeline_analysis] SAMPLE type/category fields: "
                f"Type={sample.get('Type')!r} type={sample.get('type')!r} "
                f"SalesOrderType={sample.get('SalesOrderType')!r} "
                f"JobType={sample.get('JobType')!r} "
                f"Category={sample.get('Category')!r} category={sample.get('category')!r} "
                f"Name={str(sample.get('Name') or '')[:60]!r}",
                flush=True,
            )
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

        # Classify tasks using tiered preview logic + install detection
        preview_task, preview_tier = _pick_preview_task(tasks)
        install_tasks = [t for t in tasks if _classify_install(t)]
        install_task  = install_tasks[0] if install_tasks else None

        # Determine whether this job type requires a preview task at all
        preview_required, preview_reason = _requires_preview(est)

        # Log full classification for every estimate — visible in Render logs
        print(
            f"[pipeline_analysis] est={est_id} "
            f"job_type={est.get('job_type')!r} category={est.get('category')!r} "
            f"name={str(est.get('name') or '')[:50]!r} "
            f"preview_required={preview_required} reason={preview_reason} "
            f"tasks={len(tasks)} preview_tier={preview_tier} "
            f"install={'yes' if install_task else 'no'}",
            flush=True,
        )

        # Compute timeline gaps (regardless of requirement — useful data even for exempt jobs)
        days_to_preview = (
            days_between(date_approved, preview_task.get("date_created"))
            if preview_task else None
        )
        install_ref = (
            install_task.get("due_date") or install_task.get("date_created")
            if install_task else None
        )
        days_to_install = days_between(date_approved, install_ref) if install_ref else None

        # Flag issues — preview issues ONLY raised when preview is required for this job type
        issues: list[str] = []
        if preview_required:
            if preview_tier == "none":
                issues.append("no_preview_task")
            elif preview_tier == "admin":
                # Has only an admin/status-update task — not a real site preview
                issues.append("preview_admin_only")
            elif days_to_preview is not None and days_to_preview > 3:
                issues.append(f"preview_late_{days_to_preview}d")
        if not install_task:
            issues.append("no_install_task")

        job_results.append({
            "estimate_id":       est_id,
            "estimate_number":   est.get("estimate_number"),
            "customer_name":     est.get("customer_name"),
            "sales_rep":         est.get("sales_rep") or "Unassigned",
            "status":            est.get("status"),
            "job_type":          est.get("job_type"),
            "category":          est.get("category"),
            "total":             est.get("total"),
            "date_approved":     date_approved,
            "preview_required":  preview_required,
            "preview_reason":    preview_reason,
            "preview_task":      preview_task,
            "preview_tier":      preview_tier,
            "install_task":      install_task,
            "days_to_preview":   days_to_preview,
            "days_to_install":   days_to_install,
            "all_task_count":    len(tasks),
            "issues":            issues,
            "has_issues":        bool(issues),
        })

    # ── Step 3: Summary stats ─────────────────────────────────────────────────
    # Split the job list into preview-required vs exempt FIRST.
    # All preview issue counts are computed ONLY within the required set.
    preview_required_jobs = [j for j in job_results if     j["preview_required"]]
    preview_exempt_jobs   = [j for j in job_results if not j["preview_required"]]
    unclassified_jobs     = [j for j in job_results if j["preview_reason"].startswith("unknown")]

    total_req  = len(preview_required_jobs)
    total_ex   = len(preview_exempt_jobs)

    no_preview    = [j for j in preview_required_jobs if "no_preview_task"    in j["issues"]]
    preview_admin = [j for j in preview_required_jobs if "preview_admin_only" in j["issues"]]
    preview_late  = [j for j in preview_required_jobs if any("preview_late"   in i for i in j["issues"])]
    no_install    = [j for j in job_results           if "no_install_task"    in j["issues"]]
    has_issues    = [j for j in job_results           if j["has_issues"]]

    # Average timings — only from preview-required jobs where the task exists
    dtp = [j["days_to_preview"] for j in preview_required_jobs if j["days_to_preview"] is not None]
    dti = [j["days_to_install"] for j in job_results           if j["days_to_install"] is not None]
    avg_days_to_preview = round(sum(dtp) / len(dtp), 1) if dtp else None
    avg_days_to_install = round(sum(dti) / len(dti), 1) if dti else None

    # ── Step 4: By-rep breakdown ──────────────────────────────────────────────
    rep_stats: dict[str, dict] = {}
    for j in job_results:
        rep = j.get("sales_rep") or "Unassigned"
        if rep not in rep_stats:
            rep_stats[rep] = {
                "rep": rep, "count": 0,
                "preview_required": 0,
                "no_preview": 0, "no_install": 0, "has_issues": 0,
            }
        rep_stats[rep]["count"] += 1
        if j["preview_required"]:                rep_stats[rep]["preview_required"] += 1
        if "no_preview_task"  in j["issues"]:    rep_stats[rep]["no_preview"]  += 1
        if "no_install_task"  in j["issues"]:    rep_stats[rep]["no_install"]  += 1
        if j["has_issues"]:                      rep_stats[rep]["has_issues"]  += 1

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
        "total_analyzed":         total_analyzed,
        "total_with_issues":      len(has_issues),
        "total_requiring_preview": total_req,
        "total_exempt_preview":    total_ex,
        "total_unclassified":      len(unclassified_jobs),
        "summary": {
            # Preview stats — percentages are out of the REQUIRED set only
            "requiring_preview":     total_req,
            "no_preview_task":       len(no_preview),
            "pct_no_preview":        round(len(no_preview)    / total_req  * 100) if total_req  else 0,
            "preview_admin_only":    len(preview_admin),
            "pct_preview_admin":     round(len(preview_admin) / total_req  * 100) if total_req  else 0,
            "preview_task_late":     len(preview_late),
            "pct_preview_late":      round(len(preview_late)  / total_req  * 100) if total_req  else 0,
            # Install stats — all jobs are expected to have an install task
            "no_install_task":       len(no_install),
            "pct_no_install":        round(len(no_install)    / total_analyzed * 100) if total_analyzed else 0,
            # Timing
            "avg_days_to_preview":   avg_days_to_preview,
            "avg_days_to_install":   avg_days_to_install,
        },
        "by_sales_rep": by_rep,
        "problem_jobs": problem_jobs,
        "all_jobs": job_results,
        "note": (
            f"Analysed {total_analyzed} estimates in {elapsed}s. "
            f"{total_req} require a preview task (remodel/new construction); "
            f"{total_ex} are exempt (enhancement/repair/service/warranty). "
            f"{len(unclassified_jobs)} could not be classified — not counted as failures. "
            "Check Render logs for 'RAW ESTIMATE FIELD KEYS' to verify type field availability. "
            "Preview detection: task_type contains 'preview', or name starts with 'preview', "
            "contains '[preview]', 'preview-', or 'site preview'. "
            "Install detection: name or type contains 'install'."
        ),
    }



# ---------------------------------------------------------------------------
# Intelligence Module 1 — Stuck Jobs Analysis
# ---------------------------------------------------------------------------

def _analyze_stuck_jobs(limit: int = 50) -> dict:
    """
    Identify jobs that are stuck across Quoted, Approved, and In Progress statuses.

    Stuck thresholds (business-defined):
      Quoted      > 7 days  — no customer follow-up
      Approved    > 5 days  — no scheduling action taken
      In Progress > 10 days — in progress but no install task scheduled

    For In Progress jobs: checks task list for install keyword (costs 1 API call/job).
    Capped at 25 in-progress estimates to control latency.

    Returns structured dict ready for Claude interpretation.
    """
    import time as _time
    from datetime import datetime

    t_start = _time.monotonic()
    today   = datetime.utcnow()

    stuck_jobs:   list[dict] = []
    total_checked = 0

    # ── Quoted: flag anything older than 7 days ───────────────────────────────
    print(f"[stuck_jobs] Fetching Quoted (19) estimates...", flush=True)
    raw_q  = striven.search_sales_orders({"PageIndex": 0, "PageSize": min(limit, 50), "StatusChangedTo": 19})
    for r in (raw_q.get("data") or []):
        total_checked += 1
        est  = _fmt(r)
        ref  = _parse_date_str(est.get("date_created"))
        if ref is None:
            continue
        days = (today - ref).days
        if days > 7:
            stuck_jobs.append({
                "id":               str(est.get("id") or ""),
                "estimate_number":  est.get("estimate_number"),
                "customer":         est.get("customer_name") or "Unknown",
                "sales_rep":        est.get("sales_rep") or "Unassigned",
                "status":           "Quoted",
                "days_in_status":   days,
                "total":            est.get("total"),
                "issue":            f"Quote not followed up in {days} days — needs customer contact",
            })

    # ── Approved: flag anything older than 5 days ─────────────────────────────
    print(f"[stuck_jobs] Fetching Approved (22) estimates...", flush=True)
    raw_a  = striven.search_sales_orders({"PageIndex": 0, "PageSize": min(limit, 50), "StatusChangedTo": 22})
    for r in (raw_a.get("data") or []):
        total_checked += 1
        est  = _fmt(r)
        ref  = _parse_date_str(est.get("date_approved") or est.get("date_created"))
        if ref is None:
            continue
        days = (today - ref).days
        if days > 5:
            stuck_jobs.append({
                "id":               str(est.get("id") or ""),
                "estimate_number":  est.get("estimate_number"),
                "customer":         est.get("customer_name") or "Unknown",
                "sales_rep":        est.get("sales_rep") or "Unassigned",
                "status":           "Approved",
                "days_in_status":   days,
                "total":            est.get("total"),
                "issue":            f"Approved {days} days ago — no scheduling action taken",
            })

    # ── In Progress: flag > 10 days with no install task ─────────────────────
    # Caps at 25 in-progress estimates to control the per-job task API call cost.
    print(f"[stuck_jobs] Fetching In Progress (25) estimates...", flush=True)
    raw_ip = striven.search_sales_orders({"PageIndex": 0, "PageSize": min(limit, 25), "StatusChangedTo": 25})
    for r in (raw_ip.get("data") or []):
        total_checked += 1
        est  = _fmt(r)
        ref  = _parse_date_str(est.get("date_approved") or est.get("date_created"))
        if ref is None:
            continue
        days = (today - ref).days
        if days <= 10:
            continue

        # Only cost the task API call for actually old in-progress jobs
        est_id      = est.get("id")
        has_install = False
        try:
            task_raw  = striven.search_tasks({"RelatedEntityId": est_id, "PageSize": 25})
            tasks     = [_fmt_task(t) for t in (task_raw.get("data") or [])]
            has_install = any(
                "install" in (t.get("name")      or "").lower() or
                "install" in (t.get("task_type") or "").lower()
                for t in tasks
            )
        except Exception as exc:
            print(f"[stuck_jobs] WARNING: task fetch failed for est {est_id}: {exc}", flush=True)

        if not has_install:
            stuck_jobs.append({
                "id":               str(est_id or ""),
                "estimate_number":  est.get("estimate_number"),
                "customer":         est.get("customer_name") or "Unknown",
                "sales_rep":        est.get("sales_rep") or "Unassigned",
                "status":           "In Progress",
                "days_in_status":   days,
                "total":            est.get("total"),
                "issue":            f"In progress {days} days — no install task scheduled",
            })

    # Sort worst-first
    stuck_jobs.sort(key=lambda x: x["days_in_status"], reverse=True)
    elapsed = round(_time.monotonic() - t_start, 2)

    print(
        f"[stuck_jobs] complete — checked={total_checked} "
        f"stuck={len(stuck_jobs)} elapsed={elapsed}s",
        flush=True,
    )

    return {
        "analysis_type":   "stuck_jobs",
        "total_checked":   total_checked,
        "stuck_count":     len(stuck_jobs),
        "stuck_jobs":      stuck_jobs,
        "thresholds": {
            "quoted_days":      7,
            "approved_days":    5,
            "in_progress_days": 10,
        },
        "elapsed_seconds": elapsed,
    }


# ---------------------------------------------------------------------------
# Intelligence Module 2 — Install Scheduling Gaps
# ---------------------------------------------------------------------------

def _analyze_install_gaps(limit: int = 40) -> dict:
    """
    Find approved or in-progress jobs that have no install task scheduled.

    Fetches up to `limit` estimates across Approved (22) and In Progress (25),
    then checks each for an install task. Returns every job missing one,
    sorted by days since approval (oldest first).

    This is the definitive install-gap check — more focused than the full
    pipeline analysis because it only answers one question: "Which jobs
    have no install date set?"
    """
    import time as _time
    from datetime import datetime

    t_start = _time.monotonic()
    today   = datetime.utcnow()
    per_status = max(limit // 2, 10)

    # Fetch approved and in-progress estimates
    all_estimates: list[dict] = []
    for sid in [22, 25]:
        raw  = striven.search_sales_orders({
            "PageIndex": 0,
            "PageSize":  per_status,
            "StatusChangedTo": sid,
        })
        data = raw.get("data") or []
        all_estimates.extend([_fmt(r) for r in data])
        print(f"[install_gaps] status={sid} → {len(data)} estimates", flush=True)

    total_checked = len(all_estimates)
    gaps: list[dict] = []

    for est in all_estimates:
        est_id      = est.get("id")
        has_install = False
        try:
            task_raw  = striven.search_tasks({"RelatedEntityId": est_id, "PageSize": 25})
            tasks     = [_fmt_task(t) for t in (task_raw.get("data") or [])]
            has_install = any(
                "install" in (t.get("name")      or "").lower() or
                "install" in (t.get("task_type") or "").lower()
                for t in tasks
            )
        except Exception as exc:
            print(f"[install_gaps] WARNING: task fetch failed for est {est_id}: {exc}", flush=True)

        if not has_install:
            ref  = _parse_date_str(est.get("date_approved") or est.get("date_created"))
            days = (today - ref).days if ref else None
            gaps.append({
                "id":                  str(est_id or ""),
                "estimate_number":     est.get("estimate_number"),
                "customer":            est.get("customer_name") or "Unknown",
                "sales_rep":           est.get("sales_rep") or "Unassigned",
                "status":              est.get("status") or "Unknown",
                "days_since_approval": days,
                "total":               est.get("total"),
            })

    # Oldest approval first — highest urgency
    gaps.sort(key=lambda x: x.get("days_since_approval") or 0, reverse=True)
    elapsed = round(_time.monotonic() - t_start, 2)

    print(
        f"[install_gaps] complete — checked={total_checked} "
        f"missing_install={len(gaps)} elapsed={elapsed}s",
        flush=True,
    )

    return {
        "analysis_type":        "install_gaps",
        "total_checked":        total_checked,
        "missing_install_count": len(gaps),
        "jobs_without_install": gaps,
        "elapsed_seconds":      elapsed,
    }


# ---------------------------------------------------------------------------
# Intelligence Module 3 — Sales Rep Pipeline Health
# ---------------------------------------------------------------------------

def _analyze_rep_pipeline(limit: int = 30) -> dict:
    """
    Sales rep pipeline health — group operational issues by rep.

    Runs the full pipeline analysis (approved + in-progress jobs, task checks,
    timeline gaps) and restructures the output as a per-rep accountability view:
      • How many jobs does each rep own?
      • How many are stuck (have issues)?
      • How many have no install scheduled?
      • What is the average days from approval to install?

    Reps are sorted by issue count (worst first) so the most urgent problems
    are always at the top.
    """
    import time as _time

    t_start  = _time.monotonic()

    # Reuse the full pipeline analysis — avoids duplicate API calls
    pipeline = _run_pipeline_analysis(limit=limit, status_ids=[22, 25])
    all_jobs = pipeline.get("all_jobs") or []

    # Build per-rep aggregates
    rep_data: dict[str, dict] = {}
    for j in all_jobs:
        rep = j.get("sales_rep") or "Unassigned"
        if rep not in rep_data:
            rep_data[rep] = {
                "rep":               rep,
                "total_jobs":        0,
                "stuck_jobs":        0,       # jobs with any issue
                "missing_install":   0,
                "days_to_install_list": [],   # raw list for avg computation
            }
        rd = rep_data[rep]
        rd["total_jobs"] += 1

        if j.get("has_issues"):
            rd["stuck_jobs"] += 1

        if "no_install_task" in (j.get("issues") or []):
            rd["missing_install"] += 1

        dti = j.get("days_to_install")
        if dti is not None:
            rd["days_to_install_list"].append(dti)

    # Compute averages and clean up raw lists
    rep_summary: list[dict] = []
    for rep, rd in rep_data.items():
        dti_list = rd.pop("days_to_install_list")
        avg = round(sum(dti_list) / len(dti_list), 1) if dti_list else None
        rep_summary.append({
            "rep":                  rd["rep"],
            "total_jobs":           rd["total_jobs"],
            "stuck_jobs":           rd["stuck_jobs"],
            "missing_install":      rd["missing_install"],
            "avg_days_to_install":  avg,
        })

    # Worst reps first (most stuck jobs, then most missing install)
    rep_summary.sort(key=lambda x: (-x["stuck_jobs"], -x["missing_install"]))

    # Overall health metrics
    total_jobs        = len(all_jobs)
    total_stuck       = sum(r["stuck_jobs"]       for r in rep_summary)
    total_no_install  = sum(r["missing_install"]   for r in rep_summary)

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[rep_pipeline] complete — {len(rep_summary)} reps, "
        f"{total_jobs} jobs, {total_stuck} stuck, "
        f"{total_no_install} missing install, {elapsed}s",
        flush=True,
    )

    return {
        "analysis_type":     "rep_pipeline",
        "total_jobs":        total_jobs,
        "total_reps":        len(rep_summary),
        "total_stuck":       total_stuck,
        "total_no_install":  total_no_install,
        "rep_summary":       rep_summary,
        "elapsed_seconds":   elapsed,
    }


# ---------------------------------------------------------------------------
# Intelligence Module 4 — Weekly Pipeline Review
# ---------------------------------------------------------------------------

def _analyze_weekly_pipeline(limit: int = 40) -> dict:
    """
    Full weekly sales pipeline review — replaces the Excel macro report.

    Fetches Quoted (19), Approved (22), and In Progress (25) estimates in
    parallel, computes days in stage, identifies stuck jobs, checks install
    tasks for approved/in-progress jobs (capped to control latency), groups
    everything by sales rep, and surfaces the top 10 highest-risk jobs.

    Returns a structured dict with four top-level sections:
      pipeline_summary — counts, averages, and threshold-breach counts per stage
      rep_summary      — per-rep breakdown (total, quoted, approved, in_progress,
                         stuck_jobs, missing_install)
      top_risks        — up to 10 jobs sorted by urgency (days over threshold)
      totals           — headline numbers (total jobs, value, stuck %, etc.)
    """
    import time as _time
    from datetime import datetime
    from concurrent.futures import ThreadPoolExecutor

    t_start = _time.monotonic()
    today   = datetime.utcnow()

    THRESHOLDS  = {19: 7, 22: 5, 25: 10}
    STATUS_NAMES = {19: "Quoted", 22: "Approved", 25: "In Progress"}

    # ── Step 1: Fetch all three status groups in parallel ─────────────────────
    page = min(limit // 3, 25)

    def _fetch(sid):
        try:
            raw = striven.search_sales_orders({
                "PageIndex": 0, "PageSize": page, "StatusChangedTo": sid,
            })
            return sid, raw.get("data") or []
        except Exception as exc:
            print(f"[weekly_pipeline] WARNING status={sid}: {exc}", flush=True)
            return sid, []

    with ThreadPoolExecutor(max_workers=3) as pool:
        fetched = list(pool.map(_fetch, [19, 22, 25]))

    # ── Step 2: Normalise estimates ───────────────────────────────────────────
    all_jobs: list[dict] = []
    for sid, data in fetched:
        status_name = STATUS_NAMES[sid]
        for r in data:
            est     = _fmt(r)
            ref_str = est.get("date_approved") if sid == 22 else est.get("date_created")
            ref     = _parse_date_str(ref_str)
            days    = (today - ref).days if ref else 0
            all_jobs.append({
                "id":           str(est.get("estimate_number") or est.get("id") or ""),
                "estimate_id":  est.get("id"),
                "customer":     est.get("customer_name") or "Unknown",
                "rep":          est.get("sales_rep") or "Unassigned",
                "status":       status_name,
                "status_id":    sid,
                "days_in_stage": days,
                "threshold":    THRESHOLDS[sid],
                "is_stuck":     days > THRESHOLDS[sid],
                "total":        est.get("total") or 0,
                "has_install":  None,   # filled below for approved/in-progress
            })

    # ── Step 3: Install-task check (approved + in-progress, capped at 20) ─────
    # Each task check costs one Striven API call.  Cap at 20 to keep the
    # total latency acceptable; remaining jobs show has_install=None (unknown).
    INSTALL_CAP = 20
    to_check = [j for j in all_jobs if j["status_id"] in (22, 25)][:INSTALL_CAP]

    for job in to_check:
        est_id = job["estimate_id"]
        if est_id is None:
            job["has_install"] = False
            continue
        try:
            task_raw  = striven.search_tasks({"RelatedEntityId": est_id, "PageSize": 25})
            tasks     = [_fmt_task(t) for t in (task_raw.get("data") or [])]
            job["has_install"] = any(
                "install" in (t.get("name")      or "").lower() or
                "install" in (t.get("task_type") or "").lower()
                for t in tasks
            )
        except Exception:
            job["has_install"] = False  # conservative

    # ── Step 4: Pipeline summary per stage ────────────────────────────────────
    pipeline_summary: dict = {}
    for sname, skey in [
        ("Quoted",      "quoted"),
        ("Approved",    "approved"),
        ("In Progress", "in_progress"),
    ]:
        group   = [j for j in all_jobs if j["status"] == sname]
        d_list  = [j["days_in_stage"] for j in group]
        v_list  = [j["total"] for j in group if j["total"]]
        pipeline_summary[skey] = {
            "count":          len(group),
            "avg_days":       round(sum(d_list) / len(d_list), 1) if d_list else 0,
            "over_threshold": sum(1 for j in group if j["is_stuck"]),
            "total_value":    sum(v_list),
        }

    # ── Step 5: Rep summary ───────────────────────────────────────────────────
    rep_data: dict[str, dict] = {}
    for j in all_jobs:
        rep = j["rep"]
        if rep not in rep_data:
            rep_data[rep] = {
                "rep":             rep,
                "total_pipeline":  0,
                "quoted":          0,
                "approved":        0,
                "in_progress":     0,
                "stuck_jobs":      0,
                "missing_install": 0,
            }
        rd = rep_data[rep]
        rd["total_pipeline"] += 1
        if   j["status"] == "Quoted":       rd["quoted"]      += 1
        elif j["status"] == "Approved":     rd["approved"]    += 1
        elif j["status"] == "In Progress":  rd["in_progress"] += 1
        if   j["is_stuck"]:                 rd["stuck_jobs"]  += 1
        if   j["has_install"] is False:     rd["missing_install"] += 1

    rep_summary = sorted(
        rep_data.values(),
        key=lambda r: (-r["stuck_jobs"], -r["total_pipeline"]),
    )

    # ── Step 6: Top risks ─────────────────────────────────────────────────────
    # Stuck jobs sorted by days-over-threshold (worst first)
    stuck_jobs      = [j for j in all_jobs if j["is_stuck"]]
    missing_install = [j for j in all_jobs if j["has_install"] is False
                       and j["status_id"] in (22, 25)]

    RISK_LABEL = {
        "Quoted":      "Quote {days}d old — {over}d over threshold, needs follow-up",
        "Approved":    "Approved {days}d ago — no scheduling action taken",
        "In Progress": "In progress {days}d — no install task confirmed",
    }
    risk_list: list[dict] = []
    seen_ids: set = set()

    for j in sorted(stuck_jobs, key=lambda x: -x["days_in_stage"])[:7]:
        seen_ids.add(j["id"])
        over = j["days_in_stage"] - j["threshold"]
        risk_list.append({
            "id":           j["id"],
            "customer":     j["customer"],
            "rep":          j["rep"],
            "status":       j["status"],
            "days_in_stage": j["days_in_stage"],
            "total":        j["total"],
            "risk":         RISK_LABEL[j["status"]].format(
                                days=j["days_in_stage"], over=over),
        })

    for j in sorted(missing_install, key=lambda x: -x["days_in_stage"])[:5]:
        if j["id"] not in seen_ids:
            risk_list.append({
                "id":           j["id"],
                "customer":     j["customer"],
                "rep":          j["rep"],
                "status":       j["status"],
                "days_in_stage": j["days_in_stage"],
                "total":        j["total"],
                "risk":         "No install task scheduled",
            })

    # ── Step 7: Totals ────────────────────────────────────────────────────────
    all_values = [j["total"] for j in all_jobs if j["total"]]
    totals = {
        "total_jobs":            len(all_jobs),
        "total_pipeline_value":  sum(all_values),
        "stuck_count":           len(stuck_jobs),
        "stuck_pct":             round(len(stuck_jobs) / len(all_jobs) * 100) if all_jobs else 0,
        "missing_install_count": len(missing_install),
        "install_checked":       len(to_check),
    }

    elapsed = round(_time.monotonic() - t_start, 2)
    print(
        f"[weekly_pipeline] complete — {len(all_jobs)} jobs, "
        f"{len(stuck_jobs)} stuck ({totals['stuck_pct']}%), "
        f"{len(missing_install)} missing install, {elapsed}s",
        flush=True,
    )

    return {
        "analysis_type":    "weekly_pipeline",
        "pipeline_summary": pipeline_summary,
        "rep_summary":      rep_summary,
        "top_risks":        risk_list[:10],
        "totals":           totals,
        "elapsed_seconds":  elapsed,
    }


def _slim_analysis_result(result: dict) -> dict:
    """
    Compress analysis tool results before sending to Claude.
    Keeps only high-signal fields, slices job lists to top 10, trims strings to 80 chars.
    Reduces payload size by 70-90% without losing analytical value.
    Regular (non-analysis) tool results pass through unchanged.
    """
    _MAX_LIST = 10
    _MAX_STR  = 80

    def _t(v):
        return str(v)[:_MAX_STR] if isinstance(v, str) else v

    def _pick(obj: dict, keys: list) -> dict:
        return {k: _t(obj[k]) for k in keys if obj.get(k) not in (None, "", [])}

    atype = result.get("analysis_type")

    if atype == "stuck_jobs":
        return {
            "analysis_type": atype,
            "total_checked": result.get("total_checked"),
            "stuck_count":   result.get("stuck_count"),
            "thresholds":    result.get("thresholds"),
            "top_stuck": [
                _pick(j, ["id", "customer", "sales_rep", "status",
                           "days_in_status", "total", "issue"])
                for j in result.get("stuck_jobs", [])[:_MAX_LIST]
            ],
        }

    if atype == "install_gaps":
        return {
            "analysis_type":         atype,
            "total_checked":         result.get("total_checked"),
            "missing_install_count": result.get("missing_install_count"),
            "top_missing": [
                _pick(j, ["id", "customer", "sales_rep", "status",
                           "days_since_approval", "total"])
                for j in result.get("jobs_without_install", [])[:_MAX_LIST]
            ],
        }

    if atype == "rep_pipeline":
        return {
            "analysis_type":    atype,
            "total_jobs":       result.get("total_jobs"),
            "total_reps":       result.get("total_reps"),
            "total_stuck":      result.get("total_stuck"),
            "total_no_install": result.get("total_no_install"),
            "rep_summary": [
                _pick(r, ["rep", "total_jobs", "stuck_jobs",
                           "missing_install", "avg_days_to_install"])
                for r in result.get("rep_summary", [])[:_MAX_LIST]
            ],
        }

    if atype == "weekly_pipeline":
        return {
            "analysis_type":    atype,
            "totals":           result.get("totals"),
            "pipeline_summary": result.get("pipeline_summary"),
            "rep_summary": [
                _pick(r, ["rep", "total_pipeline", "quoted", "approved",
                           "in_progress", "stuck_jobs", "missing_install"])
                for r in result.get("rep_summary", [])[:_MAX_LIST]
            ],
            "top_risks": [
                _pick(r, ["id", "customer", "status", "days", "risk", "sales_rep"])
                for r in result.get("top_risks", [])[:_MAX_LIST]
            ],
        }

    return result  # non-analysis tools pass through unchanged


def _slim_order(o: dict) -> dict:
    """
    Strip a normalised (_fmt) order dict down to the 6 fields Claude needs.
    Reduces per-order token cost from ~120 tokens → ~30 tokens.
    Applied to search_estimates and high_value_estimates results before Claude sees them.
    """
    return {
        "id":       o.get("id"),
        "number":   o.get("estimate_number"),
        "customer": o.get("customer_name"),
        "status":   o.get("status"),
        "rep":      o.get("sales_rep"),
        "total":    o.get("total"),
    }


def _slim_gas_log_for_claude(result: dict) -> dict:
    """
    Trim a full gas-log audit result to the minimum Claude needs for summarization.
    Full result can have 200+ matches × ~4 fields each ≈ 6,000+ chars.
    Slim version: counts + top 10 examples ≈ 400 chars (~100 tokens).
    """
    return {
        "analysis_type":      "gas_log_audit",
        "total_checked":      result.get("total_checked"),
        "gas_log_installs":   result.get("gas_log_installs"),
        "missing_removal_fee": result.get("missing_removal_fee"),
        "top_examples": [
            {
                "number":   m.get("estimate_number"),
                "customer": m.get("customer_name"),
                "rep":      m.get("sales_rep"),
                "url":      m.get("url"),
            }
            for m in (result.get("matches") or [])[:10]
        ],
    }


def _execute_tool(name: str, tool_input: dict) -> dict:
    """Map a Claude tool call to the live Striven API. All operations are read-only."""
    try:
        # ── count_estimates ──────────────────────────────────────────────────
        # POST /v1/sales-orders/search with pageSize=1.
        # We only need totalCount — no records are read.
        # NO fallback, NO Supabase, NO cache. Live Striven only.
        if name == "count_estimates":
            cached = _cache_get("count_estimates")
            if cached is not None:
                return cached
            raw   = striven.search_sales_orders({"PageIndex": 0, "PageSize": 1})
            total = raw.get("totalCount", 0)
            if not total:
                print(f"[count_estimates] WARNING: 'TotalCount' missing — keys={list(raw.keys())}", flush=True)
            print(f"[count_estimates] TotalCount={total}", flush=True)
            result = {
                "total":  total,
                "source": "striven_live",
                "note":   "Live count from Striven /v1/sales-orders/search → totalCount field",
            }
            _cache_set("count_estimates", result)
            return result

        # ── high_value_estimates ─────────────────────────────────────────────
        # Fetch 100 recent records, filter client-side for total > $10,000,
        # sort highest-first, return top 25.
        if name == "high_value_estimates":
            cached = _cache_get("high_value_estimates")
            if cached is not None:
                return cached
            raw     = striven.search_sales_orders({"PageIndex": 0, "PageSize": 100})
            data    = raw.get("data") or []
            total   = raw.get("totalCount", 0)
            if not data:
                print(f"[high_value_estimates] WARNING: 'Data' key missing or null — keys={list(raw.keys())}", flush=True)
            print(f"[high_value_estimates] TotalCount={total} fetched={len(data)}", flush=True)
            high = sorted(
                [_fmt(r) for r in data if (r.get("total") or r.get("OrderTotal") or 0) >= 10000],
                key=lambda x: x["total"] or 0,
                reverse=True,
            )[:25]
            result = {"count": len(high), "records": [_slim_order(o) for o in high], "source": "striven_live"}
            _cache_set("high_value_estimates", result)
            return result

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
                _unassigned = sum(1 for o in records if o.get("sales_rep_name") == "Unassigned")
                print(
                    f"[search_estimates] deep total_pool={grand_total} returned={len(records)} "
                    f"unassigned_rep={_unassigned}/{len(records)} "
                    f"({100*_unassigned//len(records) if records else 0}%)",
                    flush=True,
                )
                return {"total": grand_total, "count": len(records),
                        "estimates": [_slim_order(o) for o in records],
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
                records = [_fmt(r) for r in data]
                _unassigned = sum(1 for o in records if o.get("sales_rep_name") == "Unassigned")
                print(
                    f"[search_estimates] TotalCount={total} returned={len(records)} "
                    f"unassigned_rep={_unassigned}/{len(records)} "
                    f"({100*_unassigned//len(records) if records else 0}%)",
                    flush=True,
                )
                return {"total": total, "count": len(records),
                        "estimates": [_slim_order(o) for o in records],
                        "note": "Fast mode — 1 page, most recent first"}

        if name == "get_estimate_by_id":
            raw    = striven.get_estimate(tool_input["estimate_id"])
            # Use canonical detail formatter — maps all confirmed fields including
            # salesRep, orderTotal, orderDate, targetDate, customFields.
            detail = _fmt_detail(raw)
            print(
                f"[get_estimate_by_id] id={detail.get('id')} "
                f"rep={detail.get('sales_rep_name')!r} "
                f"total={detail.get('total')} "
                f"status={detail.get('status')!r} "
                f"line_items={detail.get('line_item_count')}",
                flush=True,
            )
            return detail

        if name == "gas_log_audit":
            print("[TOOL] gas_log_audit called — running _run_gas_log_audit()", flush=True)
            result = _run_gas_log_audit()
            slim   = _slim_gas_log_for_claude(result)
            print(
                f"[TOOL] gas_log_audit complete — "
                f"total_checked={result.get('total_checked')} "
                f"gas_log_installs={result.get('gas_log_installs')} "
                f"missing_removal_fee={result.get('missing_removal_fee')} "
                f"slim_chars={len(str(slim))}",
                flush=True,
            )
            return slim

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

        # ── analyze_stuck_jobs ────────────────────────────────────────────────
        if name == "analyze_stuck_jobs":
            limit = min(tool_input.get("limit", 50), 50)
            print(f"[analyze_stuck_jobs] limit={limit}", flush=True)
            return _slim_analysis_result(_analyze_stuck_jobs(limit=limit))

        # ── analyze_install_gaps ──────────────────────────────────────────────
        if name == "analyze_install_gaps":
            limit = min(tool_input.get("limit", 40), 50)
            print(f"[analyze_install_gaps] limit={limit}", flush=True)
            return _slim_analysis_result(_analyze_install_gaps(limit=limit))

        # ── analyze_rep_pipeline ──────────────────────────────────────────────
        if name == "analyze_rep_pipeline":
            limit = min(tool_input.get("limit", 30), 50)
            print(f"[analyze_rep_pipeline] limit={limit}", flush=True)
            return _slim_analysis_result(_analyze_rep_pipeline(limit=limit))

        # ── analyze_weekly_pipeline ───────────────────────────────────────────
        if name == "analyze_weekly_pipeline":
            limit = min(tool_input.get("limit", 40), 75)
            print(f"[analyze_weekly_pipeline] limit={limit}", flush=True)
            return _slim_analysis_result(_analyze_weekly_pipeline(limit=limit))

        # ── search_knowledge ──────────────────────────────────────────────────
        # Searches the structured knowledge base (markdown files in /knowledge/).
        # Returns the most relevant sections for the query.
        if name == "search_knowledge":
            query  = tool_input.get("query", "").strip()
            top_k  = min(tool_input.get("top_k", 4), 8)
            print(f"[search_knowledge] query={query!r} top_k={top_k}", flush=True)
            results  = _knowledge.search(query, top_k=top_k)
            formatted = _knowledge.format_search_results(results)
            return {
                "query":    query,
                "sections_found": len(results),
                "content":  formatted,
            }

        return {"error": f"Unknown tool: {name}"}

    except Exception as exc:
        return {"error": str(exc)}


@app.route("/", methods=["GET"])
def chat_ui():
    """Serve the WilliamSmith chat interface."""
    return render_template("index.html")


@app.route("/logs", methods=["GET"])
def view_logs():
    """Admin view — shows the last 100 WilliamSmith search queries."""
    try:
        rows = get_chat_logs(limit=100)
    except Exception as exc:
        rows = []
        print(f"[logs] Failed to fetch chat logs: {exc}", flush=True)
    return render_template("logs.html", logs=rows)


@app.route("/api/chat", methods=["POST"])
def chat_api():
    """
    Agentic chat endpoint.
    Accepts: { messages: [{role, content}, ...] }
    Returns: { response: "<markdown string>" }

    Reliability guarantees:
      - Max 6 tool-use iterations (prevents infinite loops)
      - Tool results truncated to 6,000 chars (prevents context explosion)
      - Full try/except returns clean JSON on any failure (prevents "Network error")
      - Uses module-level Anthropic singleton (no TLS handshake per request)
      - Knowledge context NOT embedded in prompt (saves ~4,000 tokens per call)
    """
    # ── Constants ─────────────────────────────────────────────────────────────
    MAX_ITERATIONS   = 6        # hard cap on agentic tool-use loops
    MAX_RESULT_CHARS = 5_000    # safety-net truncation after _slim_order/_slim_analysis_result
    MAX_OUTPUT_TOKENS = 2_048   # sufficient for all normal responses
    MAX_HISTORY      = 8        # keep last N messages (4 turns) — older context trimmed

    t_req_start = _time_mod.monotonic()

    data     = request.get_json(force=True)
    messages = data.get("messages", [])

    if not os.getenv("ANTHROPIC_API_KEY"):
        return jsonify({"error": "ANTHROPIC_API_KEY not configured on server."}), 500

    # ── Server-side throttle — block burst requests ───────────────────────────
    if not _check_and_claim_request():
        print("[chat_api] rate-limited — too soon since last request", flush=True)
        return jsonify({
            "error": "System is processing another request — please wait a moment and try again."
        }), 429

    # Capture the user's question for logging
    user_question = ""
    for m in reversed(messages):
        if m.get("role") == "user":
            content = m.get("content", "")
            user_question = content if isinstance(content, str) else str(content)
            break

    # ── Hard-coded fast returns — no Claude call, no token cost ──────────────
    # Questions that are too broad or expensive are intercepted here and answered
    # with deterministic guidance before any tool or LLM work begins.
    _q_lower = user_question.lower()
    if "what should i be working on today" in _q_lower:
        print("[chat_api] fast-return: 'working on today' intercepted", flush=True)
        return jsonify({
            "response": (
                "Start with **Stuck Jobs** and **Install Gaps** — "
                "these highlight approved jobs that are not moving and need immediate attention."
            )
        })

    tools_used: list[str] = []

    # ── System prompt ─────────────────────────────────────────────────────────
    effective_prompt = _SYSTEM_PROMPT

    # ── FAST PATH — single Claude call ────────────────────────────────────────
    # For questions where we can determine the right tool deterministically from
    # keywords, we skip the first Claude call entirely:
    #   Normal flow:  Claude call #1 (pick tool) → Striven → Claude call #2 (answer)
    #   Fast path:    Striven → Claude call #1 (answer)
    #
    # This cuts response time roughly in half for the most common queries.
    # The full agentic loop below remains as the fallback for everything else.
    #
    # Rules for adding a pattern here:
    #   - The intent must be unambiguous from keywords alone
    #   - Exactly ONE tool must be the right answer
    #   - The pattern must not conflict with another (order matters: more specific first)
    #   - Anything ambiguous goes to the agentic loop — never force a wrong tool

    import re as _re

    def _fast_path_prefetch(question: str) -> tuple[str | None, dict | None]:
        """
        Classify the question and pre-fetch data without calling Claude first.

        Returns (tool_name, result_dict) if a fast-path match is found,
        or (None, None) to fall through to the agentic loop.

        Pattern priority: most specific patterns listed first.
        Gas log is now safe to fast-path: _run_gas_log_audit() checks the
        5-minute cache first, so cached responses return in < 1ms.
        Only the first uncached call is slow (60s full scan).
        """
        q = question.lower()

        # ── Gas log / removal fee ─────────────────────────────────────────────
        # Uses cache — instant if audit ran in the last 5 minutes.
        if _re.search(
            r'\bgas\s*log\b|\bremoval\s+fee\b|\bmissing.*removal\b'
            r'|\bburner\s+log\b|\bburner\s+install\b|\bgas\s+log\s+install\b',
            q,
        ):
            raw = _run_gas_log_audit()
            return "gas_log_audit", _slim_gas_log_for_claude(raw)

        # ── Estimate count ────────────────────────────────────────────────────
        if _re.search(
            r'\bhow many\b.*\bestimate|\bestimate.*\bhow many\b'
            r'|\btotal.*\bestimate|\bestimate.*\bcount\b'
            r'|\bcount.*\bestimate|\bhow many.*\border|\btotal.*\border',
            q,
        ):
            return "count_estimates", _execute_tool("count_estimates", {})

        # ── High-value / biggest jobs ─────────────────────────────────────────
        if _re.search(
            r'\bbiggest\b|\bhighest.?value\b|\blargest\b|\bmost\s+expensive\b'
            r'|\btop\s+jobs\b|\bhigh.?value\b|\bover\s+\$',
            q,
        ):
            return "high_value_estimates", _execute_tool("high_value_estimates", {})

        # ── Approved estimates ────────────────────────────────────────────────
        if _re.search(r'\bapproved\b.*\bestimate|\bestimate.*\bapproved\b|\bwhat.*\bapproved\b', q):
            return "search_estimates", _execute_tool(
                "search_estimates", {"status": 22, "page_size": 25}
            )

        # ── In-progress / active jobs ─────────────────────────────────────────
        if _re.search(r'\bin.?progress\b|\bactive\s+jobs\b|\bcurrently\s+running\b', q):
            return "search_estimates", _execute_tool(
                "search_estimates", {"status": 25, "page_size": 25}
            )

        # ── Quoted / open estimates ───────────────────────────────────────────
        if _re.search(r'\bquoted\b.*\bestimate|\bestimate.*\bquoted\b|\bopen\s+quote', q):
            return "search_estimates", _execute_tool(
                "search_estimates", {"status": 19, "page_size": 25}
            )

        # ── Recent / latest estimates (no status filter) ──────────────────────
        if _re.search(
            r'\brecent\b|\blatest\b|\blast\s+\d+\b|\bmost\s+recent\b|\bnew(?:est)?\s+estimate',
            q,
        ):
            return "search_estimates", _execute_tool(
                "search_estimates", {"page_size": 25}
            )

        # ── Specific estimate by number ───────────────────────────────────────
        m = _re.search(r'\bestimate\s+#?\s*(\d{3,6})\b|\b#(\d{3,6})\b|\bso[-\s]?(\d{3,6})\b', q)
        if m:
            eid = int(next(g for g in m.groups() if g))
            return "get_estimate_by_id", _execute_tool("get_estimate_by_id", {"estimate_id": eid})

        # ── Customer lookup ───────────────────────────────────────────────────
        # Pattern: one or more Title-Case words (no spaces in char class — prevents
        # greedy over-capture). Matches "Scenic Custom Homes" but not plain words.
        _TCASE  = r'[A-Z][a-zA-Z&]+(?:\s+[A-Z][a-zA-Z&]+)*'
        _ENTITY = r'(?:estimate|job|order)s?'   # matches singular and plural
        m = _re.search(
            rf'{_ENTITY}\s+for\s+({_TCASE})'
            rf'|(?:show|find|get)\s+(?:me\s+)?({_TCASE})(?:\'s|s)?\s+{_ENTITY}'
            rf'|\bfor\s+(?:customer\s+)?({_TCASE})',
            question,           # original case — customer names are capitalised
            _re.IGNORECASE,     # match "show"/"Show"/"SHOW" etc.
        )
        if m:
            cname = next(g for g in m.groups() if g).strip()
            return "search_estimates_by_customer", _execute_tool(
                "search_estimates_by_customer", {"name": cname}
            )

        # ── Invoices / AR ─────────────────────────────────────────────────────
        if _re.search(r'\binvoice|\bwho\s+owes\b|\boutstanding\s+balance|\baccounts\s+receiv', q):
            return "search_invoices", _execute_tool("search_invoices", {"page_size": 25})

        # ── Payments received ─────────────────────────────────────────────────
        if _re.search(r'\bpayment.*receiv|\bcash\s+collect|\bwho.*paid\b', q):
            return "search_payments", _execute_tool("search_payments", {"page_size": 25})

        # ── Bills / AP ────────────────────────────────────────────────────────
        if _re.search(r'\bbill|\bvendor\s+bill|\baccounts\s+pay|\bwhat\s+we\s+owe\b', q):
            return "search_bills", _execute_tool("search_bills", {"page_size": 25})

        # ── Purchase orders ───────────────────────────────────────────────────
        if _re.search(r'\bpurchase\s+order|\bopen\s+po\b|\bpo\s+list\b', q):
            return "search_purchase_orders", _execute_tool(
                "search_purchase_orders", {"page_size": 25}
            )

        # ── Tasks / workload ──────────────────────────────────────────────────
        if _re.search(r'\bopen\s+task|\boverdue\s+task|\btask\s+list|\bworkload\b', q):
            return "search_tasks", _execute_tool("search_tasks", {"page_size": 25})

        # ── Stuck / delayed jobs (intelligence module) ────────────────────────
        if _re.search(
            r'\bstuck\b|\bdelayed?\b|\bstalled?\b|\bnot\s+moving\b'
            r'|\bno\s+progress\b|\bwhat.*\bstuck\b|\bstuck.*\bjob',
            q,
        ):
            return "analyze_stuck_jobs", _execute_tool("analyze_stuck_jobs", {})

        # ── Install scheduling gaps (intelligence module) ─────────────────────
        if _re.search(
            r'\bno\s+install\b|\bmissing\s+install\b|\binstall\s+gap\b'
            r'|\bnot\s+scheduled\b|\bneeds?\s+to\s+be\s+scheduled\b'
            r'|\bscheduling\s+gap\b|\bwhat.*\bschedul\b|\bschedul.*\bgap\b',
            q,
        ):
            return "analyze_install_gaps", _execute_tool("analyze_install_gaps", {})

        # ── Sales rep pipeline health (intelligence module) ───────────────────
        if _re.search(
            r'\bsales\s+rep\b|\brep\s+(?:health|performance|pipeline|report)\b'
            r'|\bhow\s+are\s+(?:the\s+)?reps\b|\bwhich\s+rep\b'
            r'|\brep\s+account|\bperformance\s+by\s+rep',
            q,
        ):
            return "analyze_rep_pipeline", _execute_tool("analyze_rep_pipeline", {})

        # ── Weekly pipeline review (intelligence module) ──────────────────────
        if _re.search(
            r'\bweekly\s+pipeline\b|\bpipeline\s+review\b|\bpipeline\s+report\b'
            r'|\bsales\s+pipeline\b|\bpipeline\s+meeting\b|\bpipeline\s+status\b'
            r'|\bhow\s+is\s+(?:the\s+)?pipeline\b|\bfull\s+pipeline\b'
            r'|\bweekly\s+report\b|\bpipeline\s+by\s+rep\b',
            q,
        ):
            return "analyze_weekly_pipeline", _execute_tool("analyze_weekly_pipeline", {})

        # ── Knowledge-only questions (no live data needed) ────────────────────
        if _re.search(
            r'\bwhat\s+is\s+(?:the\s+)?process\b|\bhow\s+does\b|\bwhat\s+does\b'
            r'|\bwhat\s+(?:fees|items|line\s+items)\b|\bwhat\s+should\b'
            r'|\bexplain\b|\bdefinition\b|\bnaming\s+convention\b'
            r'|\bwhat\s+(?:is\s+)?(?:a\s+)?(?:gas\s+log|chimney|isokern|preview\s+task)\b',
            q,
        ):
            results   = _knowledge.search(question, top_k=5)
            knowledge = _knowledge.format_search_results(results)
            return "search_knowledge", {"sections_found": len(results), "content": knowledge}

        return None, None   # no fast-path match — use agentic loop

    # Try the fast path only when this is the first user message (not mid-conversation).
    # Mid-conversation follow-ups need the full loop because context matters.
    _is_first_turn = sum(1 for m in messages if m.get("role") == "user") == 1

    if _is_first_turn:
        try:
            _fp_tool, _fp_data = _fast_path_prefetch(user_question)
        except Exception as _fp_exc:
            print(f"[fast-path] prefetch error — falling back: {_fp_exc}", flush=True)
            _fp_tool, _fp_data = None, None
    else:
        _fp_tool, _fp_data = None, None

    if _fp_tool is not None and _fp_data is not None:
        # We have the data. Make exactly one Claude call — no tools, just reasoning.
        t_fp = _time_mod.monotonic()
        tools_used = [_fp_tool]

        # Truncate prefetched data the same way the loop does
        _MAX_RESULT_CHARS_FP = 10_000
        _fp_data_str = json.dumps(_fp_data)
        if len(_fp_data_str) > _MAX_RESULT_CHARS_FP:
            _fp_data_str = (
                _fp_data_str[:_MAX_RESULT_CHARS_FP]
                + f'\n... [truncated — {len(_fp_data_str) - _MAX_RESULT_CHARS_FP} chars omitted]'
            )

        # Inject the data as a prefetch context block in the user message.
        # The last message in the history is the current user question.
        # We append the data inline so Claude sees: question + data in one turn.
        _fp_messages = list(messages[:-1])  # all prior turns unchanged
        _last_user_content = messages[-1].get("content", "") if messages else ""
        _fp_messages.append({
            "role": "user",
            "content": (
                f"{_last_user_content}\n\n"
                f"[Prefetched data from {_fp_tool}]\n"
                f"{_fp_data_str}\n\n"
                "Use the data above to answer the question directly. "
                "Do not call any tools — the data is already here."
            ),
        })

        try:
            _fp_response = _anthropic_client.messages.create(
                model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5"),
                max_tokens=2_048,
                system=effective_prompt,
                messages=_fp_messages,
                # No tools passed — forces a direct text response
            )
            _fp_text = next(
                (b.text for b in _fp_response.content if hasattr(b, "text")),
                None,
            )
            if _fp_text:
                elapsed_fp = round(_time_mod.monotonic() - t_fp, 2)
                elapsed_total = round(_time_mod.monotonic() - t_req_start, 2)
                print(
                    f"[chat] FAST-PATH tool={_fp_tool} "
                    f"input_tokens={_fp_response.usage.input_tokens} "
                    f"output_tokens={_fp_response.usage.output_tokens} "
                    f"claude_elapsed={elapsed_fp}s total_elapsed={elapsed_total}s",
                    flush=True,
                )
                try:
                    log_chat(user_question, tools_used, _fp_text)
                except Exception as _le:
                    print(f"[log_chat] WARNING: {_le}", flush=True)
                return jsonify({"response": _fp_text})
            # Empty response — fall through to loop
            print("[chat] fast-path got empty response — falling back to loop", flush=True)
        except Exception as _fp_call_exc:
            print(f"[chat] fast-path Claude call failed — falling back: {_fp_call_exc}", flush=True)

    # ── Agentic loop ──────────────────────────────────────────────────────────
    # Trim conversation history to last MAX_HISTORY messages before the loop.
    # Keeps the most recent context while preventing token bloat from long sessions.
    if len(messages) > MAX_HISTORY:
        print(f"[chat] trimming history {len(messages)} → {MAX_HISTORY} messages", flush=True)
        messages = messages[-MAX_HISTORY:]

    try:
        iteration = 0
        while iteration < MAX_ITERATIONS:
            iteration += 1
            t_iter = _time_mod.monotonic()

            response = _anthropic_client.messages.create(
                model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5"),
                max_tokens=MAX_OUTPUT_TOKENS,
                system=effective_prompt,
                tools=_CHAT_TOOLS,
                messages=messages,
            )

            elapsed_iter = round(_time_mod.monotonic() - t_iter, 2)
            print(
                f"[chat] iter={iteration} stop_reason={response.stop_reason} "
                f"input_tokens={response.usage.input_tokens} "
                f"output_tokens={response.usage.output_tokens} "
                f"elapsed={elapsed_iter}s",
                flush=True,
            )

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        tools_used.append(block.name)
                        t_tool = _time_mod.monotonic()
                        result = _execute_tool(block.name, block.input)
                        elapsed_tool = round(_time_mod.monotonic() - t_tool, 2)

                        # Serialise and truncate — prevents context explosion.
                        # A 25-estimate search result can be 10KB+ of JSON.
                        # Truncating here keeps subsequent Claude calls fast.
                        result_str = json.dumps(result)
                        if len(result_str) > MAX_RESULT_CHARS:
                            result_str = (
                                result_str[:MAX_RESULT_CHARS]
                                + f'\n... [truncated — {len(result_str) - MAX_RESULT_CHARS} chars omitted]"'
                            )
                            print(
                                f"[chat] tool={block.name} result truncated to "
                                f"{MAX_RESULT_CHARS} chars",
                                flush=True,
                            )

                        print(
                            f"[chat] tool={block.name} "
                            f"result_chars={len(result_str)} "
                            f"elapsed={elapsed_tool}s",
                            flush=True,
                        )

                        tool_results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     result_str,
                        })

                messages = messages + [
                    {"role": "assistant", "content": response.content},
                    {"role": "user",      "content": tool_results},
                ]

            else:
                # stop_reason == "end_turn" (or "max_tokens") — extract text
                text = next(
                    (block.text for block in response.content if hasattr(block, "text")),
                    "No response generated.",
                )

                elapsed_total = round(_time_mod.monotonic() - t_req_start, 2)
                print(
                    f"[chat] DONE iterations={iteration} tools={tools_used} "
                    f"total_elapsed={elapsed_total}s",
                    flush=True,
                )

                try:
                    log_chat(user_question, tools_used, text)
                except Exception as log_exc:
                    print(f"[log_chat] WARNING: {log_exc}", flush=True)

                return jsonify({"response": text})

        # ── Iteration cap reached ─────────────────────────────────────────────
        # Claude used all 6 rounds of tools without producing a final answer.
        # Ask it to wrap up now with whatever it has.
        print(
            f"[chat] WARNING: iteration cap ({MAX_ITERATIONS}) reached — "
            "requesting final answer",
            flush=True,
        )
        messages = messages + [{
            "role": "user",
            "content": (
                "[System: You have reached the maximum number of tool calls. "
                "Please provide your best answer now using the information "
                "already retrieved. Do not call any more tools.]"
            ),
        }]
        response = _anthropic_client.messages.create(
            model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5"),
            max_tokens=MAX_OUTPUT_TOKENS,
            system=effective_prompt,
            tools=_CHAT_TOOLS,
            tool_choice={"type": "none"},   # force text-only response
            messages=messages,
        )
        text = next(
            (block.text for block in response.content if hasattr(block, "text")),
            "I retrieved the data but hit a processing limit. Please try a more specific question.",
        )
        elapsed_total = round(_time_mod.monotonic() - t_req_start, 2)
        print(
            f"[chat] DONE (cap) iterations={MAX_ITERATIONS} "
            f"total_elapsed={elapsed_total}s",
            flush=True,
        )
        try:
            log_chat(user_question, tools_used, text)
        except Exception as log_exc:
            print(f"[log_chat] WARNING: {log_exc}", flush=True)
        return jsonify({"response": text})

    except Exception as exc:
        # ── Safety net ────────────────────────────────────────────────────────
        # Any unhandled exception returns clean JSON — never a raw 500 with no
        # body, which is what the frontend sees as "Network error".
        elapsed_total = round(_time_mod.monotonic() - t_req_start, 2)
        exc_type = type(exc).__name__
        exc_str  = str(exc).lower()
        print(
            f"[chat] ERROR after {elapsed_total}s: {exc_type}: {exc}",
            flush=True,
        )

        # Detect rate-limit / overload conditions from Claude API and return a
        # "response" (not "error") so the UI renders it as an assistant message,
        # not a generic error banner.
        _is_rate_limit = (
            "ratelimit" in exc_type.lower()
            or "overloaded" in exc_str
            or "rate limit" in exc_str
            or "529" in exc_str
            or "overload" in exc_str
            or getattr(exc, "status_code", None) in (429, 529)
        )
        if _is_rate_limit:
            print("[chat] Claude rate-limit / overload detected — returning friendly fallback", flush=True)
            return jsonify({
                "response": (
                    "The AI is currently busy — but live data is still available. "
                    "Try clicking **Stuck Jobs**, **Install Gaps**, or **Weekly Pipeline** "
                    "above for instant results, or ask a simpler question in a moment."
                )
            })

        return jsonify({
            "error": (
                f"Something went wrong on the server ({exc_type}). "
                "Please try again — if the problem persists, try a simpler question."
            )
        }), 500


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

@app.route("/chat", methods=["POST"])
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
