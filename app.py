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
import json
import anthropic
from flask import Flask, jsonify, request, render_template
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
        data       = raw.get("Data", [])
        total      = raw.get("TotalCount", 0)
        if not data:
            print(f"[search-estimates] WARNING: 'Data' key missing — keys={list(raw.keys())}", flush=True)
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

        data  = raw.get("Data", [])
        total = raw.get("TotalCount", 0)

        if not data:
            print(f"[report-preview] WARNING: 'Data' key missing — keys={list(raw.keys())}", flush=True)

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


@app.get("/missing-portal-flag")
def missing_portal_flag():
    """
    Audit all estimates and return those where the custom field
    "Do not show items on estimate in the Customer Portal display"
    is either missing or not set to true.

    This endpoint paginates through every sales order in Striven,
    inspects each record's customFields array, and flags any record
    that is not correctly configured.

    Response shape:
        {
            "summary": {
                "total_estimates_checked": <int>,
                "total_missing_flag": <int>
            },
            "records": [
                {
                    "estimate_id": <int>,
                    "estimate_number": <str>,
                    "estimate_name": <str>,
                    "customer_name": <str>,
                    "sales_rep": <str|null>,
                    "status": <str>
                },
                ...
            ]
        }
    """
    try:
        # Fetch every estimate across all pages (~100 records per API call)
        all_estimates = striven.get_all_estimates()

        broken_records = []

        for estimate in all_estimates:
            custom_fields = estimate.get("customFields") or []

            # Find the portal flag field by name (case-insensitive)
            portal_field = next(
                (
                    f for f in custom_fields
                    if isinstance(f.get("name"), str)
                    and f["name"].strip().lower() == PORTAL_FLAG_FIELD
                ),
                None,  # Field not present on this record at all
            )

            # Flag if the field is missing entirely OR its value is not true
            field_missing = portal_field is None
            field_not_true = (
                not field_missing
                and portal_field.get("value") is not True
            )

            if field_missing or field_not_true:
                # Safely extract nested fields — Striven may omit optional keys
                customer = estimate.get("customer") or {}
                sales_rep_obj = estimate.get("salesRep") or {}
                status_obj = estimate.get("status") or {}

                broken_records.append({
                    "estimate_id": estimate.get("id"),
                    "estimate_number": estimate.get("number"),
                    "estimate_name": estimate.get("name"),
                    "customer_name": customer.get("name"),
                    "sales_rep": sales_rep_obj.get("name"),  # null if unassigned
                    "status": status_obj.get("name"),
                })

        return jsonify({
            "summary": {
                "total_estimates_checked": len(all_estimates),
                "total_missing_flag": len(broken_records),
            },
            "records": broken_records,
        })

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

_SYSTEM_PROMPT = """You are WilliamSmith, a sharp and reliable business assistant for WilliamSmith Fireplaces.
You have direct access to live company data from Striven (our business management platform).

YOUR ROLE
Answer questions about estimates, customers, job values, and the sales pipeline.
Be concise, accurate, and business-focused.

CRITICAL RULE — ALWAYS USE TOOLS
You MUST call the appropriate tool for EVERY data question. Never answer from
memory, never guess, never use a hardcoded number. The tools return live data
directly from Striven. A wrong number is worse than a slow one.

ESTIMATE COUNT — MANDATORY TOOL CALL
ANY question that involves counting estimates MUST call count_estimates.
This includes (but is not limited to):
  "how many estimates"
  "total estimates"
  "estimate count"
  "how many orders"
  "what is our total"
  "how many records"
You must NEVER answer an estimate count question without calling count_estimates first.
The tool returns the real live TotalCount from Striven — not a guess, not a cache.

ESTIMATES & SALES ORDERS
In our system "estimates" and "sales orders" are the same thing.
Status codes: 18=Incomplete  19=Quoted  20=Pending Approval  22=Approved  25=In Progress  27=Completed

TOOL ROUTING
- Any count / total / "how many" question       → count_estimates (MANDATORY)
- "Biggest / highest value jobs"                → high_value_estimates
- "Estimates for [customer name]"               → search_estimates_by_customer
- "Approved / quoted / in-progress estimates"   → search_estimates with status filter
- "Estimates from [date] to [date]"             → search_estimates with date range
- "Tell me about estimate #N"                   → get_estimate_by_id
- "Missing portal flag / portal audit"          → portal_flag_audit (warn: ~60 s)

FORMAT
Lead with the direct answer and the live number. Use a markdown table for lists
(columns: #, Customer, Total, Status). Round dollar amounts to nearest dollar.
End with a short follow-up offer."""

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
            "Flexible estimate search. "
            "Status codes: 18=Incomplete 19=Quoted 20=Pending 22=Approved 25=In Progress 27=Completed"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "status":      {"type": "integer", "description": "Filter by status ID"},
                "date_from":   {"type": "string",  "description": "Start date YYYY-MM-DD"},
                "date_to":     {"type": "string",  "description": "End date YYYY-MM-DD"},
                "keyword":     {"type": "string",  "description": "Filter by estimate name"},
                "page_size":   {"type": "integer", "description": "Results per page (default 25)"},
            },
            "required": [],
        },
    },
    {
        "name": "get_estimate_by_id",
        "description": "Fetch the full detail of a single estimate by its Striven ID.",
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
]


def _fmt(r: dict) -> dict:
    """
    Normalise a raw Striven sales-order record into a clean dict.

    POST /v1/sales-orders/search returns TitleCase keys per the API schema:
        Id, Number, Name, Customer{Id,Name}, Status{Id,Name}, DateCreated
    We check TitleCase first, then fall back to camelCase for safety.

    Note: the search endpoint does NOT return a total/price field.
    OrderTotal is only available on the single-record GET endpoint.
    """
    customer = r.get("Customer") or r.get("customer") or {}
    status   = r.get("Status")   or r.get("status")   or {}
    return {
        "id":              r.get("Id")          or r.get("id"),
        "estimate_number": r.get("Number")      or r.get("number"),
        "customer_name":   customer.get("Name") or customer.get("name"),
        "total":           r.get("OrderTotal")  or r.get("total"),
        "date":            r.get("DateCreated") or r.get("dateCreated"),
        "status":          status.get("Name")   or status.get("name"),
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
    cust_raw  = striven.search_customers(search_name, page_size=10)
    customers = cust_raw.get("Data", [])
    if not customers:
        print(f"[customer-search] WARNING: 'Data' key missing — keys={list(cust_raw.keys())}", flush=True)
    print(
        f"[customer-search] '{search_name}' → {cust_raw.get('TotalCount', 0)} customer(s) found",
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
        cust_id   = cust.get("Id") or cust.get("id")
        cust_name = cust.get("Name") or cust.get("name")

        total_count      = None   # captured once from page 0, never overwritten
        page_index       = 0
        customer_records: list[dict] = []

        while True:
            est_raw = striven.search_sales_orders({
                "PageIndex":  page_index,
                "PageSize":   PAGE_SIZE,
                "CustomerId": cust_id,
            })

            data = est_raw.get("Data", [])

            if total_count is None:
                total_count  = est_raw.get("TotalCount", 0)
                grand_total += total_count
                if not data:
                    print(f"[customer-search] WARNING: 'Data' key missing — keys={list(est_raw.keys())}", flush=True)
                print(f"[customer-search] '{cust_name}' (ID={cust_id}) → TotalCount={total_count}", flush=True)
                print(f"[customer-search] First record sample: {data[0] if data else 'EMPTY'}", flush=True)

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


def _execute_tool(name: str, tool_input: dict) -> dict:
    """Map a Claude tool call directly to the live Striven API. No local cache."""
    try:
        # ── count_estimates ──────────────────────────────────────────────────
        # POST /v1/sales-orders/search with pageSize=1.
        # We only need totalCount — no records are read.
        # NO fallback, NO Supabase, NO cache. Live Striven only.
        if name == "count_estimates":
            raw   = striven.search_sales_orders({"PageIndex": 0, "PageSize": 1})
            total = raw.get("TotalCount", 0)
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
            data    = raw.get("Data", [])
            total   = raw.get("TotalCount", 0)
            if not data:
                print(f"[high_value_estimates] WARNING: 'Data' key missing — keys={list(raw.keys())}", flush=True)
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
            body: dict = {
                "PageIndex": 0,
                "PageSize":  tool_input.get("page_size", 25),
            }
            if "status"    in tool_input: body["StatusChangedTo"] = tool_input["status"]
            if "keyword"   in tool_input: body["Name"]            = tool_input["keyword"]
            if "date_from" in tool_input or "date_to" in tool_input:
                date_range: dict = {}
                if "date_from" in tool_input: date_range["DateFrom"] = tool_input["date_from"]
                if "date_to"   in tool_input: date_range["DateTo"]   = tool_input["date_to"]
                body["DateCreatedRange"] = date_range
            raw     = striven.search_sales_orders(body)
            data    = raw.get("Data", [])
            total   = raw.get("TotalCount", 0)
            if not data:
                print(f"[search_estimates] WARNING: 'Data' key missing — keys={list(raw.keys())}", flush=True)
            print(f"[search_estimates] TotalCount={total} returned={len(data)}", flush=True)
            records = [_fmt(r) for r in data]
            return {"total": total, "count": len(records), "estimates": records}

        if name == "get_estimate_by_id":
            return striven.get_estimate(tool_input["estimate_id"])

        if name == "portal_flag_audit":
            all_estimates = striven.get_all_estimates()
            portal_field_name = "do not show items on estimate in the customer portal display"
            broken = []
            for est in all_estimates:
                custom_fields = est.get("customFields") or []
                field = next(
                    (f for f in custom_fields
                     if isinstance(f.get("name"), str)
                     and f["name"].strip().lower() == portal_field_name),
                    None,
                )
                if field is None or field.get("value") is not True:
                    customer = est.get("customer") or {}
                    broken.append({
                        "estimate_id":     est.get("id"),
                        "estimate_number": est.get("number"),
                        "estimate_name":   est.get("name"),
                        "customer_name":   customer.get("name"),
                        "status":          (est.get("status") or {}).get("name"),
                    })
            return {
                "summary": {"total_checked": len(all_estimates), "total_missing": len(broken)},
                "records": broken,
            }

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
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Fail fast if credentials are missing
    for var in ("CLIENT_ID", "CLIENT_SECRET"):
        if not os.environ.get(var):
            raise RuntimeError(f"Environment variable {var!r} is not set.")

    app.run(host="0.0.0.0", port=5000, debug=True)
