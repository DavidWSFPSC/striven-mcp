"""
mcp_server.py

MCP (Model Context Protocol) server for Striven business data.

This server exposes the deployed Flask API as Claude-callable tools so that
the "Ask WilliamSmith" Claude.ai project can answer natural-language business
questions with live company data.

Usage:
  Local (stdio — Claude Desktop / Claude Code):
      python mcp_server.py

  Production (Render — HTTP mode):
      python mcp_server.py
      Render injects PORT automatically; the server detects it and
      starts in streamable-http mode. No flags needed.
      MCP endpoint: https://<your-render-url>/mcp

All tools are READ-ONLY unless explicitly noted (sync-estimates writes to
Supabase only — it never modifies Striven).
"""

import os
import requests
from mcp.server.fastmcp import FastMCP
from starlette.responses import JSONResponse

# ---------------------------------------------------------------------------
# Knowledge-base search helpers (in-process — no inter-service HTTP call)
# ---------------------------------------------------------------------------

def _kb_embed(query: str) -> list:
    """Embed a query string using OpenAI text-embedding-3-small."""
    from openai import OpenAI as _OpenAI
    client = _OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    resp = client.embeddings.create(model="text-embedding-3-small", input=query)
    return resp.data[0].embedding


def _kb_search(query: str, top_k: int = 5) -> dict:
    """Embed query and call the Supabase match_kb_document_chunks RPC directly."""
    try:
        from supabase import create_client as _create_client
        sb = _create_client(
            os.environ["SUPABASE_URL"],
            os.environ["SUPABASE_KEY"],
        )
        embedding = _kb_embed(query)
        result = (
            sb.rpc(
                "match_kb_document_chunks",
                {"query_embedding": embedding, "match_count": min(top_k, 20)},
            )
            .execute()
        )
        results = result.data or []

        # Fire-and-forget logging — never raises, never breaks search
        try:
            from services.supabase_client import log_kb_search
            top_sim = results[0].get("similarity") if results else None
            log_kb_search(
                query=query,
                top_k=min(top_k, 20),
                result_count=len(results),
                top_similarity=top_sim,
                returned_results=bool(results),
            )
        except Exception:
            pass

        return {"query": query, "results": results}
    except Exception as exc:
        return {"error": str(exc)}

# ---------------------------------------------------------------------------
# Invoice AR helpers (in-process Supabase queries — no Flask hop)
# ---------------------------------------------------------------------------

def _sb_client():
    """Return a Supabase client using env vars (lazy, not cached — safe for forked workers)."""
    from supabase import create_client as _create_client
    return _create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])


def _aging_info(due_date_str: str | None) -> tuple[int | None, str]:
    """Return (days_outstanding, aging_bucket) computed from a due_date ISO string."""
    from datetime import date, datetime
    if not due_date_str:
        return None, "Unknown"
    try:
        due = datetime.fromisoformat(due_date_str.replace("Z", "+00:00")).date()
    except Exception:
        return None, "Unknown"
    today = date.today()
    days  = (today - due).days
    if today <= due:
        return 0, "Current"
    if days <= 30:
        return days, "1-30 Days"
    if days <= 60:
        return days, "31-60 Days"
    if days <= 90:
        return days, "61-90 Days"
    return days, "90+ Days"


def _bucket_due_date_range(bucket: str) -> tuple[str | None, str | None]:
    """Translate an aging_bucket name to a (due_date_lt, due_date_gte) pair for Supabase filtering."""
    from datetime import date, timedelta
    today = date.today()
    if bucket == "Current":
        # due_date >= today
        return None, today.isoformat()
    if bucket == "1-30 Days":
        return (today - timedelta(days=1)).isoformat(), (today - timedelta(days=30)).isoformat()
    if bucket == "31-60 Days":
        return (today - timedelta(days=31)).isoformat(), (today - timedelta(days=60)).isoformat()
    if bucket == "61-90 Days":
        return (today - timedelta(days=61)).isoformat(), (today - timedelta(days=90)).isoformat()
    if bucket == "90+ Days":
        return (today - timedelta(days=91)).isoformat(), None
    return None, None


def _invoice_ar_summary() -> dict:
    """Aggregate open-AR totals and aging from the Supabase invoices table."""
    try:
        res = (
            _sb_client()
            .table("invoices")
            .select("invoice_id, customer_name, open_balance, due_date")
            .gt("open_balance", 0)
            .limit(5000)
            .execute()
        )
        rows = res.data or []

        if not rows:
            return {"total_open_ar": 0, "invoice_count": 0, "by_bucket": {}, "oldest_invoice": None}

        from collections import defaultdict
        bucket_counts:  dict[str, int]   = defaultdict(int)
        bucket_amounts: dict[str, float] = defaultdict(float)
        total_ar = 0.0
        oldest: dict | None = None

        for r in rows:
            bal                   = float(r.get("open_balance") or 0)
            days_out, bucket      = _aging_info(r.get("due_date"))
            total_ar             += bal
            bucket_counts[bucket]  += 1
            bucket_amounts[bucket] += bal

            due = r.get("due_date")
            if due and days_out is not None and (
                oldest is None or days_out > (oldest.get("days_outstanding") or 0)
            ):
                oldest = {
                    "invoice_id":       r["invoice_id"],
                    "customer_name":    r.get("customer_name"),
                    "open_balance":     bal,
                    "due_date":         due,
                    "days_outstanding": days_out,
                }

        ordered_buckets = ["Current", "1-30 Days", "31-60 Days", "61-90 Days", "90+ Days", "Unknown"]
        by_bucket = {
            b: {"count": bucket_counts[b], "amount": round(bucket_amounts[b], 2)}
            for b in ordered_buckets
            if bucket_counts[b] > 0
        }

        return {
            "total_open_ar":  round(total_ar, 2),
            "invoice_count":  len(rows),
            "by_bucket":      by_bucket,
            "oldest_invoice": oldest,
        }
    except Exception as exc:
        return {"error": str(exc)}


def _search_invoices_supabase(
    customer_name: str   = "",
    aging_bucket:  str   = "",
    min_balance:   float = 0.0,
    limit:         int   = 50,
) -> dict:
    """Query Supabase invoices table with optional filters, sorted by days_outstanding desc."""
    try:
        q = (
            _sb_client()
            .table("invoices")
            .select("invoice_id, txn_number, txn_date, due_date, open_balance, customer_name, memo")
            .gt("open_balance", 0)
        )
        if customer_name:
            q = q.ilike("customer_name", f"%{customer_name}%")
        if min_balance > 0:
            q = q.gte("open_balance", min_balance)

        # Translate aging_bucket to a due_date range filter so it works even when
        # the generated column is null (CURRENT_DATE is volatile; PostgreSQL may
        # refuse to store it in a GENERATED ALWAYS AS STORED column).
        if aging_bucket:
            due_lt, due_gte = _bucket_due_date_range(aging_bucket)
            if aging_bucket == "Current":
                q = q.gte("due_date", due_gte)
            elif aging_bucket == "90+ Days":
                q = q.lt("due_date", due_lt)
            elif due_lt and due_gte:
                q = q.lt("due_date", due_lt).gte("due_date", due_gte)

        res  = q.limit(limit * 3).execute()   # over-fetch so Python sort+limit is accurate
        rows = res.data or []

        # Compute and attach aging fields in Python, then sort by days_outstanding desc
        enriched = []
        for r in rows:
            days_out, bucket = _aging_info(r.get("due_date"))
            enriched.append({**r, "days_outstanding": days_out, "aging_bucket": bucket})

        enriched.sort(key=lambda x: (x["days_outstanding"] or 0), reverse=True)
        enriched = enriched[:limit]

        return {
            "count":   len(enriched),
            "filters": {
                "customer_name": customer_name or None,
                "aging_bucket":  aging_bucket  or None,
                "min_balance":   min_balance   or None,
            },
            "invoices": enriched,
        }
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("FLASK_API_URL", "https://striven-mcp-v2.onrender.com")
TIMEOUT  = 120  # seconds — Render free tier cold starts can take 50+ seconds

# ---------------------------------------------------------------------------
# MCP server — identity + standing instructions for Claude
# ---------------------------------------------------------------------------

from mcp.server.transport_security import TransportSecuritySettings

# Allow the Render host + localhost for local dev.
# DNS rebinding protection in MCP 1.27 blocks all requests when allowed_hosts
# is empty — must explicitly list every valid Host header value.
_transport_security = TransportSecuritySettings(
    enable_dns_rebinding_protection=True,
    allowed_hosts=[
        "striven-mcp-server.onrender.com",
        "striven-mcp-v2.onrender.com",   # in case connected via v2 URL
        "localhost",
        "localhost:8000",
        "127.0.0.1",
        "127.0.0.1:8000",
    ],
    allowed_origins=[
        "https://claude.ai",
        "https://striven-mcp-server.onrender.com",
        "https://striven-mcp-v2.onrender.com",
    ],
)

mcp = FastMCP(
    name="Ask WilliamSmith — Striven Business Intelligence",
    transport_security=_transport_security,
    instructions="""
You are WilliamSmith, a knowledgeable business assistant for this company.
You have direct access to live company data from Striven (our business
management platform) and any documents the team has uploaded to this project.

YOUR ROLE
---------
Answer questions about estimates, customers, job values, sales pipeline,
and company operations. Be concise, accurate, and business-focused.
Always use a tool to fetch real data — never guess or invent numbers.

ESTIMATES & SALES ORDERS
------------------------
In our system, "estimates" and "sales orders" are the same thing.
Status codes:
  18 = Incomplete
  19 = Quoted
  20 = Pending Approval
  22 = Approved
  25 = In Progress
  27 = Completed

TOOL ROUTING PRIORITY — ALWAYS FOLLOW THIS ORDER
-------------------------------------------------
Before calling any tool, determine which tier can answer the question.
Call the lowest-numbered tier that can answer it. Never skip tiers.

TIER 1 — Knowledge Base (search_knowledge_base)
  Call this FIRST for ANY question about:
    - Product specs, dimensions, clearances, BTU ratings
    - Installation procedures or requirements (any brand or model)
    - Venting, gas line, or combustion air specs
    - Warranty policies or service procedures
    - Troubleshooting any symptom or brand
    - Company policies, CSR procedures, HR documents
    - Mortar mixes, refractory specs, hearth materials
    - Permits, code requirements, estimating standards
    - Price books, vendor documents, marketing materials
    - Builder books or construction phase documentation
  If the top result has similarity >= 0.75, return that answer
  and do NOT call any further tools unless the user asks for data.

TIER 2 — Supabase Cached Data (fast, no rate limits)
  Use BEFORE any live Striven call when the question is about:
    - Estimate counts, high-value jobs, brand/product summaries
    - AR totals, open invoices, payment history
    - Callback intelligence, return trip analysis
    - Customer lifetime value, conversion rates
    - Task summaries, items catalog, vendor list
  Tier 2 tools: count_estimates, high_value_estimates, brand_summary,
    search_by_product, search_callback_insights, callbacks_by_product,
    invoice_ar_summary, search_invoices, time_to_preview, customer_ltv,
    conversion_rates, task_summary, search_items, search_vendors,
    search_payments, search_estimates_by_customer, jobs_by_location,
    high_value_estimates, analyze_callback_causes, weekly_digest

TIER 3 — Live Striven API (only when Tier 1 and 2 cannot answer)
  Use ONLY when cached data is insufficient or real-time accuracy is needed:
    - Real-time estimate status, custom fields, pipeline status
    - Portal flag audit, stuck job analysis
    - Invoice lookup tied to a specific estimate
  Tier 3 tools: search_estimates, search_by_pipeline_status,
    portal_flag_audit, analyze_stuck_jobs, get_invoices_by_estimate,
    search_return_trips, backlog_by_rep, invoice_audit

TOOLS AVAILABLE
---------------
- count_estimates              → total records in database
- high_value_estimates         → jobs over $10,000 (sorted highest first)
- search_estimates_by_customer → find all estimates for a specific customer
- search_estimates             → flexible search by status, date range, keyword
- get_estimate_by_id           → full detail on a single estimate
- portal_flag_audit            → find estimates missing the Customer Portal flag
- sync_estimates               → refresh the database from Striven (use sparingly)
- api_health                   → check if the system is online
- backlog_by_rep               → active job count + revenue grouped by sales rep
- jobs_by_location             → job count + revenue by named area (zip-code accurate for tri-county)
- time_to_preview              → average days from estimate creation to site preview
- get_invoices_by_estimate     → all invoices linked to a specific estimate (status, total, balance due)
- invoice_audit                → completed estimates missing a final invoice (billing gap audit)
- invoice_ar_summary           → total AR, aging bucket breakdown, oldest unpaid invoice (Supabase)
- search_invoices              → search open invoices by customer, aging bucket, or min balance (Supabase)
- search_by_product            → search estimates by product/service keyword in line items (e.g. "isokern", "gas log")
- brand_summary                → leaderboard of all brands by job count + revenue (all 24 brands in one call)
- search_by_pipeline_status   → find active jobs by operational status (ready to schedule, waiting on product, etc.)
- search_return_trips         → find return trip / callback tasks on estimates (live Striven scan)
- search_callback_insights    → callback intelligence from historical database (fast, aggregated)

WHEN TO USE EACH TOOL
---------------------
- "How many estimates do we have?"                → count_estimates
- "Show me our biggest jobs"                      → high_value_estimates
- "What estimates do we have for Acme?"           → search_estimates_by_customer
- "Show me approved estimates this month"         → search_estimates with status=22
- "Tell me about estimate #4521"                  → get_estimate_by_id
- "Which estimates are missing the portal flag?"  → portal_flag_audit
- "The data seems outdated"                       → sync_estimates
- "Who has the most active jobs?"                 → backlog_by_rep
- "How much work do we have in West Ashley?"       → jobs_by_location (zip-resolved)
- "Show me Kiawah jobs from 2024"                 → jobs_by_location with year=2024
- "How long does it take to schedule a preview?"  → time_to_preview
- "Show me jobs ready to schedule"                → search_by_pipeline_status
- "What jobs are waiting on product?"             → search_by_pipeline_status
- "Which jobs need review before invoicing?"      → search_by_pipeline_status
- "Has estimate 9275 been invoiced?"               → get_invoices_by_estimate(9275)
- "Which completed jobs have no invoice?"          → invoice_audit()
- "Billing gaps from 2024"                         → invoice_audit(year=2024)
- "What is our total AR?"                          → invoice_ar_summary()
- "Show me the AR aging report"                    → invoice_ar_summary()
- "Which customers owe us 90+ days?"               → search_invoices(aging_bucket="90+ Days")
- "AR for Village Restoration"                     → search_invoices(customer_name="Village Restoration")
- "Invoices over $10,000 past due"                 → search_invoices(aging_bucket="90+ Days", min_balance=10000)
- "What brands do we install most?"                → brand_summary()
- "Brand breakdown in Kiawah?"                    → brand_summary(zip="29455")
- "How many isokern jobs have we done?"            → search_by_product(keyword="isokern")  ← NEVER use search_estimates for this
- "Gas log installs in Kiawah?"                   → search_by_product(keyword="gas log", zip="29455")
- "How many Heat & Glo jobs?"                     → search_by_product(keyword="heat & glo")
- "Show me all Majestic estimates"                → search_by_product(keyword="majestic")
- "Napoleon jobs completed in 2024"               → search_by_product(keyword="napoleon", status="Completed", year=2024)

CRITICAL ROUTING RULE:
Any question about a product brand or product type → search_by_product or brand_summary.
NEVER call search_estimates for brand/product questions — it hits Striven's API and rate-limits.
search_estimates is ONLY for status/date/estimate-number lookups.
- "Show me all return trips"                      → search_return_trips
- "Which jobs have callbacks?"                    → search_return_trips
- "Who has the most callbacks?"                   → search_callback_insights(by="assignee")
- "What's our callback rate by year?"             → search_callback_insights(by="year")
- "Show me open return trips"                     → search_callback_insights(status="Open")
- "How many callbacks did Steven have?"           → search_callback_insights(assignee="Steven")
- "Show callback breakdown by type"               → search_callback_insights(by="type")
- "Why are we getting callbacks?"                 → analyze_callback_causes()
- "Are callbacks product failures or user error?" → analyze_callback_causes()
- "How many callbacks are billable?"              → analyze_callback_causes()
- "Callback cause breakdown for 2024"             → analyze_callback_causes(year=2024)
- "Show me Part failure callbacks"                → analyze_callback_causes(cause="Part")

TONE & FORMAT
-------------
- Use plain English. Avoid jargon.
- For lists of estimates, present as a clean table or bulleted list.
- Always include customer name, estimate number, total value, and status.
- Round dollar amounts to the nearest dollar.
- If a query returns no results, say so clearly and suggest alternatives.
- For documents uploaded to this project, read and reference them directly
  to answer policy, process, or reference questions.
""",
)

# ---------------------------------------------------------------------------
# Helper — shared error shape
# ---------------------------------------------------------------------------

def _call(method: str, path: str, **kwargs) -> dict:
    """Make an HTTP request to the Flask API and return the JSON response."""
    url = f"{BASE_URL}{path}"
    try:
        resp = getattr(requests, method)(url, timeout=TIMEOUT, **kwargs)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.Timeout:
        return {"error": "Request timed out — the API may be waking up. Try again in 30 seconds."}
    except requests.exceptions.HTTPError as exc:
        return {"error": f"API error {exc.response.status_code}: {exc.response.text[:300]}"}
    except Exception as exc:
        return {"error": str(exc)}


# ---------------------------------------------------------------------------
# Tools — Database summary
# ---------------------------------------------------------------------------

@mcp.tool()
def count_estimates() -> dict:
    """
    TIER 2 — Supabase. Get the total number of estimates stored in the database.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'How many estimates do we have?'
      'What is our total estimate count?'
    """
    return _call("get", "/estimates/count")


@mcp.tool()
def high_value_estimates() -> dict:
    """
    TIER 2 — Supabase. Return up to 25 estimates where the total value exceeds
    $10,000, sorted from highest to lowest.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'Show me our biggest jobs'
      'What are our highest value estimates?'
      'Top estimates by dollar amount'
    """
    return _call("get", "/estimates/high-value")


@mcp.tool()
def search_estimates_by_customer(name: str) -> dict:
    """
    Search estimates by customer name. Case-insensitive, partial match supported.

    Use when asked:
      'Show me estimates for [customer name]'
      'What jobs do we have for [company]?'
      'Find all work for [client]'

    Args:
        name: Customer name or partial name (e.g. 'Clear Water' or 'smith').
    """
    return _call("get", "/estimates/by-customer", params={"name": name})


# ---------------------------------------------------------------------------
# Tools — Flexible search
# ---------------------------------------------------------------------------

@mcp.tool()
def search_estimates(
    status: int | None = None,
    customer_id: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    keyword: str | None = None,
    page_size: int = 25,
    page: int = 1,
) -> dict:
    """
    TIER 3 — Striven Live API. Search estimates by status, date range, or
    estimate name/number. This tool calls the Striven API live — only use
    if Supabase cached tools (Tier 2) cannot answer the question.
    Calls the Striven API directly — use sparingly to avoid rate limits.

    IMPORTANT — DO NOT use this tool for product or brand searches.
    If the user asks about a brand (Isokern, Heat & Glo, Napoleon, Majestic,
    Acucraft, Stellar, Heatilator, Dimplex, etc.) or a product type (gas log,
    gas insert, linear fireplace, electric fireplace) — use search_by_product
    or brand_summary instead. Those tools hit Supabase and never rate-limit.

    Use THIS tool only for:
      - Filtering by status (Approved, In Progress, Completed, etc.)
      - Filtering by date range (this month, this year, etc.)
      - Looking up an estimate by its number or name fragment
      - Filtering by a specific customer ID

    Status codes:
      18 = Incomplete
      19 = Quoted
      20 = Pending Approval
      22 = Approved
      25 = In Progress
      27 = Completed

    Args:
        status:      Filter by status ID (see codes above).
        customer_id: Filter by Striven customer ID (integer).
        date_from:   Start of date range, ISO 8601 format (e.g. '2025-01-01').
        date_to:     End of date range, ISO 8601 format (e.g. '2025-12-31').
        keyword:     Filter by estimate name or number (NOT product/brand names).
        page_size:   Number of results to return (default 25, max 100).
        page:        Page number, 1-based (default 1).

    Examples:
      search_estimates(status=22)                          → all Approved
      search_estimates(status=19, date_from='2025-01-01') → Quoted this year
      search_estimates(keyword='8452')                     → find estimate #8452
    """
    params: dict = {
        "pageSize":  page_size,
        "pageIndex": page,
    }
    if status      is not None: params["statusChangedTo"] = status
    if customer_id is not None: params["customerId"]      = customer_id
    if date_from:               params["dateCreatedFrom"] = date_from
    if date_to:                 params["dateCreatedTo"]   = date_to
    if keyword:                 params["name"]            = keyword

    return _call("get", "/search-estimates", params=params)


@mcp.tool()
def get_estimate_by_id(estimate_id: int) -> dict:
    """
    Fetch the full details of a single estimate by its Striven ID.

    Use when asked:
      'Tell me about estimate #4521'
      'Show me the details for job [number]'
      'What is on estimate [ID]?'

    Args:
        estimate_id: The integer Striven estimate / sales order ID.
    """
    return _call("get", f"/get-estimate/{estimate_id}")


# ---------------------------------------------------------------------------
# Tools — Audits
# ---------------------------------------------------------------------------

@mcp.tool()
def portal_flag_audit() -> dict:
    """
    TIER 3 — Striven Live API. Audit all estimates and return those missing the
    Customer Portal display flag. This tool calls the Striven API live — only
    use if Supabase cached tools (Tier 2) cannot answer the question.

    The "Do not show items on estimate in the Customer Portal display" field
    must be set to true on every estimate. This tool finds every record where
    it is missing or set to false.

    Use when asked:
      'Which estimates are missing the portal flag?'
      'Run the Customer Portal audit'
      'Show me estimates that need to be fixed for the portal'

    Returns a summary (total checked, total missing) and a list of affected
    records with estimate number, customer name, sales rep, and status.

    Note: This scans ALL estimates in Striven and may take 30–60 seconds.
    """
    return _call("get", "/missing-portal-flag")


# ---------------------------------------------------------------------------
# Tools — Data pipeline
# ---------------------------------------------------------------------------

@mcp.tool()
def sync_estimates(limit: int = 50) -> dict:
    """
    Refresh the Supabase database with the latest data from Striven.

    Use when asked:
      'The data seems old — can you update it?'
      'Sync the estimates'
      'Refresh the database'

    Safe to run repeatedly — uses upsert logic, no duplicates are created.
    Striven is never modified.

    Args:
        limit: Maximum number of records to sync this run (default 50).
               Use 200–500 for a larger refresh. Full sync is ~9,300 records
               and takes several minutes — only use when explicitly requested.
    """
    return _call("get", "/sync-estimates", params={"limit": limit})


# ---------------------------------------------------------------------------
# Tools — Business Intelligence
# ---------------------------------------------------------------------------

@mcp.tool()
def backlog_by_rep(limit: int = 50) -> dict:
    """
    Get workload by sales rep: active job count and total revenue, grouped by rep.

    Pulls all Approved and In-Progress estimates live from Striven, looks up
    the sales rep on each one (via the detail endpoint), then groups the results
    so you can see which rep has the most active work and how much revenue they
    are carrying.

    Use when asked:
      'Who has the most active jobs right now?'
      'Show me the backlog by rep'
      'Which sales rep is carrying the most work?'
      'What is each rep's open pipeline?'

    Args:
        limit: Number of estimates to enrich with rep detail (default 50, max 100).
               Higher values are more complete but slower — each estimate requires
               one extra API call to retrieve the sales rep name.

    Returns:
        count         — total active estimates found
        enriched_count — how many were enriched with rep data
        data          — list of {rep, total_jobs, total_revenue}, sorted by job count
    """
    params: dict = {}
    if limit != 50:
        params["limit"] = limit
    return _call("get", "/queries/backlog-by-rep", params=params or None)


@mcp.tool()
def jobs_by_location(location: str, year: int = 0) -> dict:
    """
    Get job counts and total revenue for a specific service area or city.

    Uses zip-code-based geographic matching for Charleston tri-county named areas,
    which is far more accurate than city-name matching. A customer whose address
    says "Charleston" but zip 29407 will correctly appear under "West Ashley".

    Named areas (zip-resolved — use these for best results):
      West Ashley            — zips 29407, 29414
      James Island           — zip 29412
      Johns Island           — zip 29455 (also covers Kiawah, Seabrook)
      Kiawah Island          — zip 29455
      Seabrook Island        — zip 29455
      Downtown Charleston    — zips 29401, 29403
      North Charleston       — zips 29405, 29406, 29418, 29420
      Mount Pleasant         — zips 29464, 29466 (both N and S)
      Mount Pleasant South   — zip 29464
      Mount Pleasant North   — zip 29466
      Daniel Island          — zip 29492
      Summerville            — zips 29483, 29485
      Goose Creek            — zip 29445
      Hanahan                — zip 29410
      Folly Beach            — zip 29439
      Sullivan's Island      — zip 29482
      Isle of Palms          — zip 29451

    Use when asked:
      'How much work do we have in West Ashley?'
      'Show me jobs in Mount Pleasant'
      'What is our revenue from Isle of Palms?'
      'How many jobs have we done in Kiawah?'
      'How much business did we do in Summerville in 2024?'
      'Compare North Charleston to West Ashley'
      'Show me all Daniel Island jobs from last year'

    Args:
        location: Named area or city to search (case-insensitive).
                  Named tri-county areas resolve to zip codes for accurate matching.
                  Any other city/area name falls back to city-name substring search.
        year:     Optional calendar year to filter results (e.g. 2024 or 2025).
                  Use 0 to include all years (default).

    Returns:
        count           — total estimates for customers in this area
        total_revenue   — sum of all estimate values
        customers_found — distinct customers with addresses in the area
        area_label      — canonical display name for the matched area
        zips_used       — zip codes searched (empty if city-name fallback used)
        method          — "zip" (accurate) or "city_name" (fallback)
        year_filter     — year applied, or null
        by_status       — estimate counts + revenue grouped by Striven status
        sample          — up to 50 most-recent estimates with customer, rep, total
    """
    params: dict = {"location": location}
    if year and year > 0:
        params["year"] = year
    return _call("get", "/queries/jobs-by-location", params=params)


@mcp.tool()
def time_to_preview() -> dict:
    """
    TIER 2 — Supabase. Get the average and median number of days from estimate
    creation to the first site preview / inspection task being scheduled.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Measures the gap between when an estimate is created in Striven and when
    a Site Inspections/Preview task (task type 15) is logged against it.
    This is a key operational metric for understanding how quickly the team
    moves from quote to site visit.

    Use when asked:
      'How long does it take to schedule a preview?'
      'What is our average time from estimate to site visit?'
      'How fast do we move from quote to inspection?'
      'Show me our preview scheduling speed'

    Returns:
        average_days — mean days from estimate creation to preview task
        median_days  — median days (less sensitive to outliers)
        sample_size  — number of estimates included in the calculation
        data_note    — explanation of exactly what is being measured
        data         — up to 25 sample records sorted fastest-to-slowest,
                       each with estimate number, customer, and days_to_preview
    """
    return _call("get", "/queries/time-to-preview")


@mcp.tool()
def search_by_product(
    keyword: str,
    zip:     str = "",
    status:  str = "",
    year:    int = 0,
    limit:   int = 50,
) -> dict:
    """
    TIER 2 — Supabase. Search estimates by product or service keyword in line
    items, with optional filters for zip code, status, and year.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Searches both item_name (product SKU) and description (free-text notes)
    fields across all estimate line items, then joins to the estimate record.

    Use when asked:
      'How many isokern jobs have we done?'
      'Show me all gas log estimates'
      'How many linear fireplace installs in Kiawah?'
      'What napoleon jobs did we complete in 2024?'
      'How much isokern revenue came from Mount Pleasant?'
      'Show me all electric fireplace estimates'
      'How many isokern jobs in zip 29455?'
      'What's our total revenue from gas log installs?'

    Brand keywords (use the brand name as the keyword):
      Masonry & custom systems:
        "isokern"         — Isokern masonry fireplace systems
        "firerock"        — FireRock masonry fireplaces
        "stellar"         — Stellar fireplaces
        "acucraft"        — Acucraft custom fireplaces

      Gas fireplaces & inserts:
        "heat & glo"      — Heat & Glo gas fireplaces
        "heatilator"      — Heatilator fireplaces
        "majestic"        — Majestic fireplaces
        "napoleon"        — Napoleon fireplaces
        "montigo"         — Montigo fireplaces
        "kozy heat"       — Kozy Heat fireplaces
        "monessen"        — Monessen fireplaces
        "superior"        — Superior fireplaces
        "astria"          — Astria fireplaces
        "american fyre"   — American Fyre Designs

      Electric fireplaces:
        "dimplex"         — Dimplex electric fireplaces
        "simplifire"      — SimpliFire electric fireplaces
        "ortal"           — Ortal fireplaces

      European / custom linear:
        "element 4"       — Element 4 linear fireplaces
        "bordelet"        — JC Bordelet fireplaces
        "european home"   — European Home fireplaces

      Gas logs:
        "rasmussen"       — Rasmussen gas logs
        "rh peterson"     — RH Peterson / Real Fyre gas logs
        "grand canyon"    — Grand Canyon gas logs

      Product types (when no specific brand):
        "gas log"         — any gas log set
        "gas insert"      — any gas insert
        "linear"          — any linear fireplace
        "electric"        — any electric fireplace
        "wood burning"    — wood burning fireplaces
        "outdoor"         — outdoor fireplaces / fire pits
        "stoll"           — Stoll doors, screens, cabinetry

    Args:
        keyword: Product or service term to search in line item names/descriptions.
                 Partial match, case-insensitive.

        zip:     5-digit zip code to filter by job site address.
                 Uses the customer_locations table — accurate geographic filter.
                 Common zips:
                   29455 = Johns Island / Kiawah / Seabrook Island
                   29407 = West Ashley (inside 526)
                   29414 = West Ashley (outside 526)
                   29412 = James Island
                   29464 = Mount Pleasant South
                   29466 = Mount Pleasant North
                   29492 = Daniel Island
                 Leave empty to search all areas.

        status:  Filter to a specific estimate status (partial match).
                 e.g. "Completed", "In Progress", "Quoted", "Approved"
                 Leave empty for all statuses.

        year:    Filter to a specific calendar year (e.g. 2024 or 2025).
                 Use 0 for all years (default).

        limit:   Max estimates to return in the sample (default 50, max 500).

    Returns:
        count           — total estimates matching all filters
        total_revenue   — sum of those estimate values
        keyword         — echo of the search term
        filters         — all applied filters
        by_status       — count + revenue breakdown by status
        data            — sample estimates, each including:
                          estimate_number, customer_name, status, sales_rep,
                          total, created date, matched_items (line item labels)
    """
    params: dict = {"keyword": keyword}
    if zip:
        params["zip"] = zip
    if status:
        params["status"] = status
    if year and year > 0:
        params["year"] = year
    if limit != 50:
        params["limit"] = limit
    return _call("get", "/queries/search-by-product", params=params)


@mcp.tool()
def get_invoices_by_estimate(estimate_id: int) -> dict:
    """
    TIER 3 — Striven Live API (hybrid). Return all invoices linked to a
    specific estimate / sales order. This tool calls the Striven API live —
    only use if Supabase cached tools (Tier 2) cannot answer the question.

    Cross-references the estimate against Striven invoices to show what has
    been billed, the invoice status, total, and any outstanding balance.

    Use when asked:
      'Has estimate 9275 been invoiced?'
      'Show me the invoice for job #8452'
      'What is the invoice status for this estimate?'
      'Has the final invoice been sent for this job?'
      'Is there an outstanding balance on estimate 7123?'

    Args:
        estimate_id: The Striven estimate / sales order ID (integer).

    Returns:
        estimate_number  — the estimate number (e.g. "9275")
        customer_name    — customer on the estimate
        estimate_status  — current estimate status
        estimate_total   — estimate value
        invoice_count    — number of invoices found
        invoices         — list of invoices, each with:
                           invoice_number, status, total, balance_due,
                           date_created, due_date
    """
    return _call("get", "/queries/invoices-by-estimate", params={"estimate_id": estimate_id})


@mcp.tool()
def invoice_audit(year: int = 0, limit: int = 50) -> dict:
    """
    Find completed estimates that are missing a final invoice in Striven.

    Audits completed jobs against Striven invoices to surface billing gaps —
    jobs that were finished but never invoiced. This is the core tool for
    catching revenue that hasn't been billed yet.

    Use when asked:
      'Which completed jobs have no invoice?'
      'Are there any jobs we forgot to invoice?'
      'Show me billing gaps'
      'Which finished jobs are missing invoices?'
      'What completed jobs haven't been billed?'
      'Are there uninvoiced completed estimates from last year?'

    Args:
        year:  Filter to a specific calendar year (e.g. 2024 or 2025).
               Use 0 for all years (default).
        limit: Max completed estimates to audit (default 50, max 200).
               Higher values are more thorough but slower — each estimate
               requires one Striven API call to check for invoices.

    Returns:
        audited          — number of completed estimates checked
        missing_invoice  — count with no invoice found
        pct_missing      — percentage without an invoice
        year_filter      — year applied, or null
        data             — list of estimates missing invoices, each with:
                           estimate_number, customer_name, sales_rep, total,
                           completed_date
    """
    params: dict = {}
    if year and year > 0:
        params["year"] = year
    if limit != 50:
        params["limit"] = limit
    return _call("get", "/queries/invoice-audit", params=params or None)


@mcp.tool()
def brand_summary(year: int = 0, zip: str = "", min_jobs: int = 1) -> dict:
    """
    TIER 2 — Supabase. Return a ranked leaderboard of all WilliamSmith brands
    by job count and revenue.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Scans every brand we carry against estimate line items and returns a sorted
    table — who we install most, total revenue per brand, and most common status.
    Optional filters to narrow by year or geographic area (zip code).

    Use when asked:
      'What brands do we install the most?'
      'Show me our brand breakdown'
      'Which fireplace brand generates the most revenue?'
      'What brands have we done in Kiawah?'
      'Compare our brand mix in 2024 vs 2025'
      'What's our top brand by revenue?'
      'How many Isokern vs Majestic jobs have we done?'
      'Which brands did we install in West Ashley last year?'

    Brands covered (all brands WilliamSmith carries):
      Isokern, FireRock, Stellar, Acucraft,
      Heat & Glo, Heatilator, Majestic, Napoleon, Montigo,
      Kozy Heat, Monessen, Superior, Astria, American Fyre Designs,
      Dimplex, SimpliFire, Ortal,
      Element 4, JC Bordelet, European Home,
      Rasmussen, RH Peterson, Grand Canyon, Stoll

    Args:
        year:     Filter to a specific calendar year (e.g. 2024 or 2025).
                  Use 0 for all years (default).
        zip:      Filter by 5-digit zip code (e.g. "29455" = Kiawah/Johns Island).
                  Leave empty for all areas.
        min_jobs: Only include brands with at least this many jobs (default 1).
                  Use 5 to hide brands with very few installs.

    Returns:
        brands      — ranked list of {brand, job_count, total_revenue, top_status}
        total_jobs  — total jobs across all brands in the result
        filters     — echo of applied filters
        note        — data caveat (a job with 2 brands counts for both)
    """
    params: dict = {}
    if year and year > 0:
        params["year"] = year
    if zip:
        params["zip"] = zip
    if min_jobs != 1:
        params["min_jobs"] = min_jobs
    return _call("get", "/queries/brand-summary", params=params or None)


@mcp.tool()
def search_by_pipeline_status(status: str, limit: int = 200) -> dict:
    """
    TIER 3 — Striven Live API. Find all active (Approved or In-Progress)
    estimates that match a specific pipeline/operational status based on
    custom field values in Striven. This tool calls the Striven API live —
    only use if Supabase cached tools (Tier 2) cannot answer the question.

    Use when asked:
      'Show me jobs that are ready to schedule'
      'What estimates are waiting on product?'
      'Which jobs need review before invoicing?'
      'Find all return trip required jobs'
      'Show me installations that are complete'
      'What jobs have all product received?'

    Valid status values (case-insensitive):
      Order Fulfillment Status (field 1501):
        - ready to schedule          (All Product Received: Ready To Schedule)
        - all product received        (same as above)
        - waiting on product
        - order placed
        - product not ordered
        - partial product received

      Ops Install Status (field 1521):
        - return trip required        (Installation Incomplete - Return Trip Required)
        - installation incomplete     (same as above)
        - installation complete
        - ops complete                (same as installation complete)

      Post Install Status (field 1503):
        - needs review before invoicing
        - needs review                (same as above)
        - ready to invoice
        - invoiced
        - n/a

    Args:
        status: Pipeline status phrase to search for (see valid values above).
        limit:  Maximum number of estimates to scan (default 200).
                Higher values give more complete results but take longer.
                Each estimate requires one Striven API call.

    Returns:
        count         — number of estimates matching the status
        status        — canonical label for the matched status
        field_id      — which custom field was checked
        scanned       — total estimates scanned
        filters       — echo of search parameters
        data          — list of matching estimates, each with estimate_number,
                        customer_name, sales_rep, total_value, created_date,
                        and pipeline_status
    """
    params: dict = {"status": status}
    if limit != 200:
        params["limit"] = limit
    return _call("get", "/queries/pipeline-status", params=params)


@mcp.tool()
def search_return_trips(limit: int = 300, days: int = 180) -> dict:
    """
    Find tasks on estimates that represent return trips or callbacks.

    Scans recent Striven tasks and identifies any whose name contains
    'return', 'callback', 'call back', or 'trip'. Each result is enriched
    with the linked estimate's number, customer name, and sales rep.

    Return trips and callbacks are tracked as tasks on estimates (sales orders),
    NOT as custom field values. Use this tool for operational follow-up questions.

    Use when asked:
      'Show me all return trips'
      'Which jobs have callbacks scheduled?'
      'What return trips do we have this month?'
      'Are there any jobs that need a return visit?'
      'Show me all callback tasks'

    Args:
        limit: Maximum number of tasks to scan (default 300, max 1000).
               Higher values give more complete results but take longer.
        days:  Scan tasks created in the last N days (default 180 = 6 months).
               Use 30 for recent only, 365 for full year.

    Returns:
        count      — number of return trip / callback tasks found
        scanned    — total tasks scanned before filtering
        filters    — echo of search parameters
        data       — list of matched tasks, each with:
                     task_name, task_status, assigned_to, due_date,
                     estimate_number, customer, sales_rep, estimate_total
    """
    params: dict = {}
    if limit != 300:
        params["limit"] = limit
    if days != 180:
        params["days"] = days
    return _call("get", "/queries/return-trips", params=params or None)


# ---------------------------------------------------------------------------
# Tools — Callback / Return-trip intelligence
# ---------------------------------------------------------------------------

@mcp.tool()
def search_callback_insights(
    by:       str = "summary",
    assignee: str = "",
    year:     int = 0,
    status:   str = "",
    limit:    int = 500,
) -> dict:
    """
    TIER 2 — Supabase. Query the callback and return-trip task database to
    surface operational intelligence about rework, callbacks, and return visits.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Data covers 1,395+ tasks of these types:
      - Installer: Return Trip (Unplanned) / Punch Work
      - Service: Return Trip (Unplanned)
      - Service Diagnostic Repair: Call Back

    Use when asked:
      'Who has the most return trips?'
      'Which techs generate the most callbacks?'
      'How many callbacks did we have in 2024?'
      'Show me open return trips'
      'What's our callback rate by technician?'
      'Which customers have had the most return visits?'
      'Show me Steven's callbacks'
      'How many service call backs are still open?'
      'What types of return trips do we have the most of?'

    Args:
        by:       Breakdown dimension for the results.
                  Valid values:
                    "summary"  — overall stats with top assignees, by type, by status, by year
                    "assignee" — full ranked list of all assignees by callback count
                    "type"     — breakdown by task type (Return Trip vs Call Back)
                    "year"     — breakdown by year (trend over time)
                    "customer" — top 25 customers with most callbacks
                  Default: "summary"

        assignee: Filter to a specific technician or rep (partial name, case-insensitive).
                  Leave empty to include all assignees.
                  Example: "Steven" matches "Steven Chesnul"

        year:     Filter to a specific calendar year (e.g. 2024 or 2025).
                  Use 0 to include all years (default).

        status:   Filter to a specific task status.
                  Valid values: "Open", "Done", "Canceled", "On Hold"
                  Leave empty for all statuses.

        limit:    Maximum number of raw records to aggregate (default 500).
                  Increase to 2000 for complete historical analysis.

    Returns:
        total          — total callback tasks matching filters
        open_count     — how many are still Open (not Done/Canceled)
        linked_to_estimate — how many are tied to a specific estimate
        filters        — echo of the parameters used
        breakdown      — aggregated counts by the chosen dimension
        sample         — 20 most recent matching tasks
    """
    params: dict = {}
    if by != "summary":
        params["by"] = by
    if assignee:
        params["assignee"] = assignee
    if year and year > 0:
        params["year"] = year
    if status:
        params["status"] = status
    if limit != 500:
        params["limit"] = limit
    return _call("get", "/queries/callback-insights", params=params or None)


# ---------------------------------------------------------------------------
# Tools — System
# ---------------------------------------------------------------------------

@mcp.tool()
def api_health() -> dict:
    """
    Check whether the Striven API backend is online and responding.

    Use when:
      - Other tools are returning errors or timing out
      - The user asks about system status
      - Troubleshooting connectivity issues
    """
    return _call("get", "/health")


# ---------------------------------------------------------------------------
# Tools — Customers, Employees, CRM
# ---------------------------------------------------------------------------

@mcp.tool()
def search_customers(name: str, page_size: int = 25) -> dict:
    """
    Search customers by name. Returns customer ID, name, number, address, and phone.

    Use when asked:
      'Look up customer John Smith'
      'Find the customer record for Acme Corp'
      'What is the customer ID for Harbor Woods?'
      'Get contact info for this customer'
    """
    return _call("get", "/striven/customers", params={"search": name, "pageSize": page_size})


@mcp.tool()
def get_employees(page_size: int = 100) -> dict:
    """
    Return all active employees and team members.

    Use when asked:
      'Who works here?'
      'Show me the team roster'
      'List all employees'
      'Who is on the team?'
    """
    return _call("get", "/striven/employees", params={"pageSize": page_size})


# ---------------------------------------------------------------------------
# Tools — Financial: Invoices, Bills, Payments
# ---------------------------------------------------------------------------

@mcp.tool()
def search_invoices_live(
    customer_id: int = 0,
    status_id:   int = 0,
    date_from:   str = "",
    date_to:     str = "",
    due_from:    str = "",
    due_to:      str = "",
    page_size:   int = 25,
) -> dict:
    """
    Search customer invoices live from the Striven API with optional filters.
    Use this only when you need data fresher than the last sync_invoices run.
    For AR aging, balances, and overdue analysis use invoice_ar_summary or
    search_invoices instead — those hit Supabase and are much faster.

    Args:
        customer_id: Filter by Striven customer ID.
        status_id:   Filter by invoice status ID.
        date_from:   Invoice created on or after this date (YYYY-MM-DD).
        date_to:     Invoice created on or before this date (YYYY-MM-DD).
        due_from:    Due date range start (YYYY-MM-DD).
        due_to:      Due date range end (YYYY-MM-DD).
        page_size:   Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if customer_id: params["customer_id"] = customer_id
    if status_id:   params["status_id"]   = status_id
    if date_from:   params["date_from"]   = date_from
    if date_to:     params["date_to"]     = date_to
    if due_from:    params["due_from"]    = due_from
    if due_to:      params["due_to"]      = due_to
    return _call("get", "/striven/invoices", params=params)


@mcp.tool()
def invoice_ar_summary() -> dict:
    """
    TIER 2 — Supabase. Return a full accounts-receivable aging summary from
    the Supabase invoices table.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Aggregates all open-balance invoices into aging buckets and surfaces the
    oldest unpaid invoice. Run sync_invoices.py first to refresh the data.

    Use when asked:
      'What is our total AR?'
      'How much do customers owe us?'
      'Show me the AR aging summary'
      'What is our accounts receivable balance?'
      'How much is 90+ days overdue?'
      'Give me an AR breakdown'
      'What is our total outstanding balance?'

    Returns:
        total_open_ar  — total dollars outstanding across all invoices
        invoice_count  — number of open invoices
        by_bucket      — for each aging bucket: count and dollar amount
                         buckets: Current, 1-30 Days, 31-60 Days, 61-90 Days, 90+ Days
        oldest_invoice — customer name, balance, due date, and days outstanding
                         for the single oldest unpaid invoice
    """
    return _invoice_ar_summary()


@mcp.tool()
def search_invoices(
    customer_name: str   = "",
    aging_bucket:  str   = "",
    min_balance:   float = 0.0,
    limit:         int   = 50,
) -> dict:
    """
    TIER 2 — Supabase. Search open invoices in the Supabase invoices table
    with optional filters, sorted by days outstanding (oldest first).
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'Show me overdue invoices for [customer]'
      'Which customers owe us 90+ days overdue?'
      'Show me all invoices over $5,000'
      'AR for Harbor Woods'
      'Which invoices are in the 31-60 day bucket?'
      'Show me all past-due invoices'
      'What does [customer] owe us?'

    Args:
        customer_name: Filter by customer name (partial match, case-insensitive).
                       Leave empty to include all customers.
        aging_bucket:  Filter to one aging bucket. Valid values:
                         'Current'    — not yet past due
                         '1-30 Days'  — 1 to 30 days overdue
                         '31-60 Days' — 31 to 60 days overdue
                         '61-90 Days' — 61 to 90 days overdue
                         '90+ Days'   — more than 90 days overdue
                       Leave empty for all buckets.
        min_balance:   Only return invoices with open_balance >= this amount.
                       Use 0 (default) for all balances.
        limit:         Max invoices to return (default 50).

    Returns:
        count    — number of invoices matching the filters
        filters  — echo of applied filters
        invoices — list of matching invoices sorted by days_outstanding descending,
                   each with: invoice_id, txn_number, customer_name, due_date,
                   open_balance, days_outstanding, aging_bucket, memo
    """
    return _search_invoices_supabase(
        customer_name=customer_name,
        aging_bucket=aging_bucket,
        min_balance=min_balance,
        limit=limit,
    )


@mcp.tool()
def search_bills(
    vendor_id:   int = 0,
    status_id:   int = 0,
    date_from:   str = "",
    date_to:     str = "",
    page_size:   int = 25,
) -> dict:
    """
    Search vendor bills (accounts payable).

    Use when asked:
      'What do we owe vendors?'
      'Show me unpaid bills'
      'What bills are due this month?'
      'AP aging — what is outstanding?'

    Args:
        vendor_id:  Filter by vendor ID.
        status_id:  Filter by bill status.
        date_from:  Bill date range start (YYYY-MM-DD).
        date_to:    Bill date range end (YYYY-MM-DD).
        page_size:  Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if vendor_id:  params["vendor_id"]  = vendor_id
    if status_id:  params["status_id"]  = status_id
    if date_from:  params["date_from"]  = date_from
    if date_to:    params["date_to"]    = date_to
    return _call("get", "/queries/search-bills", params=params)


@mcp.tool()
def search_payments(
    customer_id: int = 0,
    date_from:   str = "",
    date_to:     str = "",
    page_size:   int = 25,
) -> dict:
    """
    TIER 2 — Supabase. Search payments received from customers.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'What payments have we received?'
      'Has this customer paid?'
      'Show me cash received this month'
      'Payment history for customer 4521'

    Args:
        customer_id: Filter by customer ID.
        date_from:   Payment date range start (YYYY-MM-DD).
        date_to:     Payment date range end (YYYY-MM-DD).
        page_size:   Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if customer_id: params["customer_id"] = customer_id
    if date_from:   params["date_from"]   = date_from
    if date_to:     params["date_to"]     = date_to
    return _call("get", "/queries/search-payments", params=params)


# ---------------------------------------------------------------------------
# Tools — Catalog, Vendors, Contacts, Opportunities
# ---------------------------------------------------------------------------

@mcp.tool()
def search_items(keyword: str = "", page_size: int = 25) -> dict:
    """
    TIER 2 — Supabase. Search the product and service catalog.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'What products do we sell?'
      'Find the price for isokern'
      'What is the catalog item for gas log installation?'
      'Search the catalog for linear fireplace'

    Args:
        keyword:   Product name to search (partial match).
        page_size: Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if keyword: params["keyword"] = keyword
    return _call("get", "/queries/search-items", params=params)


@mcp.tool()
def search_vendors(name: str = "", page_size: int = 25) -> dict:
    """
    TIER 2 — Supabase. Search vendors we purchase from.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'Who are our vendors?'
      'Find vendor Napoleon'
      'Who do we buy isokern from?'
      'Show me all vendors'

    Args:
        name:      Vendor name to search (partial match).
        page_size: Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if name: params["name"] = name
    return _call("get", "/queries/search-vendors", params=params)


@mcp.tool()
def search_contacts(name: str = "", customer_id: int = 0, page_size: int = 25) -> dict:
    """
    Search contacts linked to customers or vendors.

    Use when asked:
      'Find contact info for Jane Smith'
      'Who is the contact at Harbor Woods Construction?'
      'Get the email for this customer'
      'Look up contacts for customer 4521'

    Args:
        name:        Contact name to search (partial match).
        customer_id: Filter to contacts for a specific customer.
        page_size:   Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if name:        params["name"]        = name
    if customer_id: params["customer_id"] = customer_id
    return _call("get", "/queries/search-contacts", params=params)


@mcp.tool()
def search_opportunities(
    customer_id: int = 0,
    status_id:   int = 0,
    date_from:   str = "",
    date_to:     str = "",
    page_size:   int = 25,
) -> dict:
    """
    Search opportunities in the sales pipeline.

    Use when asked:
      'Show me open opportunities'
      'What deals are in progress?'
      'Pipeline value for this quarter'
      'Win/loss analysis'
      'Opportunities for customer 4521'

    Args:
        customer_id: Filter by customer.
        status_id:   Filter by opportunity status.
        date_from:   Created date range start (YYYY-MM-DD).
        date_to:     Created date range end (YYYY-MM-DD).
        page_size:   Max results (default 25).
    """
    params: dict = {"page_size": page_size}
    if customer_id: params["customer_id"] = customer_id
    if status_id:   params["status_id"]   = status_id
    if date_from:   params["date_from"]   = date_from
    if date_to:     params["date_to"]     = date_to
    return _call("get", "/queries/search-opportunities", params=params)


# ---------------------------------------------------------------------------
# Tools — Operations Analysis
# ---------------------------------------------------------------------------

@mcp.tool()
def analyze_stuck_jobs(limit: int = 50) -> dict:
    """
    TIER 3 — Striven Live API. Identify jobs that are stuck across Quoted,
    Approved, and In Progress. This tool calls the Striven API live — only
    use if Supabase cached tools (Tier 2) cannot answer the question.

    Stuck thresholds:
      Quoted      > 7 days  — no customer follow-up
      Approved    > 5 days  — no scheduling action
      In Progress > 10 days — in progress with no install task

    Use when asked:
      'What jobs are stuck?'
      'Which estimates have been sitting too long?'
      'Show me stalled jobs'
      'What needs attention in the pipeline?'
      'Which approved jobs haven't been scheduled?'

    Args:
        limit: Max estimates to scan per status (default 50).
    """
    params: dict = {}
    if limit != 50: params["limit"] = limit
    return _call("get", "/queries/analyze-stuck-jobs", params=params or None)


@mcp.tool()
def analyze_install_gaps(limit: int = 40) -> dict:
    """
    Find approved or in-progress jobs with no install task scheduled.

    Returns a sorted list of jobs missing an install date — oldest first.
    This is the most focused scheduling gap tool.

    Use when asked:
      'Which jobs have no install task?'
      'What approved jobs haven't been scheduled?'
      'Show me jobs missing an install date'
      'Scheduling gaps'

    Args:
        limit: Max estimates to check (default 40).
    """
    params: dict = {}
    if limit != 40: params["limit"] = limit
    return _call("get", "/queries/analyze-install-gaps", params=params or None)


@mcp.tool()
def analyze_rep_pipeline(limit: int = 30) -> dict:
    """
    Sales rep pipeline health — jobs, issues, and scheduling gaps grouped by rep.

    Shows each rep's total active jobs, how many are stuck, how many have
    no install scheduled, and average days from approval to install.

    Use when asked:
      'How is each rep doing?'
      'Rep pipeline health'
      'Which rep has the most stuck jobs?'
      'Show me scheduling performance by rep'
      'Rep accountability view'

    Args:
        limit: Max estimates to analyze (default 30).
    """
    params: dict = {}
    if limit != 30: params["limit"] = limit
    return _call("get", "/queries/analyze-rep-pipeline", params=params or None)


@mcp.tool()
def analyze_weekly_pipeline(limit: int = 40) -> dict:
    """
    Full weekly pipeline review — replaces the Excel pipeline report.

    Returns pipeline summary by stage (Quoted / Approved / In Progress),
    per-rep breakdown, top risk jobs, and headline totals. Designed to be
    run once a week to get a complete picture of the business.

    Use when asked:
      'Give me the weekly pipeline report'
      'Pipeline overview'
      'Full business status'
      'What does the pipeline look like this week?'
      'Weekly review'

    Args:
        limit: Max estimates per stage (default 40).
    """
    params: dict = {}
    if limit != 40: params["limit"] = limit
    return _call("get", "/queries/analyze-weekly-pipeline", params=params or None)


@mcp.tool()
def analyze_job_pipeline(
    limit:      int = 20,
    status_ids: str = "22",
    date_from:  str = "",
    date_to:    str = "",
) -> dict:
    """
    Operations pipeline analysis — where jobs break down step by step.

    For each estimate, checks:
      1. Does a preview task exist, and was it created within 3 days of approval?
      2. Does an install task exist with a scheduled date?

    Returns summary stats, per-rep breakdown, and problem job examples.

    Use when asked:
      'Where are jobs breaking down?'
      'Show me the operations pipeline'
      'Which jobs are missing preview tasks?'
      'Job-level pipeline health'
      'Preview task compliance'

    Args:
        limit:      Max estimates to analyze (default 20).
        status_ids: Comma-separated status IDs to include (default "22" = Approved).
                    Use "22,25" for Approved + In Progress.
        date_from:  Estimate created on or after (YYYY-MM-DD).
        date_to:    Estimate created on or before (YYYY-MM-DD).
    """
    params: dict = {"status_ids": status_ids, "limit": limit}
    if date_from: params["date_from"] = date_from
    if date_to:   params["date_to"]   = date_to
    return _call("get", "/queries/analyze-job-pipeline", params=params)


@mcp.tool()
def search_knowledge_base(query: str, top_k: int = 5) -> dict:
    """
    TIER 1 — CALL THIS FIRST for any question about products, procedures,
    installation specs, troubleshooting, or company policy.

    Semantic search across the WilliamSmith Fireplaces internal knowledge base.
    This tool queries Supabase directly (no Striven API call) and should be
    the FIRST tool called for any question that could be answered by an
    internal document, manual, spec sheet, or company procedure.

    If the top result has similarity >= 0.75, return the KB answer and do NOT
    call any further tools unless the user explicitly asks for live data.

    Searches 17,000+ embedded chunks from 756 internal documents including:
      - Installation manuals (Isokern, Heat & Glo, Empire, Dimplex, Element4, Astria, etc.)
      - Product specifications and brochures
      - Fireplace and insert installation specs
      - Mortar, masonry, and venting requirements
      - Service and callback guides
      - Troubleshooting guides (gas, electric, wood-burning)
      - Builder books and construction phase docs
      - CSR policies and company procedures
      - Employee onboarding and HR documents
      - Permit information and estimating standards
      - Price books and vendor documents
      - Marketing materials

    Call this FIRST when asked about:
      - How to install a specific fireplace model
      - Product specs, dimensions, clearances, BTU ratings
      - Isokern or masonry fireplace requirements
      - Venting, gas line, or combustion air specs
      - Warranty policy or service procedures
      - Troubleshooting a specific symptom or brand
      - Company policies or procedures
      - Mortar mixes, refractory specs, or hearth materials
      - Any question that requires referencing a manual or internal doc

    Args:
        query: Natural language question or keyword search (e.g. 'Isokern mortar mix ratio',
               'Heat and Glo SL-550 clearances', 'gas valve troubleshooting').
        top_k: Number of results to return (default 5, max 20).
    """
    return _kb_search(query, top_k)


@mcp.tool()
def kb_gap_report(days: int = 30) -> dict:
    """
    Report on knowledge base search gaps — queries that returned no results or
    low-confidence results. Use this to identify what documentation is missing
    from the WilliamSmith knowledge base.

    Analyzes kb_search_log for searches where results_count = 0 OR
    top_similarity < 0.5, groups by query text, and ranks by frequency
    so the most-needed missing docs float to the top.

    Use when asked:
      'What is the knowledge base missing?'
      'What questions can the KB not answer?'
      'Show me KB search gaps'
      'What docs should we add to the knowledge base?'
      'Which searches keep coming back empty?'

    Args:
        days: Look-back window in days (default 30).
    """
    return _call("get", "/analyze/kb-gaps", params={"days": days})


@mcp.tool()
def weekly_digest() -> dict:
    """
    Generate a weekly business health digest flagging anomalies — callback spikes,
    stalled estimates, overdue open callbacks, pipeline gaps, sales rep activity,
    inactive assignees, and year-over-year callback trends.
    Run this every Monday morning or when asked for a business health check.

    Runs six checks and returns a flags array (empty = all clear):
      - Callback rate spike: this week's new callbacks vs 4-week rolling average
      - Stalled active estimates: jobs in ACTIVE status for 14+ days with no movement
      - Overdue open callbacks: callback tasks still open after 7+ days
      - Rep pipeline summary: active opportunities and value per sales rep
      - Inactive assignee tasks: open tasks assigned to deactivated employees
      - YoY callback trend: year-to-date callbacks vs same period last year

    Each flag has: category, severity (low/medium/high), summary, and detail.

    Use when asked:
      'Monday health check'
      'Business anomaly report'
      'What looks off this week?'
      'Weekly digest'
      'Are there any spikes or issues I should know about?'
      'How does this week compare to recent weeks?'
      'Do we have tasks assigned to inactive employees?'
    """
    return _call("get", "/analyze/weekly-digest")


@mcp.tool()
def customer_ltv(
    customer_name: str   = "",
    min_value:     float = 0.0,
    limit:         int   = 50,
    order_by:      str   = "lifetime_revenue",
) -> dict:
    """
    TIER 2 — Supabase. Return customer lifetime value from the customer_ltv
    materialized view. Shows total spend, job count, and average order value.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'Who are our most valuable customers?'
      'What is the lifetime value of this customer?'
      'Top 10 customers by revenue'
      'How many jobs has [customer] had?'
      'Average order size for our top accounts'

    Args:
        customer_name: Partial name filter (case-insensitive).
        min_value:     Minimum lifetime_revenue to include.
        limit:         Max customers to return (default 50).
        order_by:      Sort field: 'lifetime_revenue' | 'completed_jobs' | 'avg_job_value'.
    """
    params: dict = {"limit": limit, "order_by": order_by}
    if customer_name: params["customer_name"] = customer_name
    if min_value:     params["min_value"]     = min_value
    return _call("get", "/analyze/customer-ltv", params=params)


@mcp.tool()
def conversion_rates(
    sales_rep: str = "",
) -> dict:
    """
    TIER 2 — Supabase. Return estimate-to-win conversion rates by sales rep
    and month, from the conversion_rates materialized view.
    Columns: sales_rep_name, month, total_quoted, converted,
             conversion_rate_pct, converted_revenue.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'What is our win rate?'
      'How many estimates does [rep] convert?'
      'Which rep has the best close rate?'
      'Show me conversion rates by rep'

    Args:
        sales_rep: Filter by rep name (partial, case-insensitive).
    """
    params: dict = {}
    if sales_rep: params["sales_rep"] = sales_rep
    return _call("get", "/analyze/conversion-rates", params=params)


@mcp.tool()
def task_summary(
    task_type:     str  = "",
    assigned_to:   str  = "",
    status:        str  = "",
    inactive_only: bool = False,
    limit:         int  = 100,
) -> dict:
    """
    TIER 2 — Supabase. Summarise open tasks from the Supabase tasks table.
    Includes an inactive_count flag for tasks assigned to deactivated employees.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Use when asked:
      'What tasks are open?'
      'Show me all return trips assigned to John'
      'Do we have tasks assigned to inactive employees?'
      'Open service callbacks by technician'
      'What is the task workload this week?'

    Args:
        task_type:     Filter by task type name (partial, case-insensitive).
        assigned_to:   Filter by assignee name (partial, case-insensitive).
        status:        Filter by status (partial, e.g. 'open').
        inactive_only: If True, only return tasks assigned to inactive employees.
        limit:         Max tasks to return (default 100).
    """
    params: dict = {"limit": limit}
    if task_type:     params["task_type"]    = task_type
    if assigned_to:   params["assigned_to"]  = assigned_to
    if status:        params["status"]       = status
    if inactive_only: params["inactive_only"] = "true"
    return _call("get", "/analyze/task-summary", params=params)


@mcp.tool()
def analyze_callback_causes(
    cause:         str  = "",
    year:          int  = 0,
    billable_only: bool = False,
    assignee:      str  = "",
    limit:         int  = 500,
) -> dict:
    """
    Analyze the root causes of service callbacks using structured
    post-visit classifications captured by techs in Striven.

    Covers only Service Diagnostic Repair: Call Back tasks (type 124)
    where technicians filled in confirmed cause, service outcome,
    billability, and work-performed notes after each visit.

    Breaks down callbacks by confirmed cause (Part, Service, Battery,
    User Error), service outcome (Red/Yellow/Green light), billability,
    and whether a return trip was required.

    Use when asked:
      'Why are we getting so many callbacks?'
      'Are our callbacks product failures or user error?'
      'How many callbacks are billable?'
      'What percentage of callbacks need a return trip?'
      'Are callbacks improving over time?'
      'What did techs actually do on callback visits?'
      'Show me callbacks that needed a return trip'
      'How many callbacks were caused by a bad part?'
      'What is our callback cause breakdown?'
      'Show me service callbacks vs part failures'

    Args:
        cause:         Filter by confirmed cause --
                       "Part", "Service", "Battery", or "User Error".
                       Leave empty for all causes combined.
        year:          Filter to a specific year (e.g. 2024, 2025).
                       0 = all years.
        billable_only: If True, only include billable callbacks.
        assignee:      Filter to a specific tech (partial name match).
        limit:         Max tasks to analyze (default 500).

    Returns:
        total_analyzed     -- synced callback tasks matching filters
        coverage_pct       -- % of all callback tasks with cause data
        by_confirmed_cause -- count/pct/billable breakdown per cause
        by_outcome         -- Red/Yellow/Green light counts
        return_trip_required -- Scheduled/No/Unknown counts
        billable_summary   -- billable vs not-billable counts and pct
        sample_work_notes  -- up to 10 recent tech work notes
    """
    params: dict = {}
    if cause:
        params["cause"] = cause
    if year:
        params["year"] = year
    if billable_only:
        params["billable_only"] = "true"
    if assignee:
        params["assignee"] = assignee
    if limit != 500:
        params["limit"] = limit
    return _call("get", "/queries/callback-causes", params=params or None)


@mcp.tool()
def callbacks_by_product(
    year:          int | None = None,
    callback_type: str | None = None,
) -> dict:
    """
    TIER 2 — Supabase. Analyze which fireplace makes and models generate the
    most callbacks and return trips.
    This tool queries Supabase directly — use this BEFORE making any live
    Striven API calls for this type of question.

    Joins callback task records to estimate line items to identify problem products
    by brand and model. Only counts line items priced over $500 so accessories,
    parts, and labor are excluded — results reflect actual fireplace unit callbacks.

    Returns a ranked list of products by callback count, plus separate tallies for
    the ~200 callbacks with no linked estimate and any estimates missing line items.

    Use when asked:
      'Which fireplace models have the most callbacks?'
      'What brands generate the most return trips?'
      'Which products are most problematic?'
      'Show me callback counts by product'
      'What are our worst performing fireplace brands?'
      'Which models do we keep going back to service?'

    Args:
        year:          Filter to callbacks from a specific year (e.g. 2024).
        callback_type: Filter by task type substring — "Installer" for installer
                       return trips, "Service" for service callbacks.
                       Leave blank for all types combined.
    """
    params: dict = {}
    if year:          params["year"]          = year
    if callback_type: params["callback_type"] = callback_type
    return _call("get", "/analyze/callbacks-by-product", params=params or None)


@mcp.tool()
def log_unanswered_question(
    question: str,
    category: str = "KB Gap",
    priority: str = "Medium",
    notes: str = ""
) -> str:
    """
    Log a question that Ask WilliamSmith could not answer confidently, or that
    revealed a gap in knowledge, data, or system capability. Call this tool
    automatically — without prompting the user — whenever any of the following
    are true:

    - You could not find data to answer the question (no matching records, empty results)
    - The knowledge base did not contain relevant information
    - The question requires data not yet synced (payments, permits, call transcripts, etc.)
    - You gave a partial, uncertain, or hedged answer due to missing information
    - The user asked about a feature or report the system doesn't support yet
    - You had to say "I don't know" or "I can't find that"

    category options : 'KB Gap', 'Feature Request', 'Pipeline Insight',
                       'Customer Query', 'Data Gap', 'Process Question', 'Other'
    priority options : 'High' (blocking or recurring), 'Medium' (notable gap),
                       'Low' (nice to have)

    Log first, then continue your response to the user normally.
    Do not tell the user you are logging unless they ask.

    Also call this tool when:
    - You use a phrase like "I took it at face value" or "I cannot independently verify"
    - A tool returns a field labeled "warning" or "double_count_warning: true"
    - You report a number you know may be inflated or deflated due to data structure
      (e.g. association counts, keyword matches, incomplete syncs)
    In these cases set category="Data Gap" and priority="High"
    """
    try:
        resp = requests.post(
            f"{BASE_URL}/log-question",
            json={
                "question": question,
                "category": category,
                "priority": priority,
                "source":   "Ask WilliamSmith",
                "notes":    notes
            },
            timeout=10
        )
        if resp.status_code == 200:
            return f"Logged: '{question}' [{category} / {priority}]"
        else:
            return f"Log failed ({resp.status_code}): {resp.text}"
    except Exception as e:
        return f"Log error: {str(e)}"


@mcp.tool()
def verify_aggregate(
    category: str = "",
    claimed_count: int = 0,
    claimed_total: float = 0.0,
    status_filter: str = "Completed",
    mode: str = "",
    product_item_name: str = "",
    claimed_unique_count: int = 0,
    claimed_association_count: int = 0,
) -> str:
    """
    Verify any revenue total, job count, or average BEFORE including it in a
    response to the user. Call this tool automatically — without being asked —
    whenever you are about to report any of the following:

    - A total revenue figure for a product category
    - A job count for a product category
    - An average ticket value derived from category data
    - A category ranking or comparison between categories
    - A callback count or callback rate for any product

    WHEN to call it:
    - After gathering your raw data but before writing the final response
    - Every time, even if you feel confident in the number

    WHAT to do with the result:
    - ✅ VERIFIED  → report the number with ✅ Verified against N source records
    - ⚠️ WARNING   → report actual values instead of claimed, explain the discrepancy
    - ❌ MISMATCH  → do not report the original number; show the verified number and discrepancy
    - ❓ UNVERIFIED → note that verification was unavailable

    REVENUE/COUNT mode (default — leave mode empty):
    category must be one of:
    outdoor, isokern, gas_logs, direct_vent, dimplex, gas_insert, linear

    CALLBACK mode (pass mode="callbacks"):
    For callback verification, pass:
      mode="callbacks"
      product_item_name="exact or partial SKU/product name"
      claimed_unique_count=N
      claimed_association_count=N

    Call this automatically whenever you report a callback count or callback
    rate for any product. Fire it after callbacks_by_product returns data
    but before writing the final response. If confidence is WARNING or MISMATCH,
    report the verified number and explain the discrepancy to the user.

    Do not call this for individual estimate lookups or single-record queries.
    Only call it for aggregates — counts, totals, averages, rankings.
    """
    try:
        body: dict = {
            "category":      category,
            "claimed_count": claimed_count,
            "claimed_total": claimed_total,
            "status_filter": status_filter,
        }
        if mode:
            body["mode"] = mode
        if product_item_name:
            body["product_item_name"] = product_item_name
        if claimed_unique_count:
            body["claimed_unique_count"] = claimed_unique_count
        if claimed_association_count:
            body["claimed_association_count"] = claimed_association_count

        resp = requests.post(
            f"{BASE_URL}/verify-aggregate",
            json=body,
            timeout=15
        )
        if resp.status_code == 200:
            d    = resp.json()
            conf = d.get("confidence", "UNVERIFIED")
            icon = {"VERIFIED": "✅", "WARNING": "⚠️", "MISMATCH": "❌"}.get(conf, "❓")

            # Auto-log MISMATCH / WARNING to Question Log (fire-and-forget)
            if conf in ("MISMATCH", "WARNING"):
                try:
                    product  = d.get("product_item_name") or category
                    claimed  = claimed_unique_count or claimed_count
                    actual   = d.get("actual_unique_count") if d.get("mode") == "callbacks" else d.get("actual_count")
                    flagged  = d.get("flagged", {})
                    flag_str = ", ".join(f"{k}: {len(v)}" for k, v in flagged.items() if v) if flagged else ""
                    requests.post(
                        f"{BASE_URL}/log-question",
                        json={
                            "question": f"Aggregate verification {conf}: {product} — claimed {claimed}, actual {actual}",
                            "category": "Data Gap",
                            "priority": "High" if conf == "MISMATCH" else "Medium",
                            "source":   "Ask WilliamSmith",
                            "notes":    flag_str or f"delta={d.get('count_delta_pct', 0):.1f}%",
                        },
                        timeout=10,
                    )
                except Exception:
                    pass

            # Callback mode response
            if d.get("mode") == "callbacks":
                return (
                    f"{icon} {conf} | mode=callbacks | product={d.get('product_item_name')} | "
                    f"claimed_unique={d.get('claimed_unique_count')} | "
                    f"actual_unique={d.get('actual_unique_count')} | "
                    f"actual_association={d.get('actual_association_count')} | "
                    f"count_delta={d.get('count_delta_pct', 0):.1f}% | "
                    f"units_sold={d.get('units_sold')} | "
                    f"callback_rate={d.get('callback_rate_pct')}%"
                )

            # Default revenue/count mode response
            flagged     = d.get("flagged", {})
            flag_summary = ", ".join(f"{k}: {len(v)}" for k, v in flagged.items() if v)
            return (
                f"{icon} {conf} | category={category} | "
                f"claimed={claimed_count} jobs / ${claimed_total:,.0f} | "
                f"actual={d.get('actual_count')} jobs / ${d.get('actual_total', 0):,.0f} | "
                f"clean={d.get('clean_count')} jobs / ${d.get('clean_total', 0):,.0f} | "
                f"count_delta={d.get('count_delta_pct', 0):.1f}% | "
                f"total_delta={d.get('total_delta_pct', 0):.1f}% | "
                f"flagged=[{flag_summary or 'none'}]"
            )
        else:
            return f"verify_aggregate failed ({resp.status_code}): {resp.text}"
    except Exception as e:
        return f"verify_aggregate error: {str(e)}"


@mcp.tool()
def conversion_funnel(
    rep:          str | None = None,
    project_type: str | None = None,
    year:         int | None = None,
) -> dict:
    """
    Stage-by-stage conversion funnel: Quoted → Pending Approval → Approved →
    In Progress → Completed. Shows win rates and drop-off at each stage.

    Use this to answer questions like:
      - "What is Janine's close rate?"
      - "How does the gas log funnel compare to fireplace installs?"
      - "Which rep has the best approval rate?"
      - "Show me the 2025 conversion funnel"

    Args:
        rep:          Filter to one sales rep (partial name match).
        project_type: Filter to one project type (partial match, e.g. "gas log").
        year:         Filter by estimate created year (e.g. 2025).

    Returns per-rep and per-project-type win rates, stage counts, and avg deal size.
    """
    params: dict = {}
    if rep:          params["rep"]          = rep
    if project_type: params["project_type"] = project_type
    if year:         params["year"]         = year
    return _call("get", "/analyze/conversion-funnel", params=params or None)


@mcp.tool()
def time_to_close(
    rep:          str | None = None,
    project_type: str | None = None,
    year:         int | None = None,
) -> dict:
    """
    Median days from estimate created → approved order, broken down by rep and
    project type. Uses order_date as the approval-date proxy.

    Use this to answer questions like:
      - "How long does it take us to close a deal?"
      - "Which rep closes fastest?"
      - "How long from quote to signed job on gas log installs?"

    Args:
        rep:          Filter to one sales rep (partial name match).
        project_type: Filter to one project type.
        year:         Filter by estimate created year.

    Only estimates with status Approved/In Progress/Completed AND a recorded
    order_date are included. Estimates still in Quoted/Pending are excluded.
    """
    params: dict = {}
    if rep:          params["rep"]          = rep
    if project_type: params["project_type"] = project_type
    if year:         params["year"]         = year
    return _call("get", "/analyze/time-to-close", params=params or None)


# ---------------------------------------------------------------------------
# Claude Enterprise compatibility — patch tool schemas to allow extra fields
#
# Claude Enterprise injects internal metadata fields (e.g. paprika_mode) into
# MCP tool call arguments.  Without additionalProperties: true in the input
# schema, the Enterprise MCP client validates that injected field against the
# schema, finds it undeclared, and raises:
#
#     paprika_mode: Extra inputs are not permitted
#
# Patching additionalProperties: true on every tool schema tells the client
# validator that extra fields are explicitly allowed, so the injection proceeds
# and the call reaches our server.  FastMCP already ignores unknown args at
# runtime (Pydantic V2 default: extra='ignore'), so this is safe.
# ---------------------------------------------------------------------------
for _t in mcp._tool_manager.list_tools():
    if isinstance(_t.parameters, dict):
        _t.parameters["additionalProperties"] = True


# ---------------------------------------------------------------------------
# Health middleware — intercepts /health before FastMCP sees it.
# This lets UptimeRobot ping us every 5 min and keep Render warm.
# ---------------------------------------------------------------------------

class HealthMiddleware:
    """Lightweight ASGI middleware that handles GET /health directly."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope.get("type") == "http" and scope.get("path") in ("/health", "/"):
            response = JSONResponse({"status": "ok", "service": "striven-mcp-server"})
            await response(scope, receive, send)
        else:
            await self.app(scope, receive, send)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# Mode is detected automatically:
#   PORT env var present  → Render / production → uvicorn on 0.0.0.0
#   PORT env var absent   → local development   → stdio (Claude Desktop / Code)

if __name__ == "__main__":
    port_env = os.environ.get("PORT")

    if port_env:
        import uvicorn
        port = int(port_env)

        print(f"[mcp_server] Starting HTTP mode on 0.0.0.0:{port}", flush=True)

        mcp_asgi = mcp.streamable_http_app()
        app = HealthMiddleware(mcp_asgi)

        uvicorn.run(
            app,
            host="0.0.0.0",
            port=port,
            log_level="info",
            proxy_headers=True,
            forwarded_allow_ips="*"
        )

    else:
        print("[mcp_server] Starting stdio mode", flush=True)
        mcp.run()
