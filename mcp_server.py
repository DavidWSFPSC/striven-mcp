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
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("FLASK_API_URL", "https://striven-mcp-v2.onrender.com")
TIMEOUT  = 30  # seconds — allow for cold-start latency on Render free tier

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
- jobs_by_location             → job count + revenue for a specific location/area
- time_to_preview              → average days from estimate creation to site preview
- search_by_pipeline_status   → find active jobs by operational status (ready to schedule, waiting on product, etc.)

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
- "How much work do we have in Kiawah?"           → jobs_by_location
- "How long does it take to schedule a preview?"  → time_to_preview
- "Show me jobs ready to schedule"                → search_by_pipeline_status
- "What jobs are waiting on product?"             → search_by_pipeline_status
- "Which jobs need review before invoicing?"      → search_by_pipeline_status

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
    Get the total number of estimates stored in the database.

    Use when asked:
      'How many estimates do we have?'
      'What is our total estimate count?'
    """
    return _call("get", "/estimates/count")


@mcp.tool()
def high_value_estimates() -> dict:
    """
    Return up to 25 estimates where the total value exceeds $10,000,
    sorted from highest to lowest.

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
    Flexible estimate search with optional filters.

    Use when asked about estimates filtered by status, date range, or name.

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
        keyword:     Filter by estimate name or number keyword.
        page_size:   Number of results to return (default 25, max 100).
        page:        Page number, 1-based (default 1).

    Examples:
      search_estimates(status=22)                          → all Approved
      search_estimates(status=19, date_from='2025-01-01') → Quoted this year
      search_estimates(keyword='roof')                     → name contains 'roof'
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
    Audit all estimates and return those missing the Customer Portal display flag.

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
def jobs_by_location(location: str, year: int | None = None) -> dict:
    """
    Get job counts and total revenue for a specific location or area name.

    Searches Striven for customers whose name matches the location keyword,
    then pulls all their estimates and aggregates by customer. Use this to
    answer questions about a specific neighborhood, island, city, or development.

    Use when asked:
      'How much work do we have in Kiawah?'
      'Show me jobs in Mount Pleasant'
      'What is our revenue from Isle of Palms customers?'
      'How many jobs have we done in Daniel Island?'

    Args:
        location: Location keyword to search — can be a neighborhood, city, or
                  partial name (e.g. 'Kiawah', 'Mount Pleasant', 'Daniel Island').
                  Matched against customer names — not a geographic address search.
        year:     Optional calendar year to filter results (e.g. 2024 or 2025).
                  Omit to return all years.

    Returns:
        count   — total estimates found for matching customers
        filters — echo of the search parameters used
        data    — per-customer summary: {customer_name, total_jobs, total_revenue,
                  active_jobs, completed_jobs}
        sample  — up to 25 most-recent individual estimates
    """
    params: dict = {"location": location}
    if year is not None:
        params["year"] = year
    return _call("get", "/queries/jobs-by-location", params=params)


@mcp.tool()
def time_to_preview() -> dict:
    """
    Get the average and median number of days from estimate creation to the
    first site preview / inspection task being scheduled.

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
def search_by_pipeline_status(status: str, limit: int = 200) -> dict:
    """
    Find all active (Approved or In-Progress) estimates that match a specific
    pipeline/operational status based on custom field values in Striven.

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
        if scope.get("type") == "http" and scope.get("path") == "/health":
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
