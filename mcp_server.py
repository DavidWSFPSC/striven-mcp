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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_URL = os.environ.get("FLASK_API_URL", "https://striven-mcp-v2.onrender.com")
TIMEOUT  = 30  # seconds — allow for cold-start latency on Render free tier

# ---------------------------------------------------------------------------
# MCP server — identity + standing instructions for Claude
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="Ask WilliamSmith — Striven Business Intelligence",
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
- count_estimates          → total records in database
- high_value_estimates     → jobs over $10,000 (sorted highest first)
- search_estimates_by_customer → find all estimates for a specific customer
- search_estimates         → flexible search by status, date range, keyword
- get_estimate_by_id       → full detail on a single estimate
- portal_flag_audit        → find estimates missing the Customer Portal flag
- sync_estimates           → refresh the database from Striven (use sparingly)
- api_health               → check if the system is online

WHEN TO USE EACH TOOL
---------------------
- "How many estimates do we have?"           → count_estimates
- "Show me our biggest jobs"                 → high_value_estimates
- "What estimates do we have for Acme?"      → search_estimates_by_customer
- "Show me approved estimates this month"    → search_estimates with status=22
- "Tell me about estimate #4521"             → get_estimate_by_id
- "Which estimates are missing the portal flag?" → portal_flag_audit
- "The data seems outdated"                  → sync_estimates

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
# Entry point
# ---------------------------------------------------------------------------
# Mode is detected automatically:
#   PORT env var present  → Render / production → uvicorn on 0.0.0.0
#   PORT env var absent   → local development   → stdio (Claude Desktop / Code)
#
# We run uvicorn directly (instead of mcp.run()) so we can explicitly bind
# to host="0.0.0.0" — FastMCP.run() does not accept a host argument and
# uvicorn defaults to 127.0.0.1, which Render cannot reach.

if __name__ == "__main__":
    port_env = os.environ.get("PORT")
    if port_env:
        import uvicorn
        port = int(port_env)
        print(f"[mcp_server] Starting HTTP mode on 0.0.0.0:{port}", flush=True)
        # Get FastMCP's Starlette ASGI app and hand it to uvicorn directly.
        # This gives us full control over host/port binding.
        app = mcp.streamable_http_app()
        uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
    else:
        print("[mcp_server] Starting stdio mode", flush=True)
        mcp.run()
