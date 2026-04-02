"""
mcp_server.py

MCP (Model Context Protocol) server for Striven business data.

This server exposes the deployed Flask API as Claude-callable tools.
Claude reads the tool descriptions and decides which to call based on
the user's natural language question.

Usage (after Render deployment):
  1. Update BASE_URL below to your Render URL
  2. Claude Desktop: registered via claude_desktop_config.json
  3. Claude Code:    registered via .claude/settings.json

All tools are READ-ONLY. No data is modified anywhere.
"""

import os
import sys
import requests
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# When deployed to Render, FLASK_API_URL env var overrides this default.
# Locally, it points to whatever is running on localhost.
BASE_URL = os.environ.get("FLASK_API_URL", "https://striven-api.onrender.com")

TIMEOUT = 20  # seconds — Render free tier can be slow on cold start

# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="Striven Business Data",
    instructions=(
        "You have access to a company estimates database. "
        "Use these tools to answer questions about estimates, customers, and job values. "
        "Always call a tool to get real data — do not guess or make up numbers."
    ),
)

# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def count_estimates() -> dict:
    """
    Get the total number of estimates stored in the database.
    Use this when asked: 'how many estimates do we have?'
    """
    return requests.get(f"{BASE_URL}/estimates/count", timeout=TIMEOUT).json()


@mcp.tool()
def high_value_estimates() -> dict:
    """
    Get estimates with a total value over $10,000, sorted highest first.
    Returns up to 25 records with estimate number, customer name, and total.
    Use this when asked about 'high value jobs', 'big estimates', or 'top estimates'.
    """
    return requests.get(f"{BASE_URL}/estimates/high-value", timeout=TIMEOUT).json()


@mcp.tool()
def search_estimates_by_customer(name: str) -> dict:
    """
    Search estimates by customer name. Case-insensitive, partial match supported.
    Use this when asked to find estimates for a specific customer or company.

    Args:
        name: Customer name or partial name to search for (e.g. 'Clear Water' or 'smith').
    """
    return requests.get(
        f"{BASE_URL}/estimates/by-customer",
        params={"name": name},
        timeout=TIMEOUT,
    ).json()


@mcp.tool()
def api_health() -> dict:
    """
    Check if the API is online and responding.
    Use this if other tools are failing or the user asks about system status.
    """
    return requests.get(f"{BASE_URL}/health", timeout=TIMEOUT).json()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
# Two modes:
#   python mcp_server.py          → stdio (Claude Desktop / Claude Code)
#   python mcp_server.py --http   → HTTP server (Render / Claude.ai integration)

if __name__ == "__main__":
    if "--http" in sys.argv:
        port = int(os.environ.get("PORT", 8000))
        mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
    else:
        mcp.run()
