"""
services/supabase_client.py

Supabase client wrapper.

Responsibilities:
  - Initialize the Supabase connection from environment variables
  - Provide write helpers used by the sync layer

NOTE: This module is intentionally named supabase_client.py (not supabase.py)
to avoid shadowing the `supabase` pip package on import.

READ/WRITE POLICY:
  - Writes go TO Supabase only (our own data layer)
  - No Striven data is ever modified from this file
"""

import os
from supabase import create_client, Client

# ---------------------------------------------------------------------------
# Client initialisation
# ---------------------------------------------------------------------------

def _init_client() -> Client:
    """
    Create and return a Supabase client.
    Raises a clear error at startup if env vars are missing.
    """
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url:
        raise RuntimeError("Environment variable 'SUPABASE_URL' is not set.")
    if not key:
        raise RuntimeError("Environment variable 'SUPABASE_KEY' is not set.")

    return create_client(url, key)


# Lazy singleton — created on first use so load_dotenv() in app.py always
# runs before we try to read the environment variables.
_client: Client | None = None


def _get_client() -> Client:
    """Return the shared client, creating it on first call."""
    global _client
    if _client is None:
        _client = _init_client()
    return _client


# ---------------------------------------------------------------------------
# Write helpers (Supabase only — never touches Striven)
# ---------------------------------------------------------------------------

def insert_estimates(records: list[dict]) -> dict:
    """
    Upsert a batch of estimate records into the `estimates` table.

    Uses upsert (not insert) so re-running a sync never creates duplicates.
    Conflicts are resolved on the `id` column (Striven's primary key).

    Args:
        records: List of dicts that match the `estimates` table schema.
                 Each dict must include an `id` field.

    Returns:
        The raw Supabase response object (contains .data and .count).
    """
    if not records:
        return {}

    response = (
        _get_client()
        .table("estimates")
        .upsert(records, on_conflict="id")
        .execute()
    )

    return response


# ---------------------------------------------------------------------------
# Read helpers — query Supabase, never Striven
# ---------------------------------------------------------------------------

def count_estimates() -> int:
    """Return the total number of estimates stored in Supabase."""
    res = (
        _get_client()
        .table("estimates")
        .select("id", count="exact")
        .execute()
    )
    return res.count or 0


def get_high_value_estimates(min_total: float = 10000, limit: int = 25) -> list[dict]:
    """
    Return estimates where total > min_total, capped at limit rows.
    Only pulls the columns Claude needs — keeps the payload small.
    """
    res = (
        _get_client()
        .table("estimates")
        .select("estimate_number, customer_name, total")
        .gt("total", min_total)
        .order("total", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


def get_estimates_by_customer(name: str) -> list[dict]:
    """
    Case-insensitive search on customer_name using Postgres ilike.
    Wraps the term in % wildcards so partial names match.
    """
    res = (
        _get_client()
        .table("estimates")
        .select("estimate_number, customer_name, status, total, created_date")
        .ilike("customer_name", f"%{name}%")
        .order("created_date", desc=True)
        .execute()
    )
    return res.data or []
