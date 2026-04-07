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


# ---------------------------------------------------------------------------
# Chat log helpers — record every WilliamSmith conversation turn
# ---------------------------------------------------------------------------

def log_chat(user_message: str, tools_called: list[str], response: str) -> None:
    """
    Insert one row into chat_logs for each completed chat turn.

    Args:
        user_message:  The raw question the user typed.
        tools_called:  List of tool names WilliamSmith invoked (may be empty).
        response:      The final answer text (truncated to 500 chars for storage).
    """
    _get_client().table("chat_logs").insert({
        "user_message":    user_message[:1000],
        "tools_called":    ", ".join(tools_called) if tools_called else "none",
        "response_preview": response[:500],
    }).execute()


def get_chat_logs(limit: int = 100) -> list[dict]:
    """Return the most recent chat log rows, newest first."""
    res = (
        _get_client()
        .table("chat_logs")
        .select("id, created_at, user_message, tools_called, response_preview")
        .order("created_at", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


# ---------------------------------------------------------------------------
# Gas log audit helpers — single-row summary table (id always = 1)
# ---------------------------------------------------------------------------

def get_gas_log_audit() -> dict | None:
    """
    Read the latest gas log audit summary from Supabase.

    Returns the row dict if it exists, or None if the table is empty
    (i.e. no audit has ever been persisted yet).
    """
    res = (
        _get_client()
        .table("gas_log_audit")
        .select("id, total_checked, missing_count, percent_missing, updated_at")
        .eq("id", 1)
        .execute()
    )
    data = res.data or []
    return data[0] if data else None


def upsert_full_estimates(records: list[dict]) -> None:
    """
    Upsert a batch of fully-transformed estimate rows into the `estimates` table.

    Conflicts are resolved on `estimate_id` (Striven's primary key).
    Safe to call multiple times — idempotent.

    Args:
        records: List of dicts produced by sync._transform().
                 Each dict must include `estimate_id`.
    """
    if not records:
        return

    _get_client().table("estimates").upsert(
        records, on_conflict="estimate_id"
    ).execute()


def upsert_line_items(records: list[dict]) -> None:
    """
    Upsert a batch of line item rows into the `estimate_line_items` table.

    Conflicts are resolved on `line_item_id` (Striven's line item primary key).
    Safe to call multiple times — idempotent.

    Args:
        records: List of dicts produced by sync._transform().
                 Each dict must include `line_item_id`.
    """
    if not records:
        return

    _get_client().table("estimate_line_items").upsert(
        records, on_conflict="line_item_id"
    ).execute()


def upsert_sales_reps(records: list[dict]) -> None:
    """
    Upsert a batch of sales rep rows into the `sales_reps` table.

    Conflicts are resolved on `rep_id` (Striven's user id).
    Keeps the sales_reps lookup table in sync after every sync batch.

    Args:
        records: List of dicts with keys `rep_id` (int) and `rep_name` (str).
    """
    if not records:
        return

    _get_client().table("sales_reps").upsert(
        records, on_conflict="rep_id"
    ).execute()


def upsert_gas_log_audit(
    total_checked: int,
    missing_count: int,
    percent_missing: float,
) -> None:
    """
    Persist (or overwrite) the gas log audit summary in Supabase.

    Always writes to row id=1 — there is only ever one summary row.
    Calling this multiple times is safe; each call updates the same row.

    Args:
        total_checked:   Total estimates inspected in the scan.
        missing_count:   Number of gas-log installs missing the removal fee.
        percent_missing: missing_count / gas_log_installs * 100 (0 if no installs).
    """
    from datetime import datetime, timezone

    _get_client().table("gas_log_audit").upsert(
        {
            "id":              1,
            "total_checked":   total_checked,
            "missing_count":   missing_count,
            "percent_missing": round(percent_missing, 1),
            "updated_at":      datetime.now(timezone.utc).isoformat(),
        },
        on_conflict="id",
    ).execute()
