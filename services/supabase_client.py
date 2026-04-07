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
        .select("estimate_id", count="exact")
        .execute()
    )
    return res.count or 0


def get_high_value_estimates(min_total: float = 10000, limit: int = 25) -> list[dict]:
    """
    Return estimates where total_amount > min_total, capped at limit rows.
    Only pulls the columns Claude needs — keeps the payload small.
    """
    res = (
        _get_client()
        .table("estimates")
        .select("estimate_number, customer_name, sales_rep_name, status_normalized, total_amount")
        .gt("total_amount", min_total)
        .order("total_amount", desc=True)
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
        .select("estimate_number, customer_name, sales_rep_name, status_normalized, total_amount, created_date")
        .ilike("customer_name", f"%{name}%")
        .order("created_date", desc=True)
        .execute()
    )
    return res.data or []


# ---------------------------------------------------------------------------
# Sales rep normalization — single source of truth for the data layer
# ---------------------------------------------------------------------------

def _normalize_sales_rep(name: str | None) -> str:
    """
    Normalize a raw sales rep name to a clean, consistent string.

    Rules:
      - None / empty / whitespace-only  → "Unassigned"
      - Any other value                 → stripped of leading/trailing whitespace

    Every query helper that returns a sales_rep field MUST call this function.
    This ensures grouping, filtering, and display are always consistent.

    # FUTURE:
    # Store sales_rep_name directly in Supabase during data sync.
    # This will eliminate the need for enrichment calls entirely —
    # sales_rep will be available on every Supabase row with no extra API cost.
    """
    if not name or not str(name).strip():
        return "Unassigned"
    return str(name).strip()


# ---------------------------------------------------------------------------
# Fast direct-query helpers — no LLM, sub-second, Supabase only
# ---------------------------------------------------------------------------

def query_gas_log_missing(limit: int = 50) -> list[dict]:
    """
    Estimates that have gas log line items but are MISSING the removal fee.

    Used by /queries/gas-log-missing to answer "which gas log jobs have
    no removal fee?" without any AI reasoning step.

    sales_rep is always normalized — never null, never empty.
    """
    res = (
        _get_client()
        .table("estimates")
        .select("estimate_number, customer_name, total_amount, sales_rep_name, status_normalized")
        .eq("has_gas_logs", True)
        .eq("has_removal_fee", False)
        .order("estimate_number", desc=True)
        .limit(limit)
        .execute()
    )
    return [
        {
            "estimate_number": r["estimate_number"],
            "customer_name":   r["customer_name"],
            "total_amount":    r["total_amount"],
            "status":          r["status_normalized"],
            "sales_rep":       _normalize_sales_rep(r.get("sales_rep_name")),
        }
        for r in (res.data or [])
    ]


def query_unassigned_reps(limit: int = 50) -> list[dict]:
    """
    Estimates where no sales rep has been assigned.

    Used by /queries/unassigned-reps to surface attribution gaps quickly.
    """
    res = (
        _get_client()
        .table("estimates")
        .select("estimate_number, customer_name, total_amount, status_normalized, created_date")
        .eq("sales_rep_name", "Unassigned")
        .order("created_date", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


def query_no_line_items(limit: int = 50) -> list[dict]:
    """
    Data integrity check: estimates that have zero line items in Supabase.

    Implemented as two fast queries:
      1. Collect distinct estimate_ids present in estimate_line_items.
      2. Return estimates whose estimate_id is NOT in that set.

    This is equivalent to:
      SELECT e.estimate_number, e.customer_name
      FROM estimates e
      LEFT JOIN estimate_line_items li ON e.estimate_id = li.estimate_id
      WHERE li.estimate_id IS NULL
    """
    # Step 1: all estimate_ids that DO have line items (integer ids only)
    li_res = (
        _get_client()
        .table("estimate_line_items")
        .select("estimate_id")
        .execute()
    )
    ids_with_items = list({row["estimate_id"] for row in (li_res.data or [])})

    # Step 2: estimates not in that set
    if ids_with_items:
        res = (
            _get_client()
            .table("estimates")
            .select("estimate_number, customer_name, status_normalized, created_date")
            .not_.in_("estimate_id", ids_with_items)
            .limit(limit)
            .execute()
        )
    else:
        # No line items exist at all — return first N estimates
        res = (
            _get_client()
            .table("estimates")
            .select("estimate_number, customer_name, status_normalized, created_date")
            .limit(limit)
            .execute()
        )
    return res.data or []


# ---------------------------------------------------------------------------
# Analytics query helpers — read-only, AI-consumption endpoints
# ---------------------------------------------------------------------------

def query_jobs_by_location(search: str, year: int | None = None, limit: int = 50) -> dict:
    """
    Search estimates by customer name or job name (no address field exists in schema).

    Striven does not expose a free-text address field via the API.  We search on
    customer_name (case-insensitive ilike) and estimate_number as the closest
    available proxies.  If year is provided, created_date is filtered to that
    calendar year.

    Args:
        search: Partial/full string to match against customer_name.
        year:   Optional calendar year to restrict results (filters created_date).
        limit:  Maximum rows to return (default 50, max 200).

    Returns:
        {"count": N, "note": "...", "jobs": [...]}
    """
    q = (
        _get_client()
        .table("estimates")
        .select(
            "estimate_id, estimate_number, customer_name, "
            "status_normalized, total_amount, created_date, sales_rep_name",
            count="exact",
        )
        .ilike("customer_name", f"%{search}%")
    )

    if year:
        q = (
            q
            .gte("created_date", f"{year}-01-01T00:00:00+00:00")
            .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
        )

    res = q.order("created_date", desc=True).limit(limit).execute()
    return {
        "count": res.count or len(res.data or []),
        "note":  "Searched on customer_name — no address field is available in the data layer.",
        "jobs":  [
            {
                "estimate_id": r["estimate_id"],
                "customer":    r["customer_name"],
                "address":     None,   # not stored — field does not exist in Striven API response
                "status":      r["status_normalized"],
                "amount":      r["total_amount"],
                "sales_rep":   _normalize_sales_rep(r.get("sales_rep_name")),
                "created":     r["created_date"],
            }
            for r in (res.data or [])
        ],
    }


def query_jobs_past_install_date(limit: int = 100) -> dict:
    """
    Active estimates whose target_date has already passed today's date.

    "Active" = status_normalized = 'ACTIVE', which covers Striven statuses
    Quoted (19), Pending Approval (20), Approved (22), and In Progress (25).
    Only estimates with a non-null target_date are included.

    days_overdue is calculated in Python (today − target_date in days).

    Returns:
        {"count": N, "jobs": [...]}
    """
    from datetime import date, datetime, timezone

    today_str = date.today().isoformat()

    res = (
        _get_client()
        .table("estimates")
        .select(
            "estimate_id, estimate_number, customer_name, "
            "target_date, sales_rep_name, status_normalized, total_amount",
        )
        .eq("status_normalized", "ACTIVE")
        .not_.is_("target_date", "null")
        .lt("target_date", today_str)
        .order("target_date")           # oldest overdue first
        .limit(limit)
        .execute()
    )

    today = date.today()
    jobs  = []
    for r in (res.data or []):
        raw = r["target_date"]
        try:
            # target_date comes back as ISO string; strip timezone for date math
            td = datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
            overdue = (today - td).days
        except Exception:
            overdue = None
        jobs.append({
            "estimate_id":         r["estimate_id"],
            "customer":            r["customer_name"],
            "target_install_date": r["target_date"],
            "days_overdue":        overdue,
            "sales_rep":           _normalize_sales_rep(r.get("sales_rep_name")),
            "amount":              r["total_amount"],
        })

    return {"count": len(jobs), "jobs": jobs}


def query_sales_rep_backlog() -> dict:
    """
    Aggregate active estimate counts per sales rep with three dimensions:
      - total_jobs:       all ACTIVE estimates for this rep
      - unscheduled_jobs: ACTIVE estimates with no target_date set
      - overdue_jobs:     ACTIVE estimates where target_date < today

    Aggregation happens in Python (supabase-py does not support GROUP BY).
    Only ACTIVE estimates are included; completed/cancelled jobs are excluded.

    Returns:
        {"reps": [...], "total_active": N}  sorted descending by total_jobs
    """
    from datetime import date
    from collections import defaultdict

    today_str = date.today().isoformat()

    # Fetch all active estimates — only the three columns we need
    res = (
        _get_client()
        .table("estimates")
        .select("sales_rep_name, target_date, status_normalized")
        .eq("status_normalized", "ACTIVE")
        .execute()
    )
    records = res.data or []

    backlog: dict[str, dict] = defaultdict(
        lambda: {"total_jobs": 0, "unscheduled_jobs": 0, "overdue_jobs": 0}
    )

    for r in records:
        rep = _normalize_sales_rep(r.get("sales_rep_name"))
        td  = r.get("target_date")

        backlog[rep]["total_jobs"] += 1

        if not td:
            # No install date set at all
            backlog[rep]["unscheduled_jobs"] += 1
        elif td[:10] < today_str:
            # Has a date but it has already passed
            backlog[rep]["overdue_jobs"] += 1

    reps = [
        {"rep": rep, **counts}
        for rep, counts in sorted(
            backlog.items(), key=lambda x: -x[1]["total_jobs"]
        )
    ]
    return {"total_active": len(records), "reps": reps}


def query_time_to_target() -> dict:
    """
    Calculates days from estimate creation to scheduled install (target_date).

    IMPORTANT — data limitation:
      The Striven API does not return an approved_date or preview task date.
      This function uses created_date → target_date as the nearest available
      proxy for "time from estimate to scheduled install".

    Only estimates with BOTH created_date and target_date are included.
    Estimates where target_date < created_date are excluded as data anomalies.

    Returns:
        {
          "average_days": float,
          "median_days":  float,
          "sample_size":  int,
          "data_note":    str,
          "samples":      [{"estimate_id": ..., "days_to_target": ...}, ...]
        }
    """
    from datetime import datetime
    import statistics

    res = (
        _get_client()
        .table("estimates")
        .select("estimate_id, estimate_number, customer_name, created_date, target_date")
        .not_.is_("target_date",  "null")
        .not_.is_("created_date", "null")
        .limit(500)           # large sample for statistical reliability
        .execute()
    )

    deltas = []
    samples = []
    for r in (res.data or []):
        try:
            created = datetime.fromisoformat(r["created_date"].replace("Z", "+00:00"))
            target  = datetime.fromisoformat(r["target_date"].replace("Z",  "+00:00"))
            days    = (target - created).days
            if days < 0:
                continue   # skip anomalies where target precedes creation
            deltas.append(days)
            samples.append({
                "estimate_id":    r["estimate_id"],
                "estimate_number": r["estimate_number"],
                "customer":       r["customer_name"],
                "days_to_target": days,
            })
        except Exception:
            continue

    if not deltas:
        return {
            "average_days": None,
            "median_days":  None,
            "sample_size":  0,
            "data_note":    "No records with both created_date and target_date found.",
            "samples":      [],
        }

    # Sort samples by days ascending so fastest jobs appear first
    samples.sort(key=lambda x: x["days_to_target"])

    return {
        "average_days": round(statistics.mean(deltas),   1),
        "median_days":  round(statistics.median(deltas), 1),
        "sample_size":  len(deltas),
        "data_note": (
            "Days measured from estimate created_date to target_date "
            "(Striven API does not expose approved_date or preview task dates)."
        ),
        "samples": samples[:25],   # top-25 fastest for brevity
    }


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
