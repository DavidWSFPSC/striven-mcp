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


def _reset_client() -> Client:
    """Force-create a fresh Supabase client (e.g. after a connection error)."""
    global _client
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
# Knowledge-base search logging
# ---------------------------------------------------------------------------

def log_kb_search(
    query:          str,
    results_count:  int,
    top_similarity: float | None,
) -> None:
    """
    Insert one row into kb_search_log. Called fire-and-forget after every
    search_knowledge_base invocation — failures are silently swallowed so
    they never break the search itself.

    Table DDL (run once in Supabase SQL editor):
        CREATE TABLE IF NOT EXISTS kb_search_log (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            query           TEXT NOT NULL,
            results_count   INT NOT NULL,
            top_similarity  FLOAT,
            was_helpful     BOOLEAN DEFAULT NULL,
            searched_at     TIMESTAMPTZ DEFAULT now()
        );
    """
    try:
        _get_client().table("kb_search_log").insert({
            "query":          query[:500],
            "results_count":  results_count,
            "top_similarity": top_similarity,
        }).execute()
    except Exception:
        pass   # fire-and-forget — never raise


def query_kb_gaps(days: int = 30) -> dict:
    """
    Return the most frequently searched queries that produced poor results
    (no results OR top_similarity < 0.5) in the last `days` days.

    Groups by exact query text and counts occurrences, sorted descending.
    """
    from datetime import datetime, timezone, timedelta

    since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%dT%H:%M:%S+00:00"
    )

    res = (
        _get_client()
        .table("kb_search_log")
        .select("query, results_count, top_similarity, searched_at")
        .gte("searched_at", since)
        .or_("results_count.eq.0,top_similarity.lt.0.5")
        .order("searched_at", desc=True)
        .limit(2000)
        .execute()
    )
    rows = res.data or []

    # Group by exact query text
    from collections import defaultdict
    counts: dict[str, dict] = defaultdict(lambda: {"count": 0, "top_sim_samples": [], "last_searched": ""})
    for r in rows:
        q   = r["query"]
        sim = r.get("top_similarity")
        counts[q]["count"] += 1
        if sim is not None:
            counts[q]["top_sim_samples"].append(sim)
        if r["searched_at"] > counts[q]["last_searched"]:
            counts[q]["last_searched"] = r["searched_at"]

    gaps = sorted(
        [
            {
                "query":            q,
                "search_count":     v["count"],
                "avg_similarity":   round(
                    sum(v["top_sim_samples"]) / len(v["top_sim_samples"]), 3
                ) if v["top_sim_samples"] else None,
                "last_searched":    v["last_searched"],
            }
            for q, v in counts.items()
        ],
        key=lambda x: -x["search_count"],
    )

    return {
        "days":        days,
        "since":       since,
        "total_poor_searches": len(rows),
        "unique_gap_queries":  len(gaps),
        "gaps":        gaps,
    }


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


# ---------------------------------------------------------------------------
# customer_locations — address-based location warehouse
#
# Table schema (create once in Supabase dashboard or via SQL):
#
#   CREATE TABLE customer_locations (
#       location_id   bigint PRIMARY KEY,
#       customer_id   bigint NOT NULL,
#       customer_name text,
#       address1      text,
#       city          text,
#       city_norm     text,          -- lowercased+stripped for fast matching
#       state         text,
#       postal_code   text,
#       is_primary    boolean DEFAULT false,
#       synced_at     timestamptz DEFAULT now()
#   );
#   CREATE INDEX idx_locations_city_norm  ON customer_locations(city_norm);
#   CREATE INDEX idx_locations_postal     ON customer_locations(postal_code);
#   CREATE INDEX idx_locations_customer   ON customer_locations(customer_id);
#
# ---------------------------------------------------------------------------

def upsert_customer_locations(records: list[dict]) -> int:
    """
    Upsert a batch of customer location records.
    Conflicts resolved on location_id (Striven's primary key).
    Returns the number of rows upserted.
    """
    if not records:
        return 0
    res = (
        _get_client()
        .table("customer_locations")
        .upsert(records, on_conflict="location_id")
        .execute()
    )
    return len(res.data) if res.data else 0


# ---------------------------------------------------------------------------
# callback_tasks — query helpers for callback / return-trip intelligence
#
# Table schema (see callback_audit.py for DDL):
#   task_id, task_type_id, task_type, task_status, assigned_to,
#   customer_id, customer_name, estimate_id, estimate_number,
#   created_date, due_date, synced_at
# ---------------------------------------------------------------------------

def query_callback_insights(
    by:       str        = "summary",
    assignee: str | None = None,
    year:     int | None = None,
    status:   str | None = None,
    limit:    int        = 500,
) -> dict:
    """
    Query the callback_tasks Supabase table and return aggregated insights.

    Args:
        by:       "summary" | "assignee" | "type" | "year" | "customer"
                  Controls which breakdown is returned.
        assignee: Filter to a specific technician / assignee (partial match).
        year:     Filter to a specific calendar year (based on created_date).
        status:   Filter to a specific task status (e.g. "Open", "Done").
        limit:    Max raw rows to fetch before aggregation (default 500).

    Returns:
        Dict with summary stats, optional breakdown, and a sample of recent tasks.
    """
    from collections import defaultdict

    # ── Build query ────────────────────────────────────────────────────────
    q = (
        _get_client()
        .table("callback_tasks")
        .select(
            "task_id, task_type, task_status, assigned_to, "
            "customer_name, estimate_id, estimate_number, created_date"
        )
        .order("created_date", desc=True)
        .limit(limit)
    )

    if assignee:
        q = q.ilike("assigned_to", f"%{assignee}%")
    if status:
        q = q.ilike("task_status", f"%{status}%")
    if year:
        q = (
            q
            .gte("created_date", f"{year}-01-01T00:00:00+00:00")
            .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
        )

    res  = q.execute()
    rows = res.data or []

    if not rows:
        return {
            "total":       0,
            "filters":     {"by": by, "assignee": assignee, "year": year, "status": status},
            "breakdown":   {},
            "open_count":  0,
            "sample":      [],
            "note":        "No callback tasks found matching the given filters.",
        }

    # ── Aggregate ──────────────────────────────────────────────────────────
    total      = len(rows)
    open_count = sum(1 for r in rows if (r.get("task_status") or "").lower() == "open")
    linked     = sum(1 for r in rows if r.get("estimate_id"))

    by_type:     dict[str, int] = defaultdict(int)
    by_status:   dict[str, int] = defaultdict(int)
    by_year:     dict[str, int] = defaultdict(int)
    by_assignee: dict[str, int] = defaultdict(int)
    by_customer: dict[str, int] = defaultdict(int)

    for r in rows:
        by_type[r.get("task_type") or "Unknown"]    += 1
        by_status[r.get("task_status") or "Unknown"] += 1
        yr = (r.get("created_date") or "")[:4] or "Unknown"
        by_year[yr] += 1
        by_assignee[r.get("assigned_to") or "Unassigned"] += 1
        by_customer[r.get("customer_name") or "Unknown"]  += 1

    # Choose breakdown based on `by` param
    breakdown_map = {
        "assignee": dict(sorted(by_assignee.items(), key=lambda x: -x[1])),
        "type":     dict(sorted(by_type.items(),     key=lambda x: -x[1])),
        "year":     dict(sorted(by_year.items())),
        "customer": dict(sorted(by_customer.items(), key=lambda x: -x[1])[:25]),
        "summary":  {
            "by_type":     dict(sorted(by_type.items(),   key=lambda x: -x[1])),
            "by_status":   dict(sorted(by_status.items(), key=lambda x: -x[1])),
            "by_year":     dict(sorted(by_year.items())),
            "top_assignees": dict(sorted(by_assignee.items(), key=lambda x: -x[1])[:10]),
        },
    }
    breakdown = breakdown_map.get(by, breakdown_map["summary"])

    # Recent sample (up to 20)
    sample = [
        {
            "task_type":       r.get("task_type"),
            "task_status":     r.get("task_status"),
            "assigned_to":     r.get("assigned_to"),
            "customer":        r.get("customer_name"),
            "estimate_number": r.get("estimate_number"),
            "created_date":    r.get("created_date"),
        }
        for r in rows[:20]
    ]

    return {
        "total":       total,
        "open_count":  open_count,
        "linked_to_estimate": linked,
        "filters":     {
            "by":       by,
            "assignee": assignee,
            "year":     year,
            "status":   status,
        },
        "breakdown":   breakdown,
        "sample":      sample,
    }


# ---------------------------------------------------------------------------
# Brand catalog — all brands carried by WilliamSmith Fireplaces
#
# Each entry: (display_name, [search_keywords])
# Keywords are what actually appear in Striven line item names/descriptions.
# Multiple keywords handle spelling variants, abbreviations, model prefixes.
# ---------------------------------------------------------------------------

_BRAND_CATALOG: list[tuple[str, list[str]]] = [
    # ── Masonry / custom systems ────────────────────────────────────────────
    ("Isokern",             ["isokern"]),
    ("FireRock",            ["firerock"]),
    ("Stellar",             ["stellar"]),
    ("Acucraft",            ["acucraft", "accucraft"]),

    # ── Gas fireplaces & inserts ────────────────────────────────────────────
    ("Heat & Glo",          ["heat & glo", "heat n glo", "heatnglo", "heat and glo"]),
    ("Heatilator",          ["heatilator"]),
    ("Majestic",            ["majestic"]),
    ("Napoleon",            ["napoleon"]),
    ("Montigo",             ["montigo"]),
    ("Kozy Heat",           ["kozy heat", "kozyheat"]),
    ("Monessen",            ["monessen"]),
    ("Superior",            ["superior"]),
    ("Astria",              ["astria"]),
    ("American Fyre Designs", ["american fyre", "american fyre designs"]),

    # ── Electric fireplaces ──────────────────────────────────────────────────
    ("Dimplex",             ["dimplex"]),
    ("SimpliFire",          ["simplifire"]),
    ("Ortal",               ["ortal"]),

    # ── European / custom linear ─────────────────────────────────────────────
    ("Element 4",           ["element 4", "element4"]),
    ("Focus",               ["focus fireplaces", "focus fires"]),  # 'focus' alone too generic
    ("JC Bordelet",         ["bordelet", "jc bordelet"]),
    ("European Home",       ["european home"]),

    # ── Gas logs ────────────────────────────────────────────────────────────
    ("Rasmussen",           ["rasmussen"]),
    ("RH Peterson",         ["rh peterson", "r.h. peterson", "peterson real fyre"]),
    ("Grand Canyon",        ["grand canyon"]),

    # ── Accessories / other ──────────────────────────────────────────────────
    ("Stoll",               ["stoll"]),
]


# ---------------------------------------------------------------------------
# Line-item keyword search — find estimates by product name / description
# ---------------------------------------------------------------------------

def query_estimates_by_keyword(
    keyword: str,
    zip_code:   str | None = None,
    status:     str | None = None,
    year:       int | None = None,
    limit:      int        = 50,
) -> dict:
    """
    Search estimates by keyword match in line item names or descriptions.

    Strategy (two-step — Supabase-py has no native JOIN):
      Step 1: Find estimate_ids whose line items contain the keyword
              (ilike match on item_name OR description).
      Step 2: Fetch those estimates from the estimates table, applying
              optional zip/status/year filters via customer_locations lookup.

    Args:
        keyword:  Product or service term to match (case-insensitive, partial).
                  e.g. "isokern", "gas log", "linear fireplace", "napoleon"
        zip_code: Postal code to filter by (e.g. "29455" for Johns Island area).
                  Matched against customer_locations.postal_code.
        status:   Status to filter by (partial, case-insensitive).
                  e.g. "Completed", "In Progress", "Quoted"
        year:     Calendar year to restrict by created_date.
        limit:    Max estimates to return in the sample (default 50).

    Returns dict with:
        count         — total matching estimates
        total_revenue — sum of matching estimate totals
        keyword       — echo of search term
        filters       — all applied filters
        by_status     — count + revenue breakdown by status
        data          — sample estimates (up to limit), each with matched_items list
    """
    from collections import defaultdict

    kw = keyword.strip()
    if not kw:
        return {"error": "keyword is required", "count": 0, "data": []}

    # ── Step 1: find estimate_ids with matching line items ────────────────
    # Search item_name (product SKU / name) and description (free-text detail)
    li_res_name = (
        _get_client()
        .table("estimate_line_items")
        .select("estimate_id, item_name, description")
        .ilike("item_name", f"%{kw}%")
        .execute()
    )
    li_res_desc = (
        _get_client()
        .table("estimate_line_items")
        .select("estimate_id, item_name, description")
        .ilike("description", f"%{kw}%")
        .execute()
    )

    # Merge and de-dup, keeping one matched label per estimate_id
    matched: dict[int, list[str]] = {}
    for row in (li_res_name.data or []) + (li_res_desc.data or []):
        eid   = row.get("estimate_id")
        label = (row.get("item_name") or row.get("description") or "").strip()
        if eid:
            if eid not in matched:
                matched[eid] = []
            if label and label not in matched[eid]:
                matched[eid].append(label)

    if not matched:
        return {
            "count":         0,
            "total_revenue": 0,
            "keyword":       kw,
            "filters":       {"zip": zip_code, "status": status, "year": year},
            "by_status":     {},
            "data":          [],
            "note":          f"No line items found matching '{kw}'.",
        }

    estimate_ids = list(matched.keys())

    # ── Step 2: optional zip filter — resolve zip → customer_ids ─────────
    zip_customer_ids: set[int] | None = None
    if zip_code:
        loc_res = (
            _get_client()
            .table("customer_locations")
            .select("customer_id")
            .eq("postal_code", zip_code.strip())
            .execute()
        )
        zip_customer_ids = {
            r["customer_id"] for r in (loc_res.data or []) if r.get("customer_id")
        }
        if not zip_customer_ids:
            return {
                "count":         0,
                "total_revenue": 0,
                "keyword":       kw,
                "filters":       {"zip": zip_code, "status": status, "year": year},
                "by_status":     {},
                "data":          [],
                "note":          f"No customers found in zip code '{zip_code}'.",
            }

    # ── Step 3: fetch matching estimates (chunked) ────────────────────────
    all_estimates: list[dict] = []
    chunk_size = 100
    for i in range(0, len(estimate_ids), chunk_size):
        chunk = estimate_ids[i: i + chunk_size]
        q = (
            _get_client()
            .table("estimates")
            .select(
                "estimate_id, estimate_number, customer_id, customer_name, "
                "sales_rep_name, status_raw, status_normalized, "
                "total_amount, created_date"
            )
            .in_("estimate_id", chunk)
            .order("created_date", desc=True)
        )
        if zip_customer_ids is not None:
            q = q.in_("customer_id", list(zip_customer_ids))
        if status:
            q = q.ilike("status_raw", f"%{status}%")
        if year:
            q = (
                q
                .gte("created_date", f"{year}-01-01T00:00:00+00:00")
                .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
            )
        res = q.execute()
        all_estimates.extend(res.data or [])

    if not all_estimates:
        return {
            "count":         0,
            "total_revenue": 0,
            "keyword":       kw,
            "filters":       {"zip": zip_code, "status": status, "year": year},
            "by_status":     {},
            "data":          [],
            "note":          "Line items matched but no estimates passed the applied filters.",
        }

    # ── Step 4: aggregate ─────────────────────────────────────────────────
    total_revenue = sum(e.get("total_amount") or 0 for e in all_estimates)
    by_status: dict[str, dict] = defaultdict(lambda: {"count": 0, "revenue": 0.0})
    for e in all_estimates:
        s = e.get("status_raw") or "Unknown"
        by_status[s]["count"]   += 1
        by_status[s]["revenue"] += e.get("total_amount") or 0

    for s in by_status:
        by_status[s]["revenue"] = round(by_status[s]["revenue"], 2)

    data = [
        {
            "estimate_number": e.get("estimate_number"),
            "customer_name":   e.get("customer_name"),
            "status":          e.get("status_raw"),
            "sales_rep":       _normalize_sales_rep(e.get("sales_rep_name")),
            "total":           e.get("total_amount"),
            "created":         e.get("created_date"),
            "matched_items":   matched.get(e.get("estimate_id"), [])[:5],
        }
        for e in all_estimates[:limit]
    ]

    return {
        "count":         len(all_estimates),
        "total_revenue": round(total_revenue, 2),
        "keyword":       kw,
        "filters":       {
            "zip":    zip_code,
            "status": status,
            "year":   year,
            "limit":  limit,
        },
        "by_status": dict(sorted(by_status.items(), key=lambda x: -x[1]["count"])),
        "data":      data,
    }


# ---------------------------------------------------------------------------
# Brand leaderboard — all brands ranked by job count and revenue
# ---------------------------------------------------------------------------

def query_brand_summary(
    year:     int | None = None,
    zip_code: str | None = None,
    min_jobs: int        = 1,
) -> dict:
    """
    Return a ranked leaderboard of all WilliamSmith brands by job count and revenue.

    For each brand in _BRAND_CATALOG, searches estimate_line_items for matching
    keywords (item_name + description), then joins to estimates for revenue.
    Optional year and zip_code filters are applied to the estimate side.

    Strategy:
      1. Pre-build a valid estimate_id set (filtered by year/zip if given).
      2. For each brand, query line items by keyword → intersect with valid IDs.
      3. Aggregate job count + revenue per brand.
      Runs brand lookups in parallel (ThreadPoolExecutor) for speed.

    Args:
        year:     Restrict to a specific calendar year.
        zip_code: Restrict to a specific zip code (via customer_locations).
        min_jobs: Only include brands with at least this many jobs (default 1).

    Returns dict with:
        brands      — list of {brand, job_count, total_revenue, top_status}
                      sorted by job_count descending
        total_jobs  — sum of all matched jobs across all brands
        filters     — applied filters
        note        — caveat if a zip/year filter was active
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    client = _get_client()

    # ── Step 1: build valid estimate_id → revenue lookup (filtered) ────────
    zip_customer_ids: set[int] | None = None
    if zip_code:
        loc_res = (
            client.table("customer_locations")
            .select("customer_id")
            .eq("postal_code", zip_code.strip())
            .execute()
        )
        zip_customer_ids = {
            r["customer_id"] for r in (loc_res.data or []) if r.get("customer_id")
        }

    # Fetch all estimates (chunked for year/zip filters)
    est_lookup: dict[int, dict] = {}
    chunk_size = 1000
    page = 0
    while True:
        q = (
            client.table("estimates")
            .select("estimate_id, total_amount, status_raw, customer_id")
            .range(page * chunk_size, (page + 1) * chunk_size - 1)
        )
        if year:
            q = (
                q
                .gte("created_date", f"{year}-01-01T00:00:00+00:00")
                .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
            )
        if zip_customer_ids is not None:
            # Filter in chunks of 100
            pass   # handled below
        res = q.execute()
        rows = res.data or []
        if not rows:
            break
        for r in rows:
            eid = r.get("estimate_id")
            if eid and (
                zip_customer_ids is None
                or r.get("customer_id") in zip_customer_ids
            ):
                est_lookup[eid] = r
        if len(rows) < chunk_size:
            break
        page += 1

    valid_ids = set(est_lookup.keys())

    if not valid_ids:
        return {
            "brands":     [],
            "total_jobs": 0,
            "filters":    {"year": year, "zip": zip_code},
            "note":       "No estimates found matching the given filters.",
        }

    # ── Step 2: for each brand, count matching estimate_ids in parallel ────
    def _brand_count(brand_name: str, keywords: list[str]) -> dict:
        matched: set[int] = set()
        for kw in keywords:
            for field in ("item_name", "description"):
                try:
                    li_res = (
                        client.table("estimate_line_items")
                        .select("estimate_id")
                        .ilike(field, f"%{kw}%")
                        .execute()
                    )
                    for r in (li_res.data or []):
                        eid = r.get("estimate_id")
                        if eid and eid in valid_ids:
                            matched.add(eid)
                except Exception:
                    pass
        if len(matched) < min_jobs:
            return {}
        revenue = sum(est_lookup[eid].get("total_amount") or 0 for eid in matched)
        # Find most common status
        status_counts: dict[str, int] = {}
        for eid in matched:
            s = est_lookup[eid].get("status_raw") or "Unknown"
            status_counts[s] = status_counts.get(s, 0) + 1
        top_status = max(status_counts, key=lambda x: status_counts[x]) if status_counts else "Unknown"
        return {
            "brand":         brand_name,
            "job_count":     len(matched),
            "total_revenue": round(revenue, 2),
            "top_status":    top_status,
        }

    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {
            pool.submit(_brand_count, name, keywords): name
            for name, keywords in _BRAND_CATALOG
        }
        for future in as_completed(futures):
            r = future.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: (-x["job_count"], -x["total_revenue"]))

    return {
        "brands":     results,
        "total_jobs": sum(r["job_count"] for r in results),
        "filters":    {"year": year, "zip": zip_code, "min_jobs": min_jobs},
        "note":       (
            f"Each job counted once per brand. "
            f"A single estimate may appear under multiple brands if it has "
            f"line items from more than one manufacturer."
        ),
    }


# ---------------------------------------------------------------------------
# Charleston tri-county area → zip code map
#
# Used by query_jobs_by_area() to resolve named areas to postal codes before
# querying customer_locations.  Zip-based matching is authoritative — a
# customer whose city field says "Charleston" but zip 29407 is West Ashley,
# not Downtown.  City-name ilike is used only as a fallback for non-local areas.
#
# Sources verified April 2026.
# ---------------------------------------------------------------------------

_TRI_COUNTY_AREA_ZIPS: dict[str, list[str]] = {
    # West Ashley  — inside 526 (29407) + outside 526 (29414)
    "west ashley":                  ["29407", "29414"],
    # James Island
    "james island":                 ["29412"],
    # Johns Island / Kiawah / Seabrook — all share 29455
    "johns island":                 ["29455"],
    "kiawah":                       ["29455"],
    "kiawah island":                ["29455"],
    "seabrook":                     ["29455"],
    "seabrook island":              ["29455"],
    # Downtown Charleston
    "downtown charleston":          ["29401", "29403"],
    "charleston downtown":          ["29401", "29403"],
    "downtown":                     ["29401", "29403"],
    # North Charleston
    "north charleston":             ["29405", "29406", "29418", "29420"],
    # Mount Pleasant — split N/S by zip
    "mount pleasant":               ["29464", "29466"],
    "mt pleasant":                  ["29464", "29466"],
    "mount pleasant south":         ["29464"],
    "mount pleasant north":         ["29466"],
    "mt pleasant south":            ["29464"],
    "mt pleasant north":            ["29466"],
    # Daniel Island
    "daniel island":                ["29492"],
    # Summerville
    "summerville":                  ["29483", "29485"],
    # Goose Creek
    "goose creek":                  ["29445"],
    # Hanahan
    "hanahan":                      ["29410"],
    # Beach communities
    "folly beach":                  ["29439"],
    "folly":                        ["29439"],
    "sullivans island":             ["29482"],
    "sullivan's island":            ["29482"],
    "isle of palms":                ["29451"],
    "iop":                          ["29451"],
}

# Canonical display name for each area key
_TRI_COUNTY_AREA_LABELS: dict[str, str] = {
    "west ashley":                  "West Ashley",
    "james island":                 "James Island",
    "johns island":                 "Johns Island / Kiawah / Seabrook",
    "kiawah":                       "Johns Island / Kiawah / Seabrook",
    "kiawah island":                "Johns Island / Kiawah / Seabrook",
    "seabrook":                     "Johns Island / Kiawah / Seabrook",
    "seabrook island":              "Johns Island / Kiawah / Seabrook",
    "downtown charleston":          "Downtown Charleston",
    "charleston downtown":          "Downtown Charleston",
    "downtown":                     "Downtown Charleston",
    "north charleston":             "North Charleston",
    "mount pleasant":               "Mount Pleasant (All)",
    "mt pleasant":                  "Mount Pleasant (All)",
    "mount pleasant south":         "Mount Pleasant South (29464)",
    "mount pleasant north":         "Mount Pleasant North (29466)",
    "mt pleasant south":            "Mount Pleasant South (29464)",
    "mt pleasant north":            "Mount Pleasant North (29466)",
    "daniel island":                "Daniel Island",
    "summerville":                  "Summerville",
    "goose creek":                  "Goose Creek",
    "hanahan":                      "Hanahan",
    "folly beach":                  "Folly Beach",
    "folly":                        "Folly Beach",
    "sullivans island":             "Sullivan's Island",
    "sullivan's island":            "Sullivan's Island",
    "isle of palms":                "Isle of Palms",
    "iop":                          "Isle of Palms",
}


def list_service_areas() -> list[dict]:
    """
    Return the canonical list of named service areas with their zip codes.
    Used by the /queries/areas endpoint and MCP tool.
    """
    seen: set[str] = set()
    areas = []
    for key, label in _TRI_COUNTY_AREA_LABELS.items():
        if label not in seen:
            seen.add(label)
            areas.append({
                "area":  label,
                "zips":  _TRI_COUNTY_AREA_ZIPS[key],
            })
    return areas


def _fetch_estimates_for_customers(customer_ids: list[int], year: int | None = None) -> list[dict]:
    """
    Pull all estimates for a list of customer_ids from Supabase.
    Optionally filters to a specific calendar year.
    Handles chunking automatically (Supabase IN limit ~200).
    """
    all_estimates: list[dict] = []
    chunk_size = 100
    for i in range(0, len(customer_ids), chunk_size):
        chunk = customer_ids[i: i + chunk_size]
        q = (
            _get_client()
            .table("estimates")
            .select(
                "estimate_id, estimate_number, customer_id, customer_name, "
                "sales_rep_name, status_raw, status_normalized, total_amount, "
                "created_date, target_date"
            )
            .in_("customer_id", chunk)
            .order("created_date", desc=True)
        )
        if year:
            q = (
                q
                .gte("created_date", f"{year}-01-01T00:00:00+00:00")
                .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
            )
        res = q.execute()
        all_estimates.extend(res.data or [])
    return all_estimates


def _aggregate_estimates(all_estimates: list[dict], limit: int = 50) -> dict:
    """
    Aggregate a list of estimate rows into summary stats.
    Returns: count, total_revenue, by_status, sample.
    """
    total_revenue = sum(e.get("total_amount") or 0 for e in all_estimates)
    by_status: dict[str, dict] = {}
    for e in all_estimates:
        s = e.get("status_raw") or "Unknown"
        if s not in by_status:
            by_status[s] = {"count": 0, "revenue": 0.0}
        by_status[s]["count"]   += 1
        by_status[s]["revenue"] += e.get("total_amount") or 0

    for s in by_status:
        by_status[s]["revenue"] = round(by_status[s]["revenue"], 2)

    sample = [
        {
            "estimate_number": e.get("estimate_number"),
            "customer":        e.get("customer_name"),
            "sales_rep":       _normalize_sales_rep(e.get("sales_rep_name")),
            "status":          e.get("status_raw"),
            "total":           e.get("total_amount"),
            "created":         e.get("created_date"),
        }
        for e in all_estimates[:limit]
    ]
    return {
        "count":         len(all_estimates),
        "total_revenue": round(total_revenue, 2),
        "by_status":     by_status,
        "sample":        sample,
    }


def query_jobs_by_area(city: str, limit: int = 500, year: int | None = None) -> dict:
    """
    Find all estimates whose job site is in the named area or city.

    Resolution order:
      1. Named area  — checks _TRI_COUNTY_AREA_ZIPS (e.g. "West Ashley" → 29407, 29414)
                       Queries customer_locations by postal_code — most accurate.
      2. City ilike  — falls back to city_norm substring match for non-local areas.

    Args:
        city:  Named area (e.g. "West Ashley", "Mount Pleasant") or city name.
        limit: Max estimates to return in the sample list.
        year:  Optional calendar year to restrict estimates (e.g. 2024).

    Returns dict with: count, total_revenue, customers_found, method,
                       area_label, zips_used, by_status, and sample estimates.
    """
    key = city.strip().lower()

    # ── Path 1: Named tri-county area → zip-based lookup ─────────────────
    if key in _TRI_COUNTY_AREA_ZIPS:
        zips        = _TRI_COUNTY_AREA_ZIPS[key]
        area_label  = _TRI_COUNTY_AREA_LABELS.get(key, city.title())

        loc_res = (
            _get_client()
            .table("customer_locations")
            .select("customer_id, customer_name, city, postal_code, address1")
            .in_("postal_code", zips)
            .execute()
        )
        locations = loc_res.data or []
        if not locations:
            return {
                "count":           0,
                "total_revenue":   0,
                "customers_found": 0,
                "area_label":      area_label,
                "zips_used":       zips,
                "method":          "zip",
                "by_status":       {},
                "sample":          [],
                "note":            f"No customer locations found in zip codes {zips}.",
            }

        customer_ids = list({r["customer_id"] for r in locations if r.get("customer_id")})
        all_estimates = _fetch_estimates_for_customers(customer_ids, year=year)

        if not all_estimates:
            return {
                "count":           0,
                "total_revenue":   0,
                "customers_found": len(customer_ids),
                "area_label":      area_label,
                "zips_used":       zips,
                "method":          "zip",
                "by_status":       {},
                "sample":          [],
                "note":            "Customer locations found but no estimates on record.",
            }

        agg = _aggregate_estimates(all_estimates, limit=limit)
        return {
            **agg,
            "customers_found": len(customer_ids),
            "area_label":      area_label,
            "zips_used":       zips,
            "method":          "zip",
            "year_filter":     year,
        }

    # ── Path 2: Fallback — city name ilike match ──────────────────────────
    city_norm = key
    loc_res = (
        _get_client()
        .table("customer_locations")
        .select("customer_id, customer_name, city, postal_code, address1")
        .ilike("city_norm", f"%{city_norm}%")
        .execute()
    )
    locations = loc_res.data or []
    if not locations:
        return {
            "count":           0,
            "total_revenue":   0,
            "customers_found": 0,
            "area_label":      city.title(),
            "zips_used":       [],
            "method":          "city_name",
            "by_status":       {},
            "sample":          [],
            "note":            (
                f"No customer locations found matching '{city}'. "
                f"Try a named area: West Ashley, Mount Pleasant, James Island, "
                f"Johns Island, North Charleston, Summerville, Daniel Island, etc."
            ),
        }

    customer_ids  = list({r["customer_id"] for r in locations if r.get("customer_id")})
    all_estimates = _fetch_estimates_for_customers(customer_ids, year=year)

    if not all_estimates:
        return {
            "count":           0,
            "total_revenue":   0,
            "customers_found": len(customer_ids),
            "area_label":      city.title(),
            "zips_used":       [],
            "method":          "city_name",
            "by_status":       {},
            "sample":          [],
            "note":            "Customers found but no estimates on record.",
        }

    agg = _aggregate_estimates(all_estimates, limit=limit)
    return {
        **agg,
        "customers_found": len(customer_ids),
        "area_label":      city.title(),
        "zips_used":       [],
        "method":          "city_name",
        "year_filter":     year,
    }


# ---------------------------------------------------------------------------
# Callbacks by product — which fireplace models generate the most return trips
# ---------------------------------------------------------------------------

def query_callbacks_by_product(
    year:          int | None = None,
    callback_type: str | None = None,
    min_price:     float = 500.0,
    limit:         int = 2000,
) -> dict:
    """
    Join callback_tasks to estimate_line_items to identify which fireplace
    makes and models generate the most callbacks / return trips.

    Args:
        year:          Filter callbacks to a specific calendar year.
        callback_type: Filter by task_type substring (e.g. "Installer", "Service").
        min_price:     Minimum line item price to qualify as a main product unit
                       (filters out accessories, parts, and labor). Default $500.
        limit:         Max callback rows to fetch before aggregation (default 2000).

    Returns:
        Dict with ranked by_product list, total/linked/unlinked counts, and filters.
    """
    from collections import defaultdict

    # ── Step 1: fetch callback tasks ────────────────────────────────────────
    q = (
        _get_client()
        .table("callback_tasks")
        .select(
            "task_id, task_type, task_status, assigned_to, "
            "customer_name, estimate_id, estimate_number, created_date"
        )
        .order("created_date", desc=True)
        .limit(limit)
    )
    if year:
        q = (
            q
            .gte("created_date", f"{year}-01-01T00:00:00+00:00")
            .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
        )
    if callback_type:
        q = q.ilike("task_type", f"%{callback_type}%")

    cb_res    = q.execute()
    callbacks = cb_res.data or []

    total_callbacks = len(callbacks)
    linked          = [c for c in callbacks if c.get("estimate_id")]
    unlinked_count  = total_callbacks - len(linked)

    if not linked:
        return {
            "total_callbacks": total_callbacks,
            "linked_count":    0,
            "unlinked_count":  unlinked_count,
            "by_product":      [],
            "filters": {
                "year":          year,
                "callback_type": callback_type,
                "min_price":     min_price,
            },
            "note": "No callbacks are linked to an estimate — cannot join to line items.",
        }

    # ── Step 2: fetch line items for linked estimate_ids (batched) ──────────
    estimate_ids  = list({c["estimate_id"] for c in linked})
    all_line_items: list[dict] = []
    batch_size    = 200

    for i in range(0, len(estimate_ids), batch_size):
        batch  = estimate_ids[i : i + batch_size]
        li_res = (
            _get_client()
            .table("estimate_line_items")
            .select("estimate_id, item_name, description, price, line_total")
            .in_("estimate_id", batch)
            .gte("price", min_price)
            .execute()
        )
        all_line_items.extend(li_res.data or [])

    # ── Step 3: map estimate_id → line items ────────────────────────────────
    li_by_estimate: dict[int, list] = defaultdict(list)
    for li in all_line_items:
        li_by_estimate[li["estimate_id"]].append(li)

    # ── Step 4: group callbacks by product ──────────────────────────────────
    # Use a set of task_ids per product to ensure distinct callback counts.
    product_task_ids:  dict[str, set]  = defaultdict(set)
    product_desc:      dict[str, str]  = {}
    no_line_item_count = 0

    for cb in linked:
        est_id = cb["estimate_id"]
        items  = li_by_estimate.get(est_id, [])
        if not items:
            no_line_item_count += 1
            continue
        for li in items:
            key = (li.get("item_name") or "Unknown").strip()
            product_task_ids[key].add(cb["task_id"])
            if key not in product_desc:
                product_desc[key] = (li.get("description") or "").strip()

    # ── Step 5: build ranked list ────────────────────────────────────────────
    by_product = sorted(
        [
            {
                "item_name":      name,
                "description":    product_desc.get(name, ""),
                "callback_count": len(task_ids),
            }
            for name, task_ids in product_task_ids.items()
        ],
        key=lambda x: -x["callback_count"],
    )

    return {
        "total_callbacks":    total_callbacks,
        "linked_count":       len(linked),
        "unlinked_count":     unlinked_count,
        "no_line_item_count": no_line_item_count,
        "by_product":         by_product,
        "filters": {
            "year":          year,
            "callback_type": callback_type,
            "min_price":     min_price,
        },
    }


# ---------------------------------------------------------------------------
# Weekly anomaly digest — flags business health issues vs recent baselines
# ---------------------------------------------------------------------------

def query_weekly_digest() -> dict:
    """
    Run four anomaly checks against Supabase and return a flags array
    for any conditions outside normal range.

    Checks:
      1. Callback rate spike — this week vs 4-week rolling average (>25% = flag)
      2. Stalled active estimates — in ACTIVE status, created > 14 days ago
      3. Overdue open callbacks — task_status = open, created > 7 days ago
      4. Sales rep activity drop — zero new estimates this week after prior 3-week activity

    Note: unlinked payments check is skipped — no payments table in Supabase.
    """
    from datetime import datetime, timezone, timedelta
    from collections import defaultdict

    now          = datetime.now(timezone.utc)
    week_start   = now - timedelta(days=7)
    four_wks_ago = now - timedelta(days=35)
    fourteen_ago = now - timedelta(days=14)

    def _iso(dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    def _days_since(date_str: str) -> int:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return max(0, (now - dt).days)
        except Exception:
            return 0

    flags: list[dict] = []

    # ── Check 1: Callback rate spike ─────────────────────────────────────────
    cb_rows = (
        _get_client()
        .table("callback_tasks")
        .select("task_id, created_date")
        .gte("created_date", _iso(four_wks_ago))
        .order("created_date", desc=True)
        .limit(2000)
        .execute()
    ).data or []

    this_week_cbs = [r for r in cb_rows if r["created_date"] >= _iso(week_start)]
    prior_wk_counts: list[int] = []
    for i in range(1, 5):
        s = _iso(now - timedelta(days=7 * (i + 1)))
        e = _iso(now - timedelta(days=7 * i))
        prior_wk_counts.append(sum(1 for r in cb_rows if s <= r["created_date"] < e))

    prior_avg     = sum(prior_wk_counts) / 4
    this_wk_count = len(this_week_cbs)

    if prior_avg > 0 and this_wk_count > prior_avg * 1.25:
        pct = round((this_wk_count / prior_avg - 1) * 100)
        flags.append({
            "category": "Callbacks",
            "severity": "high" if pct >= 50 else "medium",
            "summary":  f"Callback rate up {pct}% this week vs 4-week average",
            "detail": {
                "this_week_count": this_wk_count,
                "four_week_avg":   round(prior_avg, 1),
                "prior_4_weeks":   prior_wk_counts,
            },
        })

    # ── Check 2: Stalled active estimates (created > 14 days ago) ────────────
    stalled_rows = (
        _get_client()
        .table("estimates")
        .select(
            "estimate_number, customer_name, sales_rep_name, "
            "status_normalized, status_raw, project_type, created_date, total_amount"
        )
        .eq("status_normalized", "ACTIVE")
        .lt("created_date", _iso(fourteen_ago))
        .order("created_date", desc=True)
        .limit(100)
        .execute()
    ).data or []

    if stalled_rows:
        flags.append({
            "category": "Pipeline",
            "severity": "medium",
            "summary":  f"{len(stalled_rows)} active estimate(s) haven't progressed in 14+ days",
            "detail": {
                "count":  len(stalled_rows),
                "sample": [
                    {
                        "estimate":     r["estimate_number"],
                        "customer":     r["customer_name"],
                        "rep":          r.get("sales_rep_name"),
                        "stage":        r.get("status_raw"),
                        "project_type": r.get("project_type"),
                        "days_old":     _days_since(r["created_date"]),
                        "value":        r.get("total_amount"),
                    }
                    for r in stalled_rows[:10]
                ],
            },
        })

    # ── Check 3: Open callbacks older than 7 days ─────────────────────────────
    overdue_rows = (
        _get_client()
        .table("callback_tasks")
        .select(
            "task_id, task_type, assigned_to, "
            "customer_name, estimate_number, created_date"
        )
        .ilike("task_status", "%open%")
        .lt("created_date", _iso(week_start))
        .order("created_date", desc=False)   # oldest first
        .limit(200)
        .execute()
    ).data or []

    if overdue_rows:
        flags.append({
            "category": "Callbacks",
            "severity": "high" if len(overdue_rows) >= 10 else "medium",
            "summary":  f"{len(overdue_rows)} callback task(s) still open after 7+ days",
            "detail": {
                "count":  len(overdue_rows),
                "sample": [
                    {
                        "task_type":   r.get("task_type"),
                        "assigned_to": r.get("assigned_to"),
                        "customer":    r.get("customer_name"),
                        "estimate":    r.get("estimate_number"),
                        "days_open":   _days_since(r["created_date"]),
                    }
                    for r in overdue_rows[:15]
                ],
            },
        })

    # ── Fix 1 + 3: Fetch all ACTIVE estimates for pipeline matrix and rep summary ──
    active_rows = (
        _get_client()
        .table("estimates")
        .select(
            "estimate_id, sales_rep_name, total_amount, "
            "status_raw, project_type, created_date"
        )
        .eq("status_normalized", "ACTIVE")
        .order("created_date", desc=True)
        .limit(2000)
        .execute()
    ).data or []

    # ── Fix 1: Pipeline by project_type × rep (matches Backlog Summary sheet) ─
    matrix: dict[str, dict[str, dict]] = defaultdict(
        lambda: defaultdict(lambda: {"count": 0, "value": 0.0})
    )
    all_amounts: list[float] = []
    for r in active_rows:
        pt  = (r.get("project_type") or "Unspecified").strip() or "Unspecified"
        rep = (r.get("sales_rep_name") or "Unassigned").strip() or "Unassigned"
        amt = float(r.get("total_amount") or 0)
        matrix[pt][rep]["count"] += 1
        matrix[pt][rep]["value"] += amt
        all_amounts.append(amt)

    pipeline_by_type_and_rep = {
        pt: {rep: {"count": v["count"], "value": round(v["value"], 2)}
             for rep, v in sorted(reps.items())}
        for pt, reps in sorted(matrix.items())
    }

    all_zero = bool(all_amounts) and all(a == 0 for a in all_amounts)
    data_quality_note = (
        "All total_amount values are $0 or null — order totals may not be synced yet"
        if all_zero else None
    )

    # ── Fix 3: Rep activity summary (replaces broken rep-drop check) ──────────
    rep_data: dict[str, dict] = defaultdict(
        lambda: {"count": 0, "total_value": 0.0, "by_stage": defaultdict(int)}
    )
    for r in active_rows:
        rep = (r.get("sales_rep_name") or "Unassigned").strip() or "Unassigned"
        if rep.lower() in ("unassigned", "unknown"):
            continue
        amt   = float(r.get("total_amount") or 0)
        stage = (r.get("status_raw") or "Unknown").strip()
        rep_data[rep]["count"] += 1
        rep_data[rep]["total_value"] += amt
        rep_data[rep]["by_stage"][stage] += 1

    rep_summary = sorted(
        [
            {
                "rep":            rep,
                "active_jobs":    v["count"],
                "pipeline_value": round(v["total_value"], 2),
                "by_stage":       dict(
                    sorted(v["by_stage"].items(), key=lambda x: -x[1])
                ),
            }
            for rep, v in rep_data.items()
        ],
        key=lambda x: -x["pipeline_value"],
    )

    # Flag reps with zero active opportunities in the pipeline
    idle_reps = [r["rep"] for r in rep_summary if r["active_jobs"] == 0]
    if idle_reps:
        flags.append({
            "category": "Sales",
            "severity": "low",
            "summary":  f"{len(idle_reps)} rep(s) have no active opportunities in the pipeline",
            "detail":   {"reps": idle_reps},
        })

    result: dict = {
        "generated_at":             now.isoformat(),
        "flags_count":              len(flags),
        "flags":                    flags,
        "pipeline_by_type_and_rep": pipeline_by_type_and_rep,
        "rep_summary":              rep_summary,
        "checks_run": [
            "callback_rate_spike",
            "stalled_active_estimates",
            "overdue_open_callbacks",
            "rep_pipeline_summary",
        ],
        "skipped_checks": [
            "unlinked_payments — no payments table in Supabase",
        ],
    }
    if data_quality_note:
        result["data_quality_note"] = data_quality_note
    return result


# ---------------------------------------------------------------------------
# Callback root-cause analysis — structured post-visit classifications
# ---------------------------------------------------------------------------

def query_callback_causes(
    cause:         str  = "",
    year:          int  = 0,
    billable_only: bool = False,
    assignee:      str  = "",
    limit:         int  = 500,
) -> dict:
    """
    Analyse confirmed root causes of service callbacks using the structured
    fields captured by technicians in Striven (type 124 tasks only).

    Args:
        cause:         Filter by confirmed_cause (partial, case-insensitive).
        year:          Filter to a specific calendar year (0 = all).
        billable_only: If True, only include rows where was_billable = True.
        assignee:      Filter by assigned_to (partial, case-insensitive).
        limit:         Max synced rows to aggregate (default 500).

    Returns a dict with cause breakdown, outcome counts, return-trip counts,
    billability summary, coverage stats, and up to 10 sample work notes.
    """
    from collections import defaultdict

    client = _get_client()

    # Coverage denominators — global counts, no filters applied
    total_all_res = (
        client.table("callback_tasks")
        .select("task_id", count="exact")
        .execute()
    )
    total_all = total_all_res.count or 0

    synced_all_res = (
        client.table("callback_tasks")
        .select("task_id", count="exact")
        .eq("custom_fields_synced", True)
        .execute()
    )
    synced_all = synced_all_res.count or 0

    coverage_pct = round(synced_all / max(total_all, 1) * 100, 1)

    # Main query — always restrict to synced rows
    q = (
        client.table("callback_tasks")
        .select(
            "task_id, confirmed_cause, preliminary_cause, "
            "service_outcome, return_trip_required, was_billable, "
            "work_performed, customer_name, assigned_to, created_date"
        )
        .eq("custom_fields_synced", True)
        .order("created_date", desc=True)
        .limit(limit)
    )

    if cause:
        q = q.ilike("confirmed_cause", f"%{cause}%")
    if year > 0:
        q = (
            q
            .gte("created_date", f"{year}-01-01T00:00:00+00:00")
            .lt("created_date",  f"{year + 1}-01-01T00:00:00+00:00")
        )
    if billable_only:
        q = q.eq("was_billable", True)
    if assignee:
        q = q.ilike("assigned_to", f"%{assignee}%")

    res  = q.execute()
    rows = res.data or []

    total_analyzed = len(rows)

    # ── By confirmed cause ────────────────────────────────────────────────────
    known_causes = ["Part", "Service", "Battery", "User Error", "Unknown"]
    cause_agg: dict[str, dict] = defaultdict(lambda: {"count": 0, "billable_count": 0})

    for r in rows:
        c = r.get("confirmed_cause") or "Unknown"
        if c not in {"Part", "Service", "Battery", "User Error"}:
            c = "Unknown"
        cause_agg[c]["count"] += 1
        if r.get("was_billable"):
            cause_agg[c]["billable_count"] += 1

    by_confirmed_cause = {
        label: {
            "count":          cause_agg[label]["count"],
            "pct":            round(cause_agg[label]["count"] / max(total_analyzed, 1) * 100, 1),
            "billable_count": cause_agg[label]["billable_count"],
        }
        for label in known_causes
    }

    # ── By service outcome (simplified labels) ────────────────────────────────
    outcome_agg: dict[str, int] = defaultdict(int)
    for r in rows:
        o = r.get("service_outcome") or ""
        if "Red"    in o: outcome_agg["Red"]     += 1
        elif "Yellow" in o: outcome_agg["Yellow"] += 1
        elif "Green"  in o: outcome_agg["Green"]  += 1
        else:               outcome_agg["Unknown"] += 1
    by_outcome = dict(outcome_agg)

    # ── By return trip required ────────────────────────────────────────────────
    rtr_agg: dict[str, int] = defaultdict(int)
    for r in rows:
        rtr = r.get("return_trip_required") or "Unknown"
        rtr_agg[rtr] += 1
    return_trip_required = dict(rtr_agg)

    # ── Billable summary ──────────────────────────────────────────────────────
    billable     = sum(1 for r in rows if r.get("was_billable") is True)
    not_billable = sum(1 for r in rows if r.get("was_billable") is False)
    answered     = billable + not_billable
    billable_summary = {
        "billable":     billable,
        "not_billable": not_billable,
        "unanswered":   total_analyzed - answered,
        "billable_pct": round(billable / max(answered, 1) * 100, 1),
    }

    # ── Sample work notes (up to 10 most recent non-null work_performed) ──────
    sample_work_notes = []
    for r in rows:
        if r.get("work_performed") and len(sample_work_notes) < 10:
            sample_work_notes.append({
                "task_id":  r.get("task_id"),
                "customer": r.get("customer_name"),
                "cause":    r.get("confirmed_cause"),
                "note":     r.get("work_performed"),
            })

    return {
        "total_analyzed":     total_analyzed,
        "coverage_pct":       coverage_pct,
        "by_confirmed_cause": by_confirmed_cause,
        "by_outcome":         by_outcome,
        "return_trip_required": return_trip_required,
        "billable_summary":   billable_summary,
        "filters_applied": {
            "cause":         cause,
            "year":          year,
            "billable_only": billable_only,
            "assignee":      assignee,
        },
        "sample_work_notes": sample_work_notes,
    }
