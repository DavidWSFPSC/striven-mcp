"""
services/sync.py

Data pipeline: Striven → Supabase

This module owns the ETL (Extract, Transform, Load) logic:
  - Extract: pull raw records from Striven via StrivenClient
  - Transform: map Striven's response shape to our clean Supabase schema
  - Load: batch-upsert into Supabase via supabase_client

READ/WRITE POLICY:
  - Striven is READ-ONLY. No modifications are ever made to Striven data.
  - All writes go exclusively to Supabase (our own controlled data layer).
"""

from services.striven import StrivenClient
from services.supabase_client import insert_estimates

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Max records sent to Supabase in a single upsert call.
# Keeping this at 100 avoids hitting Supabase's request size limits.
BATCH_SIZE = 100


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _transform_estimate(raw: dict) -> dict:
    """
    Map a raw Striven sales-order record to our `estimates` table schema.

    Striven field          → Supabase column
    ─────────────────────────────────────────
    id                     → id              (primary key, int)
    number                 → estimate_number (string)
    customer.name          → customer_name   (string)
    status.name            → status          (string)
    total                  → total           (float, may be null)
    dateCreated            → created_date    (ISO 8601 string)
    lastUpdatedDate        → updated_date    (ISO 8601 string)

    Any missing or null fields are stored as None (NULL in Supabase).
    """
    customer = raw.get("customer") or {}
    status   = raw.get("status")   or {}

    return {
        "id":              raw.get("id"),
        "estimate_number": raw.get("number"),
        "customer_name":   customer.get("name"),
        "status":          status.get("name"),
        "total":           raw.get("total"),       # may be null on summary view
        "created_date":    raw.get("dateCreated"),
        "updated_date":    raw.get("lastUpdatedDate"),
    }


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def sync_estimates_to_supabase(limit: int | None = 50) -> int:
    """
    Pull estimates from Striven, transform them, and upsert into Supabase.

    Args:
        limit:  Cap on how many Striven records to process in one sync run.
                Defaults to 50 for safe initial testing.
                Pass None to sync all records (use with caution on 9 000+ rows).

    Returns:
        Number of records successfully sent to Supabase.

    Flow:
        1. StrivenClient.get_all_estimates() — paginated read from Striven
        2. Apply `limit` slice if set
        3. _transform_estimate() — map to Supabase schema
        4. insert_estimates() — batch upsert to Supabase (BATCH_SIZE rows each)
    """
    client = StrivenClient()

    # ── 1. Extract ──────────────────────────────────────────────────────────
    # get_all_estimates() paginates Striven automatically (100 records/call).
    # We pass page_size=100 here; pagination stops at totalCount.
    raw_records = client.get_all_estimates(page_size=100)

    # ── 2. Limit (for safe test runs) ───────────────────────────────────────
    if limit is not None:
        raw_records = raw_records[:limit]

    # ── 3. Transform ─────────────────────────────────────────────────────────
    transformed = [_transform_estimate(r) for r in raw_records]

    # ── 4. Load (batch upsert → Supabase) ────────────────────────────────────
    total_synced = 0

    for i in range(0, len(transformed), BATCH_SIZE):
        batch = transformed[i : i + BATCH_SIZE]
        insert_estimates(batch)          # upsert — safe to re-run at any time
        total_synced += len(batch)

    return total_synced
