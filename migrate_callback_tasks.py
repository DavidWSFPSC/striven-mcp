"""
migrate_callback_tasks.py

Adds 10 custom-field columns to the callback_tasks Supabase table.
Safe to run multiple times -- uses ADD COLUMN IF NOT EXISTS.

Run once before running backfill_callback_causes.py.
"""

import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

MIGRATION_SQL = """
ALTER TABLE callback_tasks
  ADD COLUMN IF NOT EXISTS preliminary_cause    TEXT,
  ADD COLUMN IF NOT EXISTS confirmed_cause      TEXT,
  ADD COLUMN IF NOT EXISTS customer_issue_desc  TEXT,
  ADD COLUMN IF NOT EXISTS work_performed       TEXT,
  ADD COLUMN IF NOT EXISTS service_outcome      TEXT,
  ADD COLUMN IF NOT EXISTS return_trip_required TEXT,
  ADD COLUMN IF NOT EXISTS parts_used           TEXT,
  ADD COLUMN IF NOT EXISTS was_billable         BOOLEAN,
  ADD COLUMN IF NOT EXISTS manager_notes        TEXT,
  ADD COLUMN IF NOT EXISTS custom_fields_synced BOOLEAN DEFAULT FALSE;
"""

NEW_COLUMNS = [
    "preliminary_cause",
    "confirmed_cause",
    "customer_issue_desc",
    "work_performed",
    "service_outcome",
    "return_trip_required",
    "parts_used",
    "was_billable",
    "manager_notes",
    "custom_fields_synced",
]


def run_migration() -> bool:
    """
    Attempt to execute the migration via supabase RPC.
    Falls back to printing SQL for manual execution.
    Returns True if executed via RPC, False if manual action is required.
    """
    from services.supabase_client import _get_client
    client = _get_client()

    try:
        client.rpc("exec_sql", {"query": MIGRATION_SQL}).execute()
        print("PASS: Migration executed via exec_sql RPC.", flush=True)
        return True
    except Exception as e:
        print(f"  exec_sql RPC not available ({type(e).__name__}) -- falling back.", flush=True)

    print()
    print("=" * 60)
    print("MANUAL MIGRATION REQUIRED")
    print("Paste the SQL below into your Supabase SQL Editor:")
    print("  https://supabase.com/dashboard > SQL Editor")
    print("=" * 60)
    print(MIGRATION_SQL.strip())
    print("=" * 60)
    print()
    return False


def verify_columns() -> bool:
    """
    Confirm every new column is accessible by attempting a SELECT.
    Returns True if all columns exist.
    """
    from services.supabase_client import _get_client
    client = _get_client()
    select_cols = ", ".join(NEW_COLUMNS)
    try:
        client.table("callback_tasks").select(select_cols).limit(1).execute()
        print(f"PASS: All {len(NEW_COLUMNS)} new columns verified.", flush=True)
        return True
    except Exception as e:
        print(f"FAIL: Column verification failed: {e}", flush=True)
        return False


if __name__ == "__main__":
    print("--- Phase 2: callback_tasks migration ---", flush=True)
    auto = run_migration()

    print("Verifying columns...", flush=True)
    ok = verify_columns()

    if ok:
        print("\nMigration complete -- proceed to Phase 3.", flush=True)
    else:
        if not auto:
            print(
                "\nRun the SQL above in Supabase, then re-run this script to verify.",
                flush=True,
            )
        else:
            print("\nRPC reported success but verify failed -- check Supabase logs.", flush=True)
