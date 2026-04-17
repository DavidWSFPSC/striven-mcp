"""
backfill_callback_causes.py

Backfills the 10 new custom-field columns in the Supabase callback_tasks
table using the local callback_audit_raw.json file.

No Striven API calls are made -- all data comes from the raw JSON on disk.
Run AFTER migrate_callback_tasks.py has added the new columns.
"""

import json
import os
from collections import defaultdict

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

RAW_JSON_FILE = "callback_audit_raw.json"
BATCH_SIZE    = 200

# Only upsert these columns (plus task_id as the conflict key)
CF_COLUMNS = [
    "task_id",
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


def main() -> None:
    from callback_audit import transform_task, CALLBACK_TYPE_IDS
    from services.supabase_client import _get_client

    print(f"Loading {RAW_JSON_FILE}...", flush=True)
    with open(RAW_JSON_FILE, encoding="utf-8") as f:
        tasks = json.load(f)
    print(f"  {len(tasks):,} records loaded.", flush=True)

    client = _get_client()

    total_processed  = 0
    total_upserted   = 0
    skipped_no_cf    = 0   # type 71/72 -- no callback custom fields expected
    cause_breakdown  = defaultdict(int)
    to_upsert        = []

    for t in tasks:
        total_processed += 1
        row = transform_task(t)

        if not row.get("custom_fields_synced"):
            skipped_no_cf += 1
            continue

        # Only send the columns we're backfilling
        payload = {k: row[k] for k in CF_COLUMNS}
        to_upsert.append(payload)

        cause_breakdown[row.get("confirmed_cause") or "None"] += 1

        # Flush in batches
        if len(to_upsert) >= BATCH_SIZE:
            res = (
                client.table("callback_tasks")
                .upsert(to_upsert, on_conflict="task_id")
                .execute()
            )
            total_upserted += len(res.data) if res.data else len(to_upsert)
            print(f"  Upserted batch: {total_upserted} rows so far...", flush=True)
            to_upsert = []

    # Final flush
    if to_upsert:
        res = (
            client.table("callback_tasks")
            .upsert(to_upsert, on_conflict="task_id")
            .execute()
        )
        total_upserted += len(res.data) if res.data else len(to_upsert)

    # Summary
    synced_count = total_processed - skipped_no_cf
    confirmed_count = sum(v for k, v in cause_breakdown.items() if k != "None")

    print()
    print("=" * 55)
    print("  BACKFILL SUMMARY")
    print("=" * 55)
    print(f"  Total records processed  : {total_processed:>6,}")
    print(f"  Type 124 (synced)        : {synced_count:>6,}")
    print(f"  Skipped (type 71/72)     : {skipped_no_cf:>6,}")
    print(f"  Rows upserted            : {total_upserted:>6,}")
    print(f"  With confirmed_cause     : {confirmed_count:>6,}")
    print()
    print("  -- confirmed_cause breakdown --")
    for cause, count in sorted(cause_breakdown.items(), key=lambda x: -x[1]):
        label = cause if cause != "None" else "(not filled)"
        print(f"    {label:<22} : {count:>5,}")
    print("=" * 55)


if __name__ == "__main__":
    main()
