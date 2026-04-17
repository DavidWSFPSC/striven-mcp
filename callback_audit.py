"""
callback_audit.py

Queries Striven for all callback / return-trip tasks and prints a summary report.

Task type IDs (confirmed from live /v2/tasks/types):
  71  = Installer: Return Trip (Unplanned)/Punch Work (ONLY)
  72  = Service: Return Trip (Unplanned) (Only Scheduled By Service Manager)
  124 = Service Diagnostic Repair: Call Back (Only Scheduled By Service Manager)

Usage:
    python callback_audit.py

Credentials read from environment (or .env file):
    STRIVEN_CLIENT_ID
    STRIVEN_CLIENT_SECRET

Output:
    Console summary report
    callback_audit_raw.json — raw task detail records
"""

import os
import json
import time
import requests
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# ---------------------------------------------------------------------------
# Load .env if present
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CLIENT_ID     = os.environ["STRIVEN_CLIENT_ID"]
CLIENT_SECRET = os.environ["STRIVEN_CLIENT_SECRET"]
AUTH_URL      = os.environ.get("TOKEN_URL", "https://api.striven.com/accesstoken")
BASE_V1       = "https://api.striven.com/v1"
BASE_V2       = "https://api.striven.com/v2"

# Task type IDs to audit — confirmed from live Striven data
CALLBACK_TYPE_IDS = {
    71:  "Installer: Return Trip",
    72:  "Service: Return Trip",
    124: "Service: Call Back",
}

PAGE_SIZE    = 100   # Striven max
DETAIL_WORKERS = 10  # parallel GET /v2/tasks/{id} calls
OUTPUT_FILE  = "callback_audit_raw.json"
CSV_FILE     = "callback_audit.csv"

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def get_token() -> str:
    import base64
    print("Authenticating with Striven...", flush=True)
    encoded = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        AUTH_URL,
        headers={
            "Authorization": f"Basic {encoded}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "ClientId":   CLIENT_ID,
        },
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    print("Auth OK.", flush=True)
    return token


def headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Step 1 — collect task stubs via POST /v2/tasks/search
# ---------------------------------------------------------------------------

def fetch_all_stubs(token: str, limit: int | None = None) -> list[dict]:
    """
    Page through tasks via POST /v2/tasks/search.
    Returns raw stubs — each has only id, dateCreated, status.
    limit: if set, stop after fetching this many stubs.
    """
    stubs: list[dict] = []
    page  = 0
    total = None

    print(f"Fetching task stubs (POST /v2/tasks/search){' [SAMPLE: ' + str(limit) + ']' if limit else ''}...", flush=True)

    while True:
        resp = requests.post(
            f"{BASE_V2}/tasks/search",
            headers={**headers(token), "Content-Type": "application/json"},
            json={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        records = data.get("Data") or data.get("data") or []
        if total is None:
            total = data.get("TotalCount") or data.get("totalCount") or 0
            print(f"  Total tasks in Striven: {total:,}", flush=True)

        if not records:
            break

        stubs.extend(records)
        fetched = len(stubs)
        print(f"  Page {page}: fetched {fetched:,}/{total:,}", flush=True)

        if limit and fetched >= limit:
            stubs = stubs[:limit]
            break
        if fetched >= total or len(records) < PAGE_SIZE:
            break
        page += 1

    print(f"Stubs collected: {len(stubs):,}", flush=True)
    return stubs


# ---------------------------------------------------------------------------
# Step 2 — fetch full detail for each stub via GET /v2/tasks/{id}
# ---------------------------------------------------------------------------

def fetch_detail(task_id: int, token: str) -> dict | None:
    """Fetch full task detail. Returns None on error."""
    try:
        resp = requests.get(
            f"{BASE_V2}/tasks/{task_id}",
            headers=headers(token),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"  [warn] Failed to fetch task {task_id}: {exc}", flush=True)
        return None


def fetch_details_parallel(stubs: list[dict], token: str) -> list[dict]:
    """
    Fetch full task detail for all stubs in parallel.
    Filters to only return tasks whose type.id is in CALLBACK_TYPE_IDS.
    """
    print(
        f"Fetching full detail for {len(stubs):,} stubs "
        f"({DETAIL_WORKERS} workers)...",
        flush=True,
    )

    callback_tasks: list[dict] = []
    done = 0

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
        id_key = lambda s: s.get("Id") or s.get("id")
        futures = {pool.submit(fetch_detail, id_key(s), token): s for s in stubs if id_key(s)}

        for future in as_completed(futures):
            done += 1
            if done % 500 == 0:
                print(f"  Processed {done:,}/{len(stubs):,}...", flush=True)

            detail = future.result()
            if not detail:
                continue

            task_type = detail.get("type") or {}
            type_id   = task_type.get("id")
            if type_id in CALLBACK_TYPE_IDS:
                callback_tasks.append(detail)

    print(f"Callback/return-trip tasks found: {len(callback_tasks):,}", flush=True)
    return callback_tasks


# ---------------------------------------------------------------------------
# Custom field value maps — confirmed from live data (Phase 1 analysis)
# ---------------------------------------------------------------------------

# Fields 1329 + 1349 share identical option IDs
CAUSE_MAP: dict[str, str] = {
    "530": "Part",
    "531": "Service",
    "532": "Battery",
    "533": "User Error",
}

OUTCOME_MAP: dict[str, str] = {
    "123": "Red Light - Critical Review Needed",
    "124": "Yellow Light - Return Trip Needed",
    "125": "Green Light - Complete",
}

# Only two values confirmed in dataset; unknown IDs fall back to raw string
RETURN_TRIP_MAP: dict[str, str] = {
    "464": "Scheduled",
    "465": "No",
}

# Sentinels that mean "no answer given" for free-text fields
_FREE_TEXT_NULLS: frozenset[str] = frozenset(
    {"", "0", "not entered", "none", "n/a"}
)

# Field IDs that only appear on Service Call Back tasks (type 124)
_CALLBACK_CF_IDS: frozenset[int] = frozenset(
    {1329, 1349, 1556, 1337, 1328, 1335, 1336, 1359, 1361}
)


def _extract_custom_field(
    fields: list,
    field_id: int,
    value_map: dict | None = None,
) -> str | None:
    """
    Locate a custom field by numeric ID and return its resolved value.

    Dropdown fields (value_map provided):
      1. Return valueText if non-null and non-empty.
      2. Return None for value "0" or null (field not answered).
      3. Return value_map[value] if key found.
      4. Return raw value string for any unrecognized non-"0" value
         (preserves Yes/Waiting etc. if Striven adds new options).

    Free-text fields (no value_map):
      1. Read from `value` (valueText is always null on these fields).
      2. Return None if value is null, empty, or in _FREE_TEXT_NULLS.
    """
    for cf in (fields or []):
        if cf.get("id") != field_id:
            continue

        vt = cf.get("valueText")
        v  = cf.get("value")

        if value_map is not None:
            # Dropdown: try valueText first
            if vt and str(vt).strip():
                return str(vt).strip()
            # Null / unset guard
            if v is None or str(v).strip() == "0":
                return None
            raw = str(v).strip()
            # Map lookup, then fall back to raw string
            return value_map.get(raw, raw)
        else:
            # Free-text: always read from value
            if v is None:
                return None
            raw = str(v).strip()
            if raw.lower() in _FREE_TEXT_NULLS:
                return None
            return raw

    return None


def _extract_billable(fields: list, field_id: int) -> bool | None:
    """
    Extract a boolean stored as the string 'True' or 'False' in value.
    valueText is always null for this field.
    """
    for cf in (fields or []):
        if cf.get("id") != field_id:
            continue
        v = cf.get("value")
        if v == "True":
            return True
        if v == "False":
            return False
        return None
    return None


# ---------------------------------------------------------------------------
# Transform raw task detail → clean flat record for storage
# ---------------------------------------------------------------------------

def transform_task(t: dict) -> dict:
    """Flatten a raw task detail into a clean record for CSV / Supabase."""
    task_type   = t.get("type")        or {}
    status      = t.get("status")      or {}
    sales_order = t.get("salesOrder")  or {}
    assignments = t.get("assignments") or []
    customer    = t.get("customer")    or {}

    info_fields = t.get("infoCustomFields")     or []
    done_fields = t.get("markDoneCustomFields")  or []

    all_cf_ids  = {cf.get("id") for cf in info_fields + done_fields}
    has_cf      = bool(all_cf_ids & _CALLBACK_CF_IDS)

    return {
        "task_id":         t.get("id"),
        "task_type_id":    task_type.get("id"),
        "task_type":       CALLBACK_TYPE_IDS.get(task_type.get("id"), task_type.get("name")),
        "task_status":     status.get("name"),
        "assigned_to":     assignments[0].get("name") if assignments else "Unassigned",
        "customer_id":     customer.get("id"),
        "customer_name":   customer.get("name"),
        "estimate_id":     sales_order.get("id"),
        "estimate_number": sales_order.get("number"),
        "created_date":    t.get("dateCreated"),
        "due_date":        t.get("dueDateTime"),
        # Custom fields — only populated for type 124 (Service Call Back)
        "preliminary_cause":    _extract_custom_field(info_fields, 1329, CAUSE_MAP),
        "confirmed_cause":      _extract_custom_field(info_fields, 1349, CAUSE_MAP),
        "customer_issue_desc":  _extract_custom_field(info_fields, 1328),
        "work_performed":       _extract_custom_field(info_fields, 1335),
        "service_outcome":      _extract_custom_field(info_fields, 1556, OUTCOME_MAP),
        "return_trip_required": _extract_custom_field(info_fields, 1337, RETURN_TRIP_MAP),
        "parts_used":           _extract_custom_field(info_fields, 1336),
        "was_billable":         _extract_billable(done_fields, 1359),
        "manager_notes":        _extract_custom_field(done_fields, 1361),
        "custom_fields_synced": has_cf,
    }


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

def write_csv(records: list[dict]) -> None:
    import csv
    if not records:
        print("No records to write to CSV.", flush=True)
        return
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)
    print(f"CSV saved to {CSV_FILE} ({len(records):,} rows)", flush=True)


# ---------------------------------------------------------------------------
# Supabase upsert
#
# Table DDL (run once in Supabase SQL editor):
#
#   CREATE TABLE IF NOT EXISTS callback_tasks (
#       task_id         bigint PRIMARY KEY,
#       task_type_id    int,
#       task_type       text,
#       task_status     text,
#       assigned_to     text,
#       customer_id     bigint,
#       customer_name   text,
#       estimate_id     bigint,
#       estimate_number text,
#       created_date    timestamptz,
#       due_date        timestamptz,
#       synced_at       timestamptz DEFAULT now()
#   );
#   CREATE INDEX IF NOT EXISTS idx_cb_estimate  ON callback_tasks(estimate_id);
#   CREATE INDEX IF NOT EXISTS idx_cb_assigned  ON callback_tasks(assigned_to);
#   CREATE INDEX IF NOT EXISTS idx_cb_type      ON callback_tasks(task_type_id);
#   CREATE INDEX IF NOT EXISTS idx_cb_created   ON callback_tasks(created_date);
#
# ---------------------------------------------------------------------------

def push_to_supabase(records: list[dict]) -> int:
    """Upsert callback records into Supabase. Returns rows upserted."""
    if not records:
        return 0
    try:
        from services.supabase_client import _get_client
        client = _get_client()
        batch_size = 500
        total = 0
        for i in range(0, len(records), batch_size):
            batch = records[i: i + batch_size]
            res = (
                client.table("callback_tasks")
                .upsert(batch, on_conflict="task_id")
                .execute()
            )
            total += len(res.data) if res.data else 0
            print(f"  Supabase: upserted {total:,}/{len(records):,}", flush=True)
        return total
    except Exception as exc:
        print(f"  [warn] Supabase upsert failed: {exc}", flush=True)
        return 0


# ---------------------------------------------------------------------------
# Step 3 — analyse and report
# ---------------------------------------------------------------------------

def parse_year(dt_str: str | None) -> str:
    if not dt_str:
        return "Unknown"
    try:
        return str(datetime.fromisoformat(dt_str.replace("Z", "+00:00")).year)
    except Exception:
        return "Unknown"


def analyse(tasks: list[dict]) -> None:
    by_year:     dict[str, int]       = defaultdict(int)
    by_status:   dict[str, int]       = defaultdict(int)
    by_type:     dict[str, int]       = defaultdict(int)
    by_assignee: dict[str, int]       = defaultdict(int)
    linked_estimates = 0

    for t in tasks:
        # Year
        year = parse_year(t.get("dateCreated"))
        by_year[year] += 1

        # Status
        status = (t.get("status") or {}).get("name") or "Unknown"
        by_status[status] += 1

        # Task type label
        task_type = (t.get("type") or {}).get("name") or "Unknown"
        by_type[task_type] += 1

        # Assignee (first in list)
        assignments = t.get("assignments") or []
        if assignments:
            assignee = assignments[0].get("name") or "Unknown"
        else:
            assignee = "Unassigned"
        by_assignee[assignee] += 1

        # Linked estimate
        if t.get("salesOrder"):
            linked_estimates += 1

    # -- Print report ----------------------------------------------------------
    print()
    print("=" * 60)
    print("  CALLBACK / RETURN TRIP AUDIT")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 60)

    print(f"\nTOTAL TASKS: {len(tasks):,}")
    print(f"Linked to estimate: {linked_estimates:,} ({linked_estimates/max(len(tasks),1)*100:.1f}%)")

    print("\n-- BY YEAR --")
    for year in sorted(by_year):
        print(f"  {year}: {by_year[year]:>5,}")

    print("\n-- BY STATUS --")
    for status, count in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"  {status:<30} {count:>5,}")

    print("\n-- BY TASK TYPE --")
    for ttype, count in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  {ttype:<55} {count:>5,}")

    print("\n-- TOP ASSIGNEES (most callbacks handled) --")
    top = sorted(by_assignee.items(), key=lambda x: -x[1])[:15]
    for name, count in top:
        print(f"  {name:<35} {count:>5,}")

    print()
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Striven callback/return-trip audit")
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max task stubs to fetch (omit for full pull, use 100-1000 for a sample)"
    )
    parser.add_argument(
        "--no-supabase", action="store_true",
        help="Skip Supabase upsert (CSV + JSON only)"
    )
    args = parser.parse_args()

    t_start = time.monotonic()

    token  = get_token()
    stubs  = fetch_all_stubs(token, limit=args.limit)
    tasks  = fetch_details_parallel(stubs, token)

    # Save raw JSON
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2, default=str)
    print(f"Raw JSON saved to {OUTPUT_FILE}", flush=True)

    # Transform to flat records
    records = [transform_task(t) for t in tasks]

    # CSV
    write_csv(records)

    # Supabase (only if table exists — skips gracefully if not)
    if not args.no_supabase:
        print("Pushing to Supabase...", flush=True)
        pushed = push_to_supabase(records)
        print(f"Supabase: {pushed:,} rows upserted.", flush=True)

    analyse(tasks)

    elapsed = round(time.monotonic() - t_start, 1)
    print(f"Total runtime: {elapsed}s")
    if args.limit:
        print(f"[sample mode -- only {args.limit} stubs scanned, run without --limit for full results]")


if __name__ == "__main__":
    main()
