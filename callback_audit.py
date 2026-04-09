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
# Transform raw task detail → clean flat record for storage
# ---------------------------------------------------------------------------

def transform_task(t: dict) -> dict:
    """Flatten a raw task detail into a clean record for CSV / Supabase."""
    task_type   = t.get("type")        or {}
    status      = t.get("status")      or {}
    sales_order = t.get("salesOrder")  or {}
    assignments = t.get("assignments") or []
    customer    = t.get("customer")    or {}

    return {
        "task_id":       t.get("id"),
        "task_type_id":  task_type.get("id"),
        "task_type":     CALLBACK_TYPE_IDS.get(task_type.get("id"), task_type.get("name")),
        "task_status":   status.get("name"),
        "assigned_to":   assignments[0].get("name") if assignments else "Unassigned",
        "customer_id":   customer.get("id"),
        "customer_name": customer.get("name"),
        "estimate_id":   sales_order.get("id"),
        "estimate_number": sales_order.get("number"),
        "created_date":  t.get("dateCreated"),
        "due_date":      t.get("dueDateTime"),
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
