"""
sync_tasks.py

Syncs ALL task types from Striven → Supabase tasks table.

Unlike callback_audit.py (which targets only types 71/72/124), this script
pulls every task type and stores the core scheduling fields needed for
workload management and the weekly_digest inactive-assignee check.

Flow:
  1. Authenticate with Striven
  2. POST /v2/tasks/search (no type filter), paginate using Data/TotalCount
  3. Fetch full GET /v2/tasks/{id} in parallel (needed for salesOrder, customer,
     assignments — not in the search stub)
  4. Transform and upsert to `tasks` table

Note: Response keys for /v2/tasks are capitalized (Data, TotalCount, Id, etc.)
which differs from v1 endpoints.

Table DDL (run once in Supabase SQL editor):
  CREATE TABLE IF NOT EXISTS tasks (
      task_id                 bigint PRIMARY KEY,
      task_type_id            int,
      task_type               text,
      estimate_id             bigint,
      estimate_number         text,
      customer_name           text,
      assigned_to             text,
      assigned_to_is_inactive bool    DEFAULT false,
      status                  text,
      due_date                timestamptz,
      scheduled_date          timestamptz,
      created_date            timestamptz,
      completed_date          timestamptz,
      notes                   text,
      synced_at               timestamptz DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_tasks_estimate  ON tasks(estimate_id);
  CREATE INDEX IF NOT EXISTS idx_tasks_assigned  ON tasks(assigned_to);
  CREATE INDEX IF NOT EXISTS idx_tasks_type_id   ON tasks(task_type_id);
  CREATE INDEX IF NOT EXISTS idx_tasks_status    ON tasks(status);
  CREATE INDEX IF NOT EXISTS idx_tasks_created   ON tasks(created_date);
  CREATE INDEX IF NOT EXISTS idx_tasks_inactive  ON tasks(assigned_to_is_inactive);

Usage:
    python sync_tasks.py [--limit N] [--no-supabase]

Required env vars:
    STRIVEN_CLIENT_ID
    STRIVEN_CLIENT_SECRET
    SUPABASE_URL
    SUPABASE_KEY
"""

import argparse
import base64
import os
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CLIENT_ID     = os.environ.get("STRIVEN_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("STRIVEN_CLIENT_SECRET", "")
AUTH_URL      = "https://api.striven.com/accesstoken"
BASE_V2       = "https://api.striven.com/v2"

PAGE_SIZE      = 100   # Striven v2 max per page
DETAIL_WORKERS = 10    # parallel GET /v2/tasks/{id} calls
UPSERT_BATCH   = 500

# Task type IDs that belong in callback_tasks (confirmed from live Striven data)
CALLBACK_TYPE_IDS = {
    71:  "Installer: Return Trip",
    72:  "Service: Return Trip",
    124: "Service: Call Back",
}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _get_token() -> str:
    encoded = base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode()).decode()
    resp = requests.post(
        AUTH_URL,
        headers={
            "Authorization": f"Basic {encoded}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "ClientId": CLIENT_ID},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token: {resp.text[:200]}")
    print("[auth] Token acquired.", flush=True)
    return token


def _hdrs(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Step 1 — collect stubs via POST /v2/tasks/search
# ---------------------------------------------------------------------------

def _fetch_all_stubs(token: str, limit: int | None = None) -> list[dict]:
    stubs: list[dict] = []
    page  = 0
    total = None

    print(
        f"[stubs] Fetching task stubs (POST /v2/tasks/search)"
        f"{f' [sample: {limit}]' if limit else ''}...",
        flush=True,
    )

    while True:
        resp = requests.post(
            f"{BASE_V2}/tasks/search",
            headers={**_hdrs(token), "Content-Type": "application/json"},
            json={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # v2 tasks API uses capitalized keys
        rows = data.get("Data") or data.get("data") or []
        if total is None:
            total = data.get("TotalCount") or data.get("totalCount") or 0
            print(f"[stubs] Total tasks in Striven: {total:,}", flush=True)

        if not rows:
            break

        stubs.extend(rows)
        print(f"[stubs] Page {page}: {len(stubs):,}/{total:,}", flush=True)

        if limit and len(stubs) >= limit:
            stubs = stubs[:limit]
            break
        if total and len(stubs) >= total:
            break
        if len(rows) < PAGE_SIZE:
            break
        page += 1

    print(f"[stubs] Collected {len(stubs):,} stubs.", flush=True)
    return stubs


# ---------------------------------------------------------------------------
# Step 2 — fetch full detail in parallel
# ---------------------------------------------------------------------------

def _fetch_detail(task_id: int, token: str) -> dict | None:
    try:
        resp = requests.get(
            f"{BASE_V2}/tasks/{task_id}",
            headers=_hdrs(token),
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[detail] Task {task_id} failed: {exc}", flush=True)
        return None


def _fetch_all_details(stubs: list[dict], token: str) -> list[dict]:
    print(
        f"[detail] Fetching full detail for {len(stubs):,} stubs "
        f"({DETAIL_WORKERS} workers)...",
        flush=True,
    )
    results: list[dict] = []
    done = 0

    def _id(s: dict) -> int | None:
        return s.get("Id") or s.get("id")

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
        futures = {pool.submit(_fetch_detail, _id(s), token): s for s in stubs if _id(s)}
        for future in as_completed(futures):
            done += 1
            if done % 500 == 0:
                print(f"[detail] {done:,}/{len(stubs):,} done...", flush=True)
            detail = future.result()
            if detail:
                results.append(detail)

    print(f"[detail] Fetched {len(results):,} full records.", flush=True)
    return results


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _transform(t: dict) -> dict:
    task_type   = t.get("type")       or t.get("Type")       or {}
    status      = t.get("status")     or t.get("Status")     or {}
    sales_order = t.get("salesOrder") or t.get("SalesOrder") or {}
    customer    = t.get("customer")   or t.get("Customer")   or {}
    assignments = t.get("assignments") or t.get("Assignments") or []

    assigned_to = "Unassigned"
    if assignments:
        first = assignments[0]
        assigned_to = first.get("name") or first.get("Name") or "Unassigned"

    # Inactive flag — Striven appends "(Inactive)" to deactivated employee names
    assigned_to_is_inactive = "(inactive)" in assigned_to.lower()

    return {
        "task_id":                 t.get("id") or t.get("Id"),
        "task_type_id":            task_type.get("id") or task_type.get("Id"),
        "task_type":               task_type.get("name") or task_type.get("Name"),
        "estimate_id":             sales_order.get("id") or sales_order.get("Id"),
        "estimate_number":         sales_order.get("number") or sales_order.get("Number"),
        "customer_name":           customer.get("name") or customer.get("Name"),
        "assigned_to":             assigned_to,
        "assigned_to_is_inactive": assigned_to_is_inactive,
        "status":                  status.get("name") or status.get("Name"),
        "due_date":                t.get("dueDateTime") or t.get("DueDateTime"),
        "scheduled_date":          t.get("scheduledDate") or t.get("ScheduledDate"),
        "created_date":            t.get("dateCreated") or t.get("DateCreated"),
        "completed_date":          t.get("dateCompleted") or t.get("DateCompleted"),
        "notes":                   t.get("description") or t.get("Description"),
        "synced_at":               datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("tasks").upsert(batch, on_conflict="task_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


# ---------------------------------------------------------------------------
# Callback tasks — secondary upsert into callback_tasks table
# ---------------------------------------------------------------------------

def _to_callback_row(t: dict) -> dict:
    """
    Map a raw Striven task detail → callback_tasks schema.
    callback_tasks uses task_status (not status) and includes customer_id.
    """
    task_type   = t.get("type")        or t.get("Type")        or {}
    status      = t.get("status")      or t.get("Status")      or {}
    sales_order = t.get("salesOrder")  or t.get("SalesOrder")  or {}
    customer    = t.get("customer")    or t.get("Customer")     or {}
    assignments = t.get("assignments") or t.get("Assignments")  or []

    assigned_to = "Unassigned"
    if assignments:
        first       = assignments[0]
        assigned_to = first.get("name") or first.get("Name") or "Unassigned"

    return {
        "task_id":        t.get("id")              or t.get("Id"),
        "task_type_id":   task_type.get("id")      or task_type.get("Id"),
        "task_type":      task_type.get("name")    or task_type.get("Name"),
        "task_status":    status.get("name")       or status.get("Name"),
        "assigned_to":    assigned_to,
        "customer_id":    customer.get("id")       or customer.get("Id"),
        "customer_name":  customer.get("name")     or customer.get("Name"),
        "estimate_id":    sales_order.get("id")    or sales_order.get("Id"),
        "estimate_number": sales_order.get("number") or sales_order.get("Number"),
        "created_date":   t.get("dateCreated")     or t.get("DateCreated"),
        "due_date":       t.get("dueDateTime")     or t.get("DueDateTime"),
        "synced_at":      datetime.now(timezone.utc).isoformat(),
    }


def _upsert_callbacks(client, records: list[dict]) -> None:
    """Upsert callback-type tasks into the callback_tasks table."""
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("callback_tasks").upsert(batch, on_conflict="task_id").execute()
        print(f"[callback_upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def _report(records: list[dict]) -> None:
    by_type:     dict[str, int] = defaultdict(int)
    by_status:   dict[str, int] = defaultdict(int)
    inactive_cnt = 0

    for r in records:
        by_type[r.get("task_type") or "Unknown"] += 1
        by_status[r.get("status") or "Unknown"]  += 1
        if r.get("assigned_to_is_inactive"):
            inactive_cnt += 1

    print("\n" + "=" * 60)
    print("  TASK SYNC SUMMARY")
    print(f"  Total tasks synced : {len(records):,}")
    print(f"  Inactive assignees : {inactive_cnt:,}")
    print("\n  By type:")
    for t, c in sorted(by_type.items(), key=lambda x: -x[1])[:20]:
        print(f"    {t:<45} {c:>5,}")
    print("\n  By status:")
    for s, c in sorted(by_status.items(), key=lambda x: -x[1]):
        print(f"    {s:<30} {c:>5,}")
    print("=" * 60)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Sync all Striven tasks to Supabase")
    parser.add_argument("--limit", type=int, default=None,
                        help="Max stubs to fetch (omit for full pull)")
    parser.add_argument("--no-supabase", action="store_true",
                        help="Skip Supabase upsert (report only)")
    args = parser.parse_args()

    if not CLIENT_ID or not CLIENT_SECRET:
        print("ERROR: STRIVEN_CLIENT_ID and STRIVEN_CLIENT_SECRET must be set.", file=sys.stderr)
        sys.exit(1)

    t_start = time.monotonic()

    token   = _get_token()
    stubs   = _fetch_all_stubs(token, limit=args.limit)
    details = _fetch_all_details(stubs, token)

    records = [_transform(t) for t in details]
    records = [r for r in records if r.get("task_id")]

    # Build callback_tasks rows from raw details (before transform discards customer_id)
    callback_raws = [
        t for t in details
        if ((t.get("type") or t.get("Type") or {}).get("id")
            or (t.get("type") or t.get("Type") or {}).get("Id"))
        in CALLBACK_TYPE_IDS
    ]
    callback_records = [_to_callback_row(t) for t in callback_raws]
    callback_records = [r for r in callback_records if r.get("task_id")]

    if not args.no_supabase:
        sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])
        _upsert(sb, records)

        if callback_records:
            print(
                f"[sync] Writing {len(callback_records):,} callback tasks "
                f"to callback_tasks table...",
                flush=True,
            )
            _upsert_callbacks(sb, callback_records)
            print(f"[sync] callback_tasks upsert complete.", flush=True)
        else:
            print("[sync] No callback-type tasks found in this sync.", flush=True)

    _report(records)
    print(
        f"\n[sync] Callbacks synced to callback_tasks: {len(callback_records):,} "
        f"(types 71/72/124)",
        flush=True,
    )
    print(f"\nTotal runtime: {round(time.monotonic() - t_start, 1)}s", flush=True)
    if args.limit:
        print(f"[sample mode — only {args.limit} stubs scanned]")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
