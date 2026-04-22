"""
sync_employees.py

Syncs the Striven employee roster → Supabase employees table.

Flow:
  1. Authenticate with Striven
  2. GET /v1/employees (paginated, PageIndex 0-based)
  3. Transform and upsert to `employees` table

Note: GET /v1/employees may return either a plain list or a paginated dict
depending on the account configuration. Both shapes are handled.

Table DDL (run once in Supabase SQL editor):
  CREATE TABLE IF NOT EXISTS employees (
      employee_id bigint PRIMARY KEY,
      name        text,
      first_name  text,
      last_name   text,
      email       text,
      phone       text,
      is_active   bool,
      synced_at   timestamptz DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_emp_name   ON employees(name);
  CREATE INDEX IF NOT EXISTS idx_emp_active ON employees(is_active);

Usage:
    python sync_employees.py

Required env vars:
    STRIVEN_CLIENT_ID
    STRIVEN_CLIENT_SECRET
    SUPABASE_URL
    SUPABASE_KEY
"""

import base64
import os
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

STRIVEN_AUTH_URL = "https://api.striven.com/accesstoken"
STRIVEN_BASE_URL = "https://api.striven.com/v1"
PAGE_SIZE        = 200
UPSERT_BATCH     = 500


def _get_token() -> str:
    client_id     = os.environ["STRIVEN_CLIENT_ID"].strip()
    client_secret = os.environ["STRIVEN_CLIENT_SECRET"].strip()
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    resp = requests.post(
        STRIVEN_AUTH_URL,
        headers={
            "Authorization": f"Basic {encoded}",
            "Content-Type":  "application/x-www-form-urlencoded",
        },
        data={"grant_type": "client_credentials", "ClientId": client_id},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {resp.text[:200]}")
    print("[auth] Token acquired.", flush=True)
    return token


def _fetch_all_employees(token: str) -> list[dict]:
    hdrs      = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    employees: list[dict] = []
    page      = 0

    while True:
        resp = requests.get(
            f"{STRIVEN_BASE_URL}/employees",
            headers=hdrs,
            params={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # Handle both list response and paginated dict response
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = data.get("data") or data.get("Data") or []
        else:
            rows = []

        if not rows:
            break

        employees.extend(rows)
        print(
            f"[fetch] Page {page}: {len(rows)} employees "
            f"(running total: {len(employees):,})",
            flush=True,
        )

        # If returned a raw list it's all records — no further pagination needed
        if isinstance(data, list) or len(rows) < PAGE_SIZE:
            break
        page += 1

    print(f"[fetch] Total employees fetched: {len(employees):,}", flush=True)
    return employees


def _transform(r: dict) -> dict:
    first = r.get("firstName") or r.get("FirstName") or ""
    last  = r.get("lastName")  or r.get("LastName")  or ""
    full  = r.get("name") or r.get("Name") or " ".join(filter(None, [first, last])) or None

    is_active = r.get("isActive")
    if is_active is None:
        is_active = r.get("IsActive")

    return {
        "employee_id": r.get("id") or r.get("Id"),
        "name":        full,
        "first_name":  first or None,
        "last_name":   last  or None,
        "email":       r.get("email") or r.get("Email"),
        "phone":       r.get("phone") or r.get("Phone"),
        "is_active":   is_active,
        "synced_at":   datetime.now(timezone.utc).isoformat(),
    }


def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("employees").upsert(batch, on_conflict="employee_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


def main() -> None:
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    token         = _get_token()
    raw_employees = _fetch_all_employees(token)

    if not raw_employees:
        print("No employees found. Nothing to sync.")
        return

    records = [_transform(r) for r in raw_employees]
    records = [r for r in records if r.get("employee_id")]

    _upsert(sb, records)

    active   = sum(1 for r in records if r.get("is_active"))
    inactive = len(records) - active
    print("\n" + "=" * 50)
    print(f"  Employees synced : {len(records):,}")
    print(f"  Active           : {active:,}")
    print(f"  Inactive         : {inactive:,}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
