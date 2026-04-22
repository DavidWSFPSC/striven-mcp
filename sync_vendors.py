"""
sync_vendors.py

Syncs vendors from Striven → Supabase vendors table.

Flow:
  1. Authenticate with Striven
  2. POST /v1/vendors/search, paginate all pages (PageIndex 0-based)
  3. Transform and upsert to `vendors` table

Table DDL (run once in Supabase SQL editor):
  CREATE TABLE IF NOT EXISTS vendors (
      vendor_id    bigint PRIMARY KEY,
      name         text,
      number       text,
      email        text,
      phone        text,
      contact_name text,
      is_active    bool,
      synced_at    timestamptz DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_vendors_name ON vendors(name);

Usage:
    python sync_vendors.py

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
PAGE_SIZE        = 500
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


def _fetch_all_vendors(token: str) -> list[dict]:
    hdrs    = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    vendors: list[dict] = []
    page    = 0
    total   = None

    while True:
        resp = requests.post(
            f"{STRIVEN_BASE_URL}/vendors/search",
            headers=hdrs,
            json={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if total is None:
            total = data.get("totalCount", 0) if isinstance(data, dict) else 0
            print(f"[fetch] Total vendors in Striven: {total:,}", flush=True)

        if not rows:
            break

        vendors.extend(rows)
        print(
            f"[fetch] Page {page}: {len(rows)} vendors "
            f"(running total: {len(vendors):,}/{total or '?'})",
            flush=True,
        )

        if total and len(vendors) >= total:
            break
        if len(rows) < PAGE_SIZE:
            break
        page += 1

    return vendors


def _transform(r: dict) -> dict:
    return {
        "vendor_id":    r.get("id") or r.get("Id"),
        "name":         r.get("name") or r.get("Name"),
        "number":       r.get("number") or r.get("Number"),
        "email":        r.get("email") or r.get("Email"),
        "phone":        r.get("phone") or r.get("Phone"),
        "contact_name": r.get("contactName") or r.get("ContactName"),
        "is_active":    r.get("isActive") if r.get("isActive") is not None else r.get("IsActive"),
        "synced_at":    datetime.now(timezone.utc).isoformat(),
    }


def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("vendors").upsert(batch, on_conflict="vendor_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


def main() -> None:
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    token       = _get_token()
    raw_vendors = _fetch_all_vendors(token)

    if not raw_vendors:
        print("No vendors found. Nothing to sync.")
        return

    records = [_transform(r) for r in raw_vendors]
    records = [r for r in records if r.get("vendor_id")]

    _upsert(sb, records)

    active   = sum(1 for r in records if r.get("is_active"))
    inactive = len(records) - active
    print("\n" + "=" * 50)
    print(f"  Vendors synced : {len(records):,}")
    print(f"  Active         : {active:,}")
    print(f"  Inactive       : {inactive:,}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
