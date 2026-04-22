"""
sync_items.py

Syncs the Striven product/service catalog → Supabase items table.

Flow:
  1. Authenticate with Striven
  2. POST /v1/items/search, paginate all pages (PageIndex 0-based)
  3. Transform and upsert to `items` table

Table DDL (run once in Supabase SQL editor):
  CREATE TABLE IF NOT EXISTS items (
      item_id     bigint PRIMARY KEY,
      name        text,
      description text,
      item_type   text,
      category    text,
      price       numeric,
      is_active   bool,
      synced_at   timestamptz DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_items_name     ON items(name);
  CREATE INDEX IF NOT EXISTS idx_items_category ON items(category);
  CREATE INDEX IF NOT EXISTS idx_items_active   ON items(is_active);

Usage:
    python sync_items.py

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


def _fetch_all_items(token: str) -> list[dict]:
    hdrs  = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    items: list[dict] = []
    page  = 0
    total = None

    while True:
        resp = requests.post(
            f"{STRIVEN_BASE_URL}/items/search",
            headers=hdrs,
            json={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if total is None:
            total = data.get("totalCount", 0) if isinstance(data, dict) else 0
            print(f"[fetch] Total items in Striven: {total:,}", flush=True)

        if not rows:
            break

        items.extend(rows)
        print(
            f"[fetch] Page {page}: {len(rows)} items "
            f"(running total: {len(items):,}/{total or '?'})",
            flush=True,
        )

        if total and len(items) >= total:
            break
        if len(rows) < PAGE_SIZE:
            break
        page += 1

    return items


def _transform(r: dict) -> dict:
    item_type = r.get("itemType") or r.get("ItemType") or {}
    category  = r.get("category") or r.get("Category") or {}

    return {
        "item_id":     r.get("id") or r.get("Id"),
        "name":        r.get("name") or r.get("Name"),
        "description": r.get("description") or r.get("Description"),
        "item_type": (
            item_type.get("name") or item_type.get("Name")
            if isinstance(item_type, dict) else str(item_type) if item_type else None
        ),
        "category": (
            category.get("name") or category.get("Name")
            if isinstance(category, dict) else str(category) if category else None
        ),
        "price":     (
            r.get("price") or r.get("Price")
            or r.get("unitPrice") or r.get("UnitPrice")
        ),
        "is_active":   r.get("isActive") if r.get("isActive") is not None else r.get("IsActive"),
        "synced_at":   datetime.now(timezone.utc).isoformat(),
    }


def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("items").upsert(batch, on_conflict="item_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


def main() -> None:
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    token    = _get_token()
    raw_items = _fetch_all_items(token)

    if not raw_items:
        print("No items found. Nothing to sync.")
        return

    records = [_transform(r) for r in raw_items]
    records = [r for r in records if r.get("item_id")]

    _upsert(sb, records)

    active   = sum(1 for r in records if r.get("is_active"))
    inactive = len(records) - active
    print("\n" + "=" * 50)
    print(f"  Items synced : {len(records):,}")
    print(f"  Active       : {active:,}")
    print(f"  Inactive     : {inactive:,}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
