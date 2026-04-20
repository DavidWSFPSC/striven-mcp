"""
sync_invoices.py

Syncs open-balance invoices from Striven → Supabase invoices table.

Flow:
  1. Authenticate with Striven (client_credentials, Basic auth)
  2. POST /v1/invoices/search with WithOpenBalanceOnly=true, paginate all pages
  3. Transform each invoice into the invoices table schema
  4. Upsert into Supabase (conflict on invoice_id)
  5. Print summary: total synced, total open balance, count per aging bucket

Usage:
    python sync_invoices.py

Required env vars (same .env as the rest of the project):
    STRIVEN_CLIENT_ID     — Striven API client ID
    STRIVEN_CLIENT_SECRET — Striven API client secret
    SUPABASE_URL          — Supabase project URL
    SUPABASE_KEY          — Supabase service role key
"""

import base64
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone

import requests
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STRIVEN_AUTH_URL = "https://api.striven.com/accesstoken"
STRIVEN_BASE_URL = "https://api.striven.com/v1"
PAGE_SIZE        = 1000
UPSERT_BATCH     = 500

AGING_BUCKETS = ["Current", "1-30 Days", "31-60 Days", "61-90 Days", "90+ Days"]


# ---------------------------------------------------------------------------
# Striven auth
# ---------------------------------------------------------------------------

def _get_token() -> str:
    client_id     = os.environ["STRIVEN_CLIENT_ID"].strip()
    client_secret = os.environ["STRIVEN_CLIENT_SECRET"].strip()
    encoded = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    resp = requests.post(
        STRIVEN_AUTH_URL,
        headers={
            "Authorization": f"Basic {encoded}",
            "Content-Type": "application/x-www-form-urlencoded",
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


# ---------------------------------------------------------------------------
# Fetch invoices from Striven
# ---------------------------------------------------------------------------

def _fetch_all_invoices(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_invoices: list[dict] = []
    page = 1

    while True:
        body = {
            "PageNumber":        page,
            "PageSize":          PAGE_SIZE,
            "WithOpenBalanceOnly": True,
        }
        resp = requests.post(
            f"{STRIVEN_BASE_URL}/invoices/search",
            headers=headers,
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data") if isinstance(data, dict) else data
        if not rows:
            break

        all_invoices.extend(rows)
        print(f"[fetch] Page {page}: {len(rows)} invoices (running total: {len(all_invoices)})", flush=True)

        if len(rows) < PAGE_SIZE:
            break
        page += 1

    return all_invoices


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _aging_bucket(due: date | None) -> str:
    if due is None:
        return "Unknown"
    today = date.today()
    if today <= due:
        return "Current"
    days = (today - due).days
    if days <= 30:
        return "1-30 Days"
    if days <= 60:
        return "31-60 Days"
    if days <= 90:
        return "61-90 Days"
    return "90+ Days"


def _days_outstanding(due: date | None) -> int | None:
    if due is None:
        return None
    return max((date.today() - due).days, 0)


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).date()
    except Exception:
        return None


def _transform(inv: dict) -> dict:
    due_str  = inv.get("dueDate")
    customer = inv.get("customer") or {}

    return {
        "invoice_id":      inv["id"],
        "txn_number":      inv.get("txnNumber"),
        "txn_date":        inv.get("dateCreated"),   # Striven search endpoint returns dateCreated
        "due_date":        due_str,
        "open_balance":    inv.get("openBalance"),
        "customer_id":     customer.get("id"),
        "customer_name":   customer.get("name"),
        "customer_number": customer.get("number"),   # null if not returned by search endpoint
        "payment_terms":   None,                     # not returned by search endpoint
        "memo":            inv.get("memo"),
        "synced_at":       datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Upsert to Supabase
# ---------------------------------------------------------------------------

def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("invoices").upsert(batch, on_conflict="invoice_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_KEY"]
    sb = create_client(supabase_url, supabase_key)

    token    = _get_token()
    invoices = _fetch_all_invoices(token)

    if not invoices:
        print("No open-balance invoices found. Nothing to sync.")
        return

    records = [_transform(inv) for inv in invoices]
    _upsert(sb, records)

    # Summary
    total_balance  = sum((r.get("open_balance") or 0) for r in records)
    bucket_counts: dict[str, int] = defaultdict(int)
    for inv in invoices:
        due = _parse_date(inv.get("dueDate"))
        bucket_counts[_aging_bucket(due)] += 1

    print("\n" + "=" * 50)
    print(f"  Invoices synced : {len(records)}")
    print(f"  Total open AR   : ${total_balance:,.2f}")
    print("  By aging bucket :")
    for bucket in AGING_BUCKETS:
        count = bucket_counts.get(bucket, 0)
        if count:
            print(f"    {bucket:<15} {count}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
