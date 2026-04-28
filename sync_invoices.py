"""
sync_invoices.py

Syncs invoices from Striven → Supabase invoices table.

Flow:
  1. Authenticate with Striven (client_credentials, Basic auth)
  2. POST /v1/invoices/search, paginate all pages
     Default: all invoices created in the last 3 years (paid + unpaid).
     Use --open-only to restrict to open-balance invoices only (legacy AR mode).
  3. Transform each invoice into the invoices table schema
  4. Upsert into Supabase (conflict on invoice_id)
  5. Print summary: total synced, total open balance, count per aging bucket

Schema columns populated:
    invoice_id       — Striven invoice ID (PK)
    invoice_number   — Striven invoice number / txn number
    txn_number       — alias for invoice_number (legacy)
    txn_date         — date invoice was created
    due_date         — payment due date
    open_balance     — remaining unpaid balance
    total_amount     — full invoice amount (before payments)
    invoice_status   — Striven invoice status name (e.g. "Open", "Paid")
    estimate_id      — linked Striven sales order / estimate ID
    estimate_number  — linked sales order number (e.g. "SO-1234")
    customer_id      — Striven customer ID
    customer_name    — customer display name
    customer_number  — Striven customer number
    payment_terms    — payment terms (not returned by search endpoint)
    memo             — invoice memo / notes
    synced_at        — timestamp of last sync

Usage:
    python sync_invoices.py              # all invoices, last 3 years (default)
    python sync_invoices.py --open-only  # open-balance invoices only
    python sync_invoices.py --years 5   # extend lookback to 5 years

Required env vars (same .env as the rest of the project):
    STRIVEN_CLIENT_ID     — Striven API client ID
    STRIVEN_CLIENT_SECRET — Striven API client secret
    SUPABASE_URL          — Supabase project URL
    SUPABASE_KEY          — Supabase service role key
"""

import argparse
import base64
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timezone, timedelta

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

def _fetch_all_invoices(token: str, open_only: bool = False, years: int = 3) -> list[dict]:
    """
    Fetch invoices from Striven, paginating until all pages are consumed.

    Args:
        open_only: If True, restrict to invoices with an outstanding balance.
                   Default False — syncs all invoices so paid deposits are captured.
        years:     How many years back to fetch (default 3).
                   Only used when open_only=False; AR-only mode fetches all open invoices
                   regardless of age.
    """
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_invoices: list[dict] = []
    page = 1

    # Date filter: fetch invoices created within the last `years` years.
    # Striven uses MM/DD/YYYY format for DateCreatedFrom.
    date_from_str: str | None = None
    if not open_only:
        cutoff = date.today() - timedelta(days=365 * years)
        date_from_str = cutoff.strftime("%m/%d/%Y")

    while True:
        body: dict = {
            "PageNumber": page,
            "PageSize":   PAGE_SIZE,
        }
        if open_only:
            body["WithOpenBalanceOnly"] = True
        if date_from_str:
            body["DateCreatedFrom"] = date_from_str

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
    due_str      = inv.get("dueDate")
    customer     = inv.get("customer")   or {}
    sales_order  = inv.get("salesOrder") or {}
    status       = inv.get("status")     or {}

    # The search endpoint may return total as "total", "amount", or "invoiceAmount"
    total = (
        inv.get("total")
        or inv.get("amount")
        or inv.get("invoiceAmount")
    )

    # Invoice status: search endpoint returns either a string or a {id, name} object
    invoice_status = (
        status.get("name") if isinstance(status, dict)
        else (str(status) if status else None)
    )

    # Estimate / sales order link — this is the key field for deposit tracking
    estimate_id     = sales_order.get("id")     if isinstance(sales_order, dict) else None
    estimate_number = sales_order.get("number") if isinstance(sales_order, dict) else None

    return {
        "invoice_id":      inv["id"],
        "invoice_number":  inv.get("number") or inv.get("txnNumber"),
        "txn_number":      inv.get("txnNumber") or inv.get("number"),  # kept for backward compat
        "txn_date":        inv.get("dateCreated"),
        "due_date":        due_str,
        "open_balance":    inv.get("openBalance") or inv.get("balanceDue"),
        "total_amount":    total,
        "invoice_status":  invoice_status,
        "estimate_id":     estimate_id,
        "estimate_number": estimate_number,
        "customer_id":     customer.get("id"),
        "customer_name":   customer.get("name"),
        "customer_number": customer.get("number"),
        "payment_terms":   None,                    # not returned by search endpoint
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
    parser = argparse.ArgumentParser(description="Sync Striven invoices → Supabase")
    parser.add_argument(
        "--open-only",
        action="store_true",
        help="Restrict to invoices with an outstanding balance (legacy AR mode)",
    )
    parser.add_argument(
        "--years",
        type=int,
        default=3,
        help="How many years back to fetch when not in open-only mode (default: 3)",
    )
    args = parser.parse_args()

    supabase_url = os.environ["SUPABASE_URL"]
    supabase_key = os.environ["SUPABASE_KEY"]
    sb = create_client(supabase_url, supabase_key)

    token    = _get_token()
    invoices = _fetch_all_invoices(token, open_only=args.open_only, years=args.years)

    if not invoices:
        print("No invoices found. Nothing to sync.")
        return

    records = [_transform(inv) for inv in invoices]
    _upsert(sb, records)

    # Summary
    total_balance  = sum((r.get("open_balance") or 0) for r in records)
    linked_count   = sum(1 for r in records if r.get("estimate_id"))
    bucket_counts: dict[str, int] = defaultdict(int)
    for inv in invoices:
        due = _parse_date(inv.get("dueDate"))
        bucket_counts[_aging_bucket(due)] += 1

    print("\n" + "=" * 50)
    print(f"  Invoices synced    : {len(records)}")
    print(f"  Linked to estimate : {linked_count}")
    print(f"  Total open AR      : ${total_balance:,.2f}")
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
