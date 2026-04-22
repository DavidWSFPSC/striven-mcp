"""
sync_payments.py

Syncs customer payments from Striven → Supabase payments table.

Flow:
  1. Authenticate with Striven (client_credentials, Basic auth)
  2. POST /v1/payments/search, paginate all pages (PageIndex 0-based)
  3. Transform each payment into the payments table schema
  4. Upsert into Supabase (conflict on payment_id)
  5. Best-effort update of invoices.open_balance for linked invoices

Table DDL (run once in Supabase SQL editor):
  CREATE TABLE IF NOT EXISTS payments (
      payment_id       bigint PRIMARY KEY,
      invoice_id       bigint,
      customer_id      bigint,
      customer_name    text,
      amount           numeric,
      payment_date     timestamptz,
      payment_method   text,
      reference_number text,
      memo             text,
      synced_at        timestamptz DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS idx_pay_customer ON payments(customer_id);
  CREATE INDEX IF NOT EXISTS idx_pay_invoice  ON payments(invoice_id);
  CREATE INDEX IF NOT EXISTS idx_pay_date     ON payments(payment_date);

Usage:
    python sync_payments.py

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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

STRIVEN_AUTH_URL = "https://api.striven.com/accesstoken"
STRIVEN_BASE_URL = "https://api.striven.com/v1"
PAGE_SIZE        = 500
UPSERT_BATCH     = 500


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _fetch_all_payments(token: str) -> list[dict]:
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    all_payments: list[dict] = []
    page  = 0
    total = None

    while True:
        resp = requests.post(
            f"{STRIVEN_BASE_URL}/payments/search",
            headers=hdrs,
            json={"PageIndex": page, "PageSize": PAGE_SIZE},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        rows = data.get("data") if isinstance(data, dict) else (data if isinstance(data, list) else [])
        if total is None:
            total = data.get("totalCount", 0) if isinstance(data, dict) else 0
            print(f"[fetch] Total payments in Striven: {total:,}", flush=True)

        if not rows:
            break

        all_payments.extend(rows)
        print(
            f"[fetch] Page {page}: {len(rows)} payments "
            f"(running total: {len(all_payments):,}/{total or '?'})",
            flush=True,
        )

        if total and len(all_payments) >= total:
            break
        if len(rows) < PAGE_SIZE:
            break
        page += 1

    return all_payments


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _transform(p: dict) -> dict:
    customer = p.get("customer") or p.get("Customer") or {}
    method   = p.get("paymentMethod") or p.get("PaymentMethod") or {}
    invoice  = p.get("invoice") or p.get("Invoice") or {}

    method_name = None
    if isinstance(method, dict):
        method_name = method.get("name") or method.get("Name")
    elif method:
        method_name = str(method)

    invoice_id = None
    if isinstance(invoice, dict):
        invoice_id = invoice.get("id") or invoice.get("Id")

    return {
        "payment_id":      p.get("id") or p.get("Id"),
        "invoice_id":      invoice_id,
        "customer_id":     customer.get("id") or customer.get("Id"),
        "customer_name":   customer.get("name") or customer.get("Name"),
        "amount":          (
            p.get("amount") or p.get("Amount")
            or p.get("total") or p.get("Total")
        ),
        "payment_date":    (
            p.get("paymentDate") or p.get("PaymentDate")
            or p.get("dateCreated") or p.get("DateCreated")
        ),
        "payment_method":  method_name,
        "reference_number": (
            p.get("reference") or p.get("Reference")
            or p.get("checkNumber") or p.get("CheckNumber")
        ),
        "memo":            p.get("memo") or p.get("Memo"),
        "synced_at":       datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def _upsert(client, records: list[dict]) -> None:
    for i in range(0, len(records), UPSERT_BATCH):
        batch = records[i : i + UPSERT_BATCH]
        client.table("payments").upsert(batch, on_conflict="payment_id").execute()
        print(f"[upsert] Rows {i + 1}–{i + len(batch)} written.", flush=True)


# ---------------------------------------------------------------------------
# Best-effort open_balance update for linked invoices
# ---------------------------------------------------------------------------

def _update_invoice_balances(client, records: list[dict]) -> None:
    """
    For payments that carry an invoice_id, reduce that invoice's open_balance
    by the payment amount (floor at 0).

    Authoritative open_balance values always come from sync_invoices.py.
    This pass handles payments processed between invoice syncs.
    """
    by_invoice: dict[int, float] = {}
    for r in records:
        inv_id = r.get("invoice_id")
        if not inv_id:
            continue
        by_invoice[int(inv_id)] = by_invoice.get(int(inv_id), 0.0) + float(r.get("amount") or 0)

    if not by_invoice:
        print(
            "[balance] No invoice_id associations in payment data — "
            "skipping open_balance update. Run sync_invoices.py for authoritative AR.",
            flush=True,
        )
        return

    updated = 0
    for inv_id, paid_total in by_invoice.items():
        try:
            res = (
                client.table("invoices")
                .select("invoice_id, open_balance")
                .eq("invoice_id", inv_id)
                .execute()
            )
            row = (res.data or [None])[0]
            if not row:
                continue
            current = float(row.get("open_balance") or 0)
            new_bal = max(round(current - paid_total, 2), 0.0)
            if new_bal != current:
                client.table("invoices").update({"open_balance": new_bal}).eq("invoice_id", inv_id).execute()
                updated += 1
        except Exception as exc:
            print(f"[balance] Invoice {inv_id} update failed: {exc}", flush=True)

    print(f"[balance] Updated open_balance on {updated} invoice(s).", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

    token    = _get_token()
    payments = _fetch_all_payments(token)

    if not payments:
        print("No payments found. Nothing to sync.")
        return

    records = [_transform(p) for p in payments]
    records = [r for r in records if r.get("payment_id")]

    _upsert(sb, records)
    _update_invoice_balances(sb, records)

    total_amount = sum(float(r.get("amount") or 0) for r in records)
    print("\n" + "=" * 50)
    print(f"  Payments synced : {len(records):,}")
    print(f"  Total amount    : ${total_amount:,.2f}")
    print("=" * 50)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
