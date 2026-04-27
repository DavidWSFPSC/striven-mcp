"""
sync_customer_locations.py

Syncs all customer location records from Striven → Supabase customer_locations table.

Pages through POST /v1/customer-locations/search (~13K records at 100 per page),
transforms each record, and upserts to Supabase. Safe to re-run — idempotent
upsert on location_id.

This is called nightly by /admin/run-sync. It can also be run manually:

    python sync_customer_locations.py
    python sync_customer_locations.py --limit 200   # test run

Flow:
  1. Authenticate with Striven (OAuth client_credentials)
  2. Call services.sync.sync_customer_locations()
  3. Report results

Required env vars:
    CLIENT_ID            — Striven OAuth client ID
    CLIENT_SECRET        — Striven OAuth client secret
    SUPABASE_URL         — Supabase project URL
    SUPABASE_KEY         — Supabase service role key

Table DDL (run once in Supabase SQL editor if not already created):
    CREATE TABLE IF NOT EXISTS customer_locations (
        location_id   bigint PRIMARY KEY,
        customer_id   bigint NOT NULL,
        customer_name text,
        address1      text,
        city          text,
        city_norm     text,
        state         text,
        postal_code   text,
        is_primary    boolean DEFAULT false,
        synced_at     timestamptz DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_locations_city_norm ON customer_locations(city_norm);
    CREATE INDEX IF NOT EXISTS idx_locations_postal    ON customer_locations(postal_code);
    CREATE INDEX IF NOT EXISTS idx_locations_customer  ON customer_locations(customer_id);
"""

import argparse
import os
import sys
import time

from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Striven customer locations → Supabase"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max locations to sync (omit for full pull)",
    )
    args = parser.parse_args()

    # Validate env vars before doing anything
    for var in ("CLIENT_ID", "CLIENT_SECRET", "SUPABASE_URL", "SUPABASE_KEY"):
        if not os.environ.get(var):
            print(f"ERROR: {var} must be set.", file=sys.stderr)
            sys.exit(1)

    t_start = time.monotonic()

    try:
        from services.striven import StrivenClient
        from services.sync import sync_customer_locations

        striven = StrivenClient()
        result  = sync_customer_locations(striven, limit=args.limit)

    except Exception as exc:
        print(f"[location-sync] Fatal error: {exc}", file=sys.stderr)
        sys.exit(1)

    elapsed = round(time.monotonic() - t_start, 1)
    print(
        f"\n[location-sync] Done — "
        f"{result.get('synced', 0):,} synced, "
        f"{result.get('skipped', 0):,} skipped, "
        f"{result.get('pages', 0)} pages, "
        f"{elapsed}s total",
        flush=True,
    )
    if args.limit:
        print(f"[location-sync] Sample mode — only {args.limit} records synced.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
