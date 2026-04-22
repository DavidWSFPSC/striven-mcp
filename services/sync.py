"""
services/sync.py

Full data pipeline: Striven → Supabase

Responsibilities:
  - Step 1: Paginate POST /v1/sales-orders/search to collect all estimate IDs
  - Step 2: Fetch full GET /v1/sales-orders/{id} for every ID (parallel, batched)
  - Step 3: Transform each detail record into estimates + line_items schema
  - Step 4: Upsert into Supabase in batches

No limits.  No shortcuts.  salesRep always comes from the GET detail endpoint.

READ/WRITE POLICY:
  - Striven is READ-ONLY.  This module never modifies Striven data.
  - All writes go exclusively to Supabase.

CONCURRENCY:
  - GET detail calls are parallelised within each page (DETAIL_WORKERS workers).
  - We process one page (SEARCH_PAGE_SIZE stubs) at a time so peak memory
    stays bounded regardless of total estimate count.

FIELD MAPPING (confirmed from live GET /v1/sales-orders/{id} responses):
  Striven field               Supabase column
  ─────────────────────────── ─────────────────────────────────────────────
  id                          estimate_id          (int, primary key)
  orderNumber                 estimate_number      (text)
  customer.id                 customer_id          (int)
  customer.name               customer_name        (text)
  salesRep.id                 sales_rep_id         (int | null)
  salesRep.name               sales_rep_name       (text, default "Unassigned")
  status.id                   status_id            (int)
  status.name                 status_raw           (text)
  <derived>                   status_normalized    (text: ACTIVE|COMPLETE|TERMINAL|INCOMPLETE|UNKNOWN)
  orderTotal                  total_amount         (numeric)
  dateCreated                 created_date         (timestamptz)
  orderDate                   order_date           (timestamptz)
  targetDate                  target_date          (timestamptz)
  customFields[id=1506]       project_type         (text)
  customFields[id=1507]       product_type         (text)
  customFields[id=1559]       project_manager      (text)
  <derived from lineItems>    has_gas_logs         (bool)
  <derived from lineItems>    has_removal_fee      (bool)
  isChangeOrder               is_change_order      (bool)
  invoiceStatus.name          invoice_status       (text)

  Line items (estimate_line_items table):
  lineItems[].id              line_item_id         (int, primary key)
  lineItems[].item.id         item_id              (int)
  lineItems[].item.name       item_name            (text)
  lineItems[].description     description          (text)
  lineItems[].qty             quantity             (numeric)
  lineItems[].price           price                (numeric)
  price * qty                 line_total           (numeric)
  bool(itemGroupLineItems)    is_group             (bool)
"""

import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from services.striven import StrivenClient
from services.supabase_client import (
    upsert_full_estimates,
    upsert_line_items,
    upsert_sales_reps,
    upsert_customer_locations,
)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SEARCH_PAGE_SIZE = 100      # stubs fetched per search call (Striven max)
DETAIL_WORKERS   = 8        # parallel GET /sales-orders/{id} calls per page
UPSERT_BATCH     = 200      # rows per Supabase upsert call

# Status normalization — maps Striven status id → business lifecycle bucket
STATUS_NORMALIZED: dict[int, str] = {
    18: "INCOMPLETE",   # Incomplete — data entry not finished
    19: "ACTIVE",       # Quoted     — sent to customer, awaiting approval
    20: "ACTIVE",       # Pending Approval
    22: "ACTIVE",       # Approved   — customer said yes
    25: "ACTIVE",       # In Progress — job underway
    27: "COMPLETE",     # Completed  — job done and invoiced
    # Lost / Cancelled are not enumerated in Striven's documented codes;
    # any unknown id falls through to "UNKNOWN" via the dict.get() default.
}

# Gas log detection keywords — matched against line item text (lowercased)
GAS_LOG_KEYWORDS   = ("gas log", "burner")
REMOVAL_FEE_TOKENS = ("removal", "log")     # BOTH must appear in same text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cf(custom_fields: list, field_id: int) -> str | None:
    """
    Extract a value from customFields[] by numeric field id.

    customFields shape (confirmed from live data):
        {"id": int, "name": str, "value": str|null, "valueText": str|null}

    Returns valueText if set, else value, else None.
    Handles repeated field ids (multi-value) by returning the first non-null.
    """
    for cf in (custom_fields or []):
        if cf.get("id") == field_id:
            val = cf.get("valueText") or cf.get("value")
            if val and str(val).strip() and str(val) != "0":
                return str(val).strip()
    return None


def _item_text(li: dict) -> str:
    """
    Return lowercased combined text of a line item's item.name + description.
    Used for gas log / removal fee keyword detection.
    """
    item = li.get("item") or {}
    parts = [
        item.get("name") or "",
        li.get("description") or "",
    ]
    return " ".join(p for p in parts if p).lower()


def _detect_gas_flags(line_items: list) -> tuple[bool, bool]:
    """
    Scan line items and return (has_gas_logs, has_removal_fee).

    has_gas_logs   = any line item text contains "gas log" or "burner"
    has_removal_fee = any line item text contains BOTH "removal" AND "log"

    Confirmed correct against the gas log audit logic in app.py.
    """
    has_gas   = False
    has_remov = False

    for li in line_items:
        text = _item_text(li)
        if not has_gas and any(kw in text for kw in GAS_LOG_KEYWORDS):
            has_gas = True
        if not has_remov and all(t in text for t in REMOVAL_FEE_TOKENS):
            has_remov = True
        if has_gas and has_remov:
            break

    return has_gas, has_remov


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _transform(detail: dict) -> tuple[dict, list[dict]]:
    """
    Map one full GET /v1/sales-orders/{id} response into:
      - one estimates row (dict)
      - N estimate_line_items rows (list of dicts)

    Returns (estimate_row, line_item_rows).
    Both are ready for direct Supabase upsert.
    """
    customer     = detail.get("customer")     or {}
    status       = detail.get("status")       or {}
    sales_rep    = detail.get("salesRep")     or {}
    invoice_st   = detail.get("invoiceStatus") or {}
    cf           = detail.get("customFields") or []
    raw_items    = detail.get("lineItems")    or []

    estimate_id  = detail.get("id")
    status_id    = status.get("id")
    rep_name     = sales_rep.get("name") or "Unassigned"
    rep_id       = sales_rep.get("id")

    has_gas, has_remov = _detect_gas_flags(raw_items)

    estimate_row = {
        "estimate_id":       estimate_id,
        "estimate_number":   detail.get("orderNumber"),
        "customer_id":       customer.get("id"),
        "customer_name":     customer.get("name"),
        "sales_rep_id":      rep_id,
        "sales_rep_name":    rep_name,
        "status_id":         status_id,
        "status_raw":        status.get("name"),
        "status_normalized": STATUS_NORMALIZED.get(status_id, "UNKNOWN"),
        "total_amount":      detail.get("orderTotal"),
        "created_date":      detail.get("dateCreated"),
        "order_date":        detail.get("orderDate"),
        "target_date":       detail.get("targetDate"),
        "project_type":      _cf(cf, 1506),
        "product_type":      _cf(cf, 1507),
        "project_manager":   _cf(cf, 1559),
        "has_gas_logs":      has_gas,
        "has_removal_fee":   has_remov,
        "is_change_order":   bool(detail.get("isChangeOrder", False)),
        "invoice_status":    invoice_st.get("name"),
    }

    line_item_rows = [
        {
            "line_item_id": li.get("id"),
            "estimate_id":  estimate_id,
            "item_id":      (li.get("item") or {}).get("id"),
            "item_name":    (li.get("item") or {}).get("name"),
            "description":  li.get("description"),
            "quantity":     li.get("qty"),
            "price":        li.get("price"),
            "line_total":   round(
                (li.get("price") or 0) * (li.get("qty") or 1), 2
            ),
            "is_group":     bool(li.get("itemGroupLineItems")),
        }
        for li in raw_items
        if li.get("id")         # skip any malformed rows
    ]

    return estimate_row, line_item_rows


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

def sync_estimates_to_supabase(limit: int | None = None) -> int:
    """
    Full pipeline: Striven → Supabase.

    Step 1: Paginate POST /v1/sales-orders/search to collect all stubs.
    Step 2: For each page of stubs, parallel-fetch GET /v1/sales-orders/{id}.
    Step 3: Transform every detail record (estimates + line items).
    Step 4: Batch-upsert into Supabase (estimates, line_items, sales_reps).

    Args:
        limit: If set, stop after processing this many estimates.
               None (default) = process ALL estimates in Striven.

    Returns:
        Total number of estimates upserted into Supabase.

    Notes:
        - salesRep is always taken from the full GET detail, never the stub.
        - Estimates and line items are upserted (safe to re-run).
        - sales_reps table is kept in sync from every batch.
        - Memory stays bounded: only SEARCH_PAGE_SIZE detail records live
          in RAM at any moment regardless of total estimate count.
    """
    client = StrivenClient()
    t_start = time.monotonic()

    total_synced  = 0
    page_index    = 0
    grand_total   = None   # set from first search response

    # Accumulate across pages before final flush
    est_buffer:   list[dict] = []
    li_buffer:    list[dict] = []
    rep_seen:     dict[int, str] = {}   # rep_id → rep_name

    print("[sync] Starting full Striven → Supabase sync.", flush=True)

    while True:
        # ── Step 1: fetch one page of search stubs ──────────────────────────
        search_resp  = client.search_sales_orders({
            "PageIndex": page_index,
            "PageSize":  SEARCH_PAGE_SIZE,
        })
        stubs        = search_resp.get("data") or []
        if grand_total is None:
            grand_total = search_resp.get("totalCount", 0)
            print(f"[sync] Total estimates in Striven: {grand_total}", flush=True)

        if not stubs:
            print(f"[sync] Page {page_index}: empty — stopping.", flush=True)
            break

        print(
            f"[sync] Page {page_index}: {len(stubs)} stubs "
            f"({total_synced}/{grand_total if grand_total else '?'} done)",
            flush=True,
        )

        # Collect IDs from this page
        ids = [s.get("Id") or s.get("id") for s in stubs if s.get("Id") or s.get("id")]

        # Apply limit: trim ids if we'd exceed it
        if limit is not None:
            remaining = limit - total_synced
            if remaining <= 0:
                break
            ids = ids[:remaining]

        # ── Step 2: parallel GET full detail for each ID ────────────────────
        details: list[dict] = []
        errors  = 0

        def _fetch(est_id: int) -> dict | None:
            try:
                return client.get_estimate(est_id)
            except Exception as exc:
                print(f"[sync] GET {est_id} failed: {exc}", flush=True)
                return None

        with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
            futures = {pool.submit(_fetch, eid): eid for eid in ids}
            for fut in as_completed(futures):
                result = fut.result()
                if result:
                    details.append(result)
                else:
                    errors += 1

        print(
            f"[sync] Page {page_index}: fetched {len(details)} details, "
            f"{errors} errors",
            flush=True,
        )

        # ── Step 3: transform ────────────────────────────────────────────────
        for detail in details:
            est_row, li_rows = _transform(detail)
            est_buffer.append(est_row)
            li_buffer.extend(li_rows)

            # Track unique reps for the sales_reps table
            rep_id   = est_row.get("sales_rep_id")
            rep_name = est_row.get("sales_rep_name", "Unassigned")
            if rep_id and rep_id not in rep_seen:
                rep_seen[rep_id] = rep_name

        # ── Step 4: flush estimates FIRST, then line items ──────────────────
        # Estimates are always written before line items so FK constraints
        # are never violated regardless of buffer sizes.

        # 4a. Flush ALL estimates accumulated so far (sub-batched for memory)
        while est_buffer:
            batch      = est_buffer[:UPSERT_BATCH]
            upsert_full_estimates(batch)
            total_synced += len(batch)
            est_buffer   = est_buffer[UPSERT_BATCH:]
            print(f"[sync] Upserted {total_synced} estimates so far.", flush=True)

        # 4b. Now safe to flush line items — every parent estimate is in Supabase
        while li_buffer:
            batch     = li_buffer[:UPSERT_BATCH]
            upsert_line_items(batch)
            li_buffer = li_buffer[UPSERT_BATCH:]

        # Flush reps every page (small table, cheap)
        if rep_seen:
            upsert_sales_reps([
                {"rep_id": rid, "rep_name": rname}
                for rid, rname in rep_seen.items()
            ])

        # ── Check stop conditions ────────────────────────────────────────────
        if limit is not None and total_synced >= limit:
            print(f"[sync] Limit {limit} reached — stopping.", flush=True)
            break

        if grand_total is not None and (page_index + 1) * SEARCH_PAGE_SIZE >= grand_total:
            print(f"[sync] All {grand_total} estimates fetched.", flush=True)
            break

        page_index += 1

    # ── Final flush of remaining buffers ─────────────────────────────────────
    if est_buffer:
        upsert_full_estimates(est_buffer)
        total_synced += len(est_buffer)
        print(f"[sync] Final estimates flush: {len(est_buffer)} rows.", flush=True)

    if li_buffer:
        upsert_line_items(li_buffer)
        print(f"[sync] Final line_items flush: {len(li_buffer)} rows.", flush=True)

    if rep_seen:
        upsert_sales_reps([
            {"rep_id": rid, "rep_name": rname}
            for rid, rname in rep_seen.items()
        ])

    # Refresh materialized views that depend on estimates + line_items
    _refresh_materialized_views()

    elapsed = round(time.monotonic() - t_start, 1)
    print(
        f"[sync] Complete — {total_synced} estimates synced in {elapsed}s.",
        flush=True,
    )
    return total_synced


def _refresh_materialized_views() -> None:
    """
    Refresh Supabase materialized views after an estimates sync.

    Views refreshed:
      customer_ltv      — lifetime value per customer (total spend, job count, avg order)
      conversion_rates  — estimate-to-win rates by sales rep and project type

    DDL for these views (run once in Supabase SQL editor):

      -- customer_ltv
      CREATE MATERIALIZED VIEW IF NOT EXISTS customer_ltv AS
      SELECT
          customer_id,
          customer_name,
          COUNT(*)                                        AS total_jobs,
          SUM(total_amount)                               AS lifetime_value,
          ROUND(AVG(total_amount)::numeric, 2)            AS avg_order_value,
          MAX(created_date)                               AS last_job_date,
          MIN(created_date)                               AS first_job_date
      FROM estimates
      WHERE status_normalized IN ('ACTIVE', 'COMPLETE')
      GROUP BY customer_id, customer_name;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_ltv_customer ON customer_ltv(customer_id);

      -- conversion_rates
      CREATE MATERIALIZED VIEW IF NOT EXISTS conversion_rates AS
      SELECT
          sales_rep_name,
          project_type,
          COUNT(*)                                        AS total_estimates,
          SUM(CASE WHEN status_normalized IN ('ACTIVE','COMPLETE') THEN 1 ELSE 0 END)
                                                          AS won_estimates,
          ROUND(
              SUM(CASE WHEN status_normalized IN ('ACTIVE','COMPLETE') THEN 1 ELSE 0 END)
              * 100.0 / NULLIF(COUNT(*), 0), 1
          )                                               AS conversion_rate_pct,
          ROUND(AVG(total_amount)::numeric, 2)            AS avg_deal_size
      FROM estimates
      WHERE status_normalized != 'INCOMPLETE'
      GROUP BY sales_rep_name, project_type;

      CREATE UNIQUE INDEX IF NOT EXISTS idx_conv_rep_type
          ON conversion_rates(sales_rep_name, project_type);
    """
    from services.supabase_client import _get_client

    for view in ("customer_ltv", "conversion_rates"):
        try:
            _get_client().rpc(
                "exec_sql",
                {"sql": f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view};"},
            ).execute()
            print(f"[sync] Refreshed materialized view: {view}", flush=True)
        except Exception as exc:
            # exec_sql RPC may not exist — view refresh is best-effort.
            # Create the view manually in the Supabase SQL editor using the DDL above.
            print(f"[sync] Could not refresh {view}: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Customer location sync — Striven → Supabase
# ---------------------------------------------------------------------------

LOCATION_PAGE_SIZE  = 100   # records per Striven page (max)
LOCATION_UPSERT_BATCH = 500 # rows per Supabase upsert


def _transform_location(loc: dict) -> dict | None:
    """
    Map one raw customer-locations record into a customer_locations row.

    Raw shape (confirmed from live GET /striven/customer-locations):
        {
          "id": int,
          "name": str,
          "isPrimary": bool,
          "customer": {"id": int, "name": str, "number": str},
          "address": {
              "address1": str, "city": str, "state": str,
              "postalCode": str, "fullAddress": str
          },
          "phones": null
        }
    """
    loc_id   = loc.get("id")
    customer = loc.get("customer") or {}
    address  = loc.get("address")  or {}

    if not loc_id or not customer.get("id"):
        return None

    city_raw  = (address.get("city")       or "").strip()
    city_norm = city_raw.lower()

    return {
        "location_id":   loc_id,
        "customer_id":   customer.get("id"),
        "customer_name": customer.get("name"),
        "address1":      (address.get("address1") or "").strip() or None,
        "city":          city_raw or None,
        "city_norm":     city_norm or None,
        "state":         (address.get("state")      or "").strip() or None,
        "postal_code":   (address.get("postalCode") or "").strip() or None,
        "is_primary":    bool(loc.get("isPrimary", False)),
    }


def sync_customer_locations(striven: StrivenClient, limit: int | None = None) -> dict:
    """
    Page through all customer locations in Striven and upsert into Supabase.

    Args:
        striven: Authenticated StrivenClient instance.
        limit:   Optional cap on total locations to sync (None = all).

    Returns:
        {"synced": int, "skipped": int, "pages": int, "elapsed_s": float}
    """
    t_start    = time.monotonic()
    page       = 0
    total_seen = 0
    synced     = 0
    skipped    = 0
    buffer: list[dict] = []

    print("[location-sync] Starting customer location sync...", flush=True)

    while True:
        try:
            resp = striven.search_customer_locations({
                "PageIndex": page,
                "PageSize":  LOCATION_PAGE_SIZE,
            })
        except Exception as exc:
            print(f"[location-sync] API error on page {page}: {exc}", flush=True)
            break

        # Response shape: {"totalCount": N, "data": [...]}
        raw_list = resp.get("data") or resp.get("Data") or []
        total_count = resp.get("totalCount") or resp.get("TotalCount") or 0

        if not raw_list:
            break

        for loc in raw_list:
            row = _transform_location(loc)
            if row:
                buffer.append(row)
            else:
                skipped += 1

        total_seen += len(raw_list)
        print(
            f"[location-sync] Page {page}: {len(raw_list)} records "
            f"({total_seen}/{total_count} total)",
            flush=True,
        )

        # Flush buffer in batches
        while len(buffer) >= LOCATION_UPSERT_BATCH:
            batch = buffer[:LOCATION_UPSERT_BATCH]
            buffer = buffer[LOCATION_UPSERT_BATCH:]
            n = upsert_customer_locations(batch)
            synced += n

        # Check stopping conditions
        if limit and total_seen >= limit:
            print(f"[location-sync] Reached limit={limit}, stopping.", flush=True)
            break
        if total_seen >= total_count or len(raw_list) < LOCATION_PAGE_SIZE:
            break

        page += 1

    # Final flush
    if buffer:
        n = upsert_customer_locations(buffer)
        synced += n

    elapsed = round(time.monotonic() - t_start, 1)
    print(
        f"[location-sync] Complete — {synced} locations synced, "
        f"{skipped} skipped, {page + 1} pages, {elapsed}s",
        flush=True,
    )
    return {
        "synced":    synced,
        "skipped":   skipped,
        "pages":     page + 1,
        "elapsed_s": elapsed,
    }
