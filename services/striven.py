import base64
import os
import time
import requests

# Base URL and auth endpoint read from env vars set in Render
STRIVEN_BASE_URL = os.environ.get("BASE_URL", "https://api.striven.com/v1")
STRIVEN_AUTH_URL = os.environ.get("TOKEN_URL", "https://api.striven.com/accesstoken")


class StrivenClient:
    """
    Client for authenticating with and querying the Striven API.
    Reads CLIENT_ID and CLIENT_SECRET from environment variables.
    """

    def __init__(self):
        self.client_id     = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET")

        if not self.client_id or not self.client_secret:
            raise EnvironmentError(
                "Missing CLIENT_ID or CLIENT_SECRET."
            )

        print(f"[StrivenClient] Initialised — base_url={STRIVEN_BASE_URL} "
              f"client_id={self.client_id[:6]}...", flush=True)

        self._token: str | None = None
        self._token_expires_at: float = 0

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        """Return True if we have a cached token that hasn't expired yet."""
        return self._token is not None and time.time() < self._token_expires_at

    def _fetch_token(self) -> None:
        """Request a new access token using the client_credentials grant.

        Striven requires Basic auth (Base64 ClientID:ClientSecret) in the
        Authorization header, with grant_type in the form body.
        """
        # Encode credentials as Base64 for the Basic auth header
        raw = f"{self.client_id}:{self.client_secret}"
        encoded = base64.b64encode(raw.encode()).decode()

        try:
            print(f"[StrivenClient] Fetching token from: {STRIVEN_AUTH_URL}", flush=True)
            response = requests.post(
                STRIVEN_AUTH_URL,
                headers={
                    "Authorization": f"Basic {encoded}",
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                data={
                    "grant_type": "client_credentials",
                    "ClientId": self.client_id,
                },
                timeout=10,
            )
            print(f"[StrivenClient] Token response status: {response.status_code}", flush=True)
            response.raise_for_status()
            payload = response.json()
        except Exception as e:
            print("STRIVEN INIT ERROR:", str(e), flush=True)
            raise

        self._token = payload["access_token"]
        # Subtract 30 s from the reported expiry as a safety buffer
        expires_in = payload.get("expires_in", 86400)  # Striven tokens last 24 h
        self._token_expires_at = time.time() + expires_in - 30

    def _get_headers(self) -> dict:
        """Return auth headers, refreshing the token first if needed."""
        if not self._is_token_valid():
            self._fetch_token()
        return {"Authorization": f"Bearer {self._token}"}

    # ------------------------------------------------------------------
    # API helpers
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Perform an authenticated GET request and return the JSON response."""
        url = f"{STRIVEN_BASE_URL}{path}"
        response = requests.get(
            url,
            headers=self._get_headers(),
            params=params,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, body: dict | None = None) -> dict:
        """Perform an authenticated POST request and return the JSON response."""
        url = f"{STRIVEN_BASE_URL}{path}"
        response = requests.post(
            url,
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=body or {},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def _patch(self, path: str, body: dict | None = None) -> dict:
        """Perform an authenticated PATCH request and return the JSON response."""
        url = f"{STRIVEN_BASE_URL}{path}"
        response = requests.patch(
            url,
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=body or {},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # Estimate endpoints
    #
    # In Striven, estimates are Sales Orders with status Quoted (19),
    # Pending Approval (20), or Approved (22). There is no separate
    # /estimates resource — the correct endpoints are /sales-orders.
    # ------------------------------------------------------------------

    def get_estimate(self, estimate_id: int) -> dict:
        """Fetch a single sales order (estimate) by its ID.

        GET /v1/sales-orders/{id}
        """
        return self._get(f"/sales-orders/{estimate_id}")

    def update_sales_order(self, order_id: int, body: dict) -> dict:
        """Partial-update a sales order via PATCH.

        PATCH /v1/sales-orders/{id}
        Striven accepts a partial JSON body — only the fields you include
        are changed. Returns the updated sales order.
        """
        return self._patch(f"/sales-orders/{order_id}", body)

    def get_item(self, item_id: int) -> dict:
        """Fetch a single item (product) by its ID.

        GET /v1/items/{id}

        Response includes Category: {Id, Name} which is used for
        exact gas-log category matching in the audit.
        """
        return self._get(f"/items/{item_id}")

    def search_sales_orders(self, filters: dict | None = None) -> dict:
        """
        Alias of search_estimates — makes the internal Striven call explicit.

        POST /v1/sales-orders/search

        "Estimates" in our business = "Sales Orders" in Striven.
        External routes keep the word "estimates"; internally we call
        this method so the code is honest about what endpoint it hits.
        """
        return self.search_estimates(filters)

    def search_estimates(self, filters: dict | None = None) -> dict:
        """
        Search sales orders (estimates) via the POST search endpoint.

        POST /v1/sales-orders/search

        Supported body keys (all optional):
            PageIndex          (int)  — zero-based page number, default 0
            PageSize           (int)  — results per page, default 100
            SortExpression     (str)  — field name to sort by
            SortOrder          (int)  — 1=Ascending (default), 2=Descending
            Number             (str)  — sales order number
            Name               (str)  — sales order name
            CustomerId         (int)  — filter by customer ID
            StatusChangedTo    (int)  — status ID the order changed to
                                        Incomplete=18, Quoted=19,
                                        Pending Approval=20, Approved=22,
                                        In Progress=25, Completed=27
            StatusChangedDateRange  — {DateFrom: ISO str, DateTo: ISO str}
            DateCreatedRange        — {DateFrom: ISO str, DateTo: ISO str}
            LastUpdatedDateRange    — {DateFrom: ISO str, DateTo: ISO str}
        """
        return self._post("/sales-orders/search", body=filters)

    def search_customers(self, name: str, page_size: int = 25) -> dict:
        """
        Search customers by name.

        POST /v1/customers/search

        Note: PageIndex is 0-based for this endpoint (unlike sales-orders).

        Response shape:
            {
                "TotalCount": <int>,
                "Data": [
                    {"Id": <int>, "Name": <str>, "Number": <str>, ...},
                    ...
                ]
            }
        """
        return self._post("/customers/search", body={
            "PageIndex": 0,
            "PageSize":  page_size,
            "Name":      name,
        })

    # ------------------------------------------------------------------
    # Task endpoints  (/v2/tasks)
    #
    # Note: tasks live under /v2, not /v1.  The base URL env var points
    # to /v1, so these methods build the URL manually using the v2 path.
    # ------------------------------------------------------------------

    def _v2_url(self, path: str) -> str:
        """Return a full URL under the v2 base (replaces trailing /v1 with /v2)."""
        base = STRIVEN_BASE_URL.rstrip("/")
        # Handle both ".../v1" and ".../v1/" endings
        if base.endswith("/v1"):
            base = base[:-3]
        return f"{base}/v2{path}"

    def search_tasks(self, filters: dict | None = None) -> dict:
        """
        Search tasks.

        POST /v2/tasks/search

        Useful filter keys (all optional):
            PageIndex        int   — 0-based page, default 0
            PageSize         int   — results per page, default 25
            StatusId         int   — task status ID
            AssignedToId     int   — user ID of assignee
            DueDateRange     dict  — {DateFrom, DateTo}
            RelatedEntityId  int   — linked estimate/project ID
            TaskTypeId       int   — type of task
        """
        url = self._v2_url("/tasks/search")
        response = requests.post(
            url,
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=filters or {},
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def get_task(self, task_id: int) -> dict:
        """
        Fetch a single task by ID.

        GET /v2/tasks/{id}
        """
        url = self._v2_url(f"/tasks/{task_id}")
        response = requests.get(url, headers=self._get_headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    def create_task(self, body: dict) -> dict:
        """
        Create a new task.

        POST /v2/tasks

        Required body keys (minimum):
            Name        str  — task title
            TaskTypeId  int  — task type
        Optional:
            Description     str
            DueDate         str  — ISO date
            AssignedToId    int
            RelatedEntityId int
        """
        url = self._v2_url("/tasks")
        response = requests.post(
            url,
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=body,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def update_task(self, task_id: int, body: dict) -> dict:
        """
        Update an existing task.

        PATCH /v2/tasks/{id}

        Pass only the fields to change — partial update supported.
        Common fields: Name, StatusId, DueDate, AssignedToId, Description
        """
        url = self._v2_url(f"/tasks/{task_id}")
        response = requests.patch(
            url,
            headers={**self._get_headers(), "Content-Type": "application/json"},
            json=body,
            timeout=15,
        )
        response.raise_for_status()
        return response.json()

    def get_all_estimates(self, page_size: int = 100) -> list[dict]:
        """
        Paginate through ALL sales orders and return every record as a flat list.

        Striven's search endpoint is zero-indexed (PageIndex 0, 1, 2 …).
        We keep fetching until we've collected totalCount records.

        Args:
            page_size: Records per API call. Max 100 per Striven limits.

        Returns:
            List of raw sales-order dicts from Striven.
        """
        all_records: list[dict] = []
        page_index = 0

        while True:
            response = self._post("/sales-orders/search", body={
                "PageIndex": page_index,
                "PageSize": page_size,
            })

            batch       = response.get("data") or []
            total_count = response.get("totalCount", 0)

            if not batch:
                print(f"[get_all_estimates] WARNING: 'Data' key missing — keys={list(response.keys())}", flush=True)
                break

            all_records.extend(batch)

            # Stop when we've received every record or the batch is empty
            if len(all_records) >= total_count:
                break

            page_index += 1

        return all_records

    # ------------------------------------------------------------------
    # Financial — Invoices, Bills, Bill Credits, Payments
    # All are POST /search + GET /{id}. Read-only.
    # ------------------------------------------------------------------

    def search_invoices(self, filters: dict | None = None) -> dict:
        """Search invoices. POST /v1/invoices/search
        Filter keys (all optional): PageIndex, PageSize, CustomerId, StatusId,
        DateCreatedRange {DateFrom, DateTo}, DueDateRange {DateFrom, DateTo}
        """
        return self._post("/invoices/search", body=filters or {})

    def get_invoice(self, invoice_id: int) -> dict:
        """Fetch a single invoice by ID. GET /v1/invoices/{id}"""
        return self._get(f"/invoices/{invoice_id}")

    def search_bills(self, filters: dict | None = None) -> dict:
        """Search vendor bills. POST /v1/bills/search
        Filter keys (all optional): PageIndex, PageSize, VendorId, StatusId,
        DateCreatedRange {DateFrom, DateTo}
        """
        return self._post("/bills/search", body=filters or {})

    def get_bill(self, bill_id: int) -> dict:
        """Fetch a single vendor bill by ID. GET /v1/bills/{id}"""
        return self._get(f"/bills/{bill_id}")

    def search_bill_credits(self, filters: dict | None = None) -> dict:
        """Search bill credits. POST /v1/bill-credits/search"""
        return self._post("/bill-credits/search", body=filters or {})

    def get_bill_credit(self, credit_id: int) -> dict:
        """Fetch a single bill credit by ID. GET /v1/bill-credits/{id}"""
        return self._get(f"/bill-credits/{credit_id}")

    def search_payments(self, filters: dict | None = None) -> dict:
        """Search customer payments received. POST /v1/payments/search
        Filter keys (all optional): PageIndex, PageSize, CustomerId,
        DateCreatedRange {DateFrom, DateTo}
        """
        return self._post("/payments/search", body=filters or {})

    def get_payment(self, payment_id: int) -> dict:
        """Fetch a single payment by ID. GET /v1/payments/{id}"""
        return self._get(f"/payments/{payment_id}")

    # ------------------------------------------------------------------
    # Procurement — Purchase Orders
    # ------------------------------------------------------------------

    def search_purchase_orders(self, filters: dict | None = None) -> dict:
        """Search purchase orders. POST /v1/purchase-orders/search
        Filter keys (all optional): PageIndex, PageSize, VendorId, StatusId,
        DateCreatedRange {DateFrom, DateTo}
        """
        return self._post("/purchase-orders/search", body=filters or {})

    def get_purchase_order(self, po_id: int) -> dict:
        """Fetch a single purchase order by ID. GET /v1/purchase-orders/{id}"""
        return self._get(f"/purchase-orders/{po_id}")

    # ------------------------------------------------------------------
    # Catalog — Items / Products
    # ------------------------------------------------------------------

    def search_items(self, filters: dict | None = None) -> dict:
        """Search items (products/services). POST /v1/items/search
        Filter keys (all optional): PageIndex, PageSize, Name,
        ItemTypeId, CategoryId, IsActive
        """
        return self._post("/items/search", body=filters or {})

    def get_item_types(self) -> dict:
        """Return all item types. GET /v1/item-types"""
        return self._get("/item-types")

    def get_inventory_locations(self) -> dict:
        """Return all inventory locations. GET /v1/inventory-locations"""
        return self._get("/inventory-locations")

    # ------------------------------------------------------------------
    # CRM — Customers (expand), Vendors, Contacts, Opportunities
    # ------------------------------------------------------------------

    def get_customer(self, customer_id: int) -> dict:
        """Fetch a single customer by ID. GET /v1/customers/{id}"""
        return self._get(f"/customers/{customer_id}")

    def search_vendors(self, filters: dict | None = None) -> dict:
        """Search vendors. POST /v1/vendors/search
        Filter keys (all optional): PageIndex, PageSize, Name
        """
        return self._post("/vendors/search", body=filters or {})

    def get_vendor(self, vendor_id: int) -> dict:
        """Fetch a single vendor by ID. GET /v1/vendors/{id}"""
        return self._get(f"/vendors/{vendor_id}")

    def search_contacts(self, filters: dict | None = None) -> dict:
        """Search contacts. POST /v1/contacts/search
        Filter keys (all optional): PageIndex, PageSize, Name,
        CustomerId, VendorId
        """
        return self._post("/contacts/search", body=filters or {})

    def get_contact(self, contact_id: int) -> dict:
        """Fetch a single contact by ID. GET /v1/contacts/{id}"""
        return self._get(f"/contacts/{contact_id}")

    def search_opportunities(self, filters: dict | None = None) -> dict:
        """Search opportunities / sales pipeline. POST /v1/opportunities/search
        Filter keys (all optional): PageIndex, PageSize, CustomerId,
        StatusId, DateCreatedRange {DateFrom, DateTo}
        """
        return self._post("/opportunities/search", body=filters or {})

    def get_opportunity(self, opportunity_id: int) -> dict:
        """Fetch a single opportunity by ID. GET /v1/opportunities/{id}"""
        return self._get(f"/opportunities/{opportunity_id}")

    # ------------------------------------------------------------------
    # Reference data — categories, payment terms/methods, task metadata
    # These are small lookup lists; no pagination needed.
    # ------------------------------------------------------------------

    def get_categories(self) -> dict:
        """Return all item/GL categories. GET /v1/categories"""
        return self._get("/categories")

    def get_payment_terms(self) -> dict:
        """Return all payment terms (e.g. Net 30). GET /v1/payment-terms"""
        return self._get("/payment-terms")

    def get_payment_methods(self) -> dict:
        """Return all payment methods (e.g. Check, ACH). GET /v1/payment-methods"""
        return self._get("/payment-methods")

    def get_task_types(self) -> dict:
        """Return all task types. GET /v2/tasks/types"""
        url = self._v2_url("/tasks/types")
        response = requests.get(url, headers=self._get_headers(), timeout=15)
        response.raise_for_status()
        return response.json()

    # ------------------------------------------------------------------
    # HR — Employees
    # ------------------------------------------------------------------

    def get_employees(self, page_index: int = 0, page_size: int = 100) -> dict:
        """
        Return a paginated list of employees.

        GET /v1/employees

        Query params:
            PageIndex  int  — 0-based page number
            PageSize   int  — results per page (default 100)

        Response shape:
            {"TotalCount": int, "Data": [{Id, Name, FirstName, LastName,
             Email, Phone, IsActive, ...}, ...]}
        """
        return self._get("/employees", params={
            "PageIndex": page_index,
            "PageSize":  page_size,
        })

    def search_customers_full(self, filters: dict | None = None) -> dict:
        """
        Full-featured customer search supporting all Striven filter keys.

        POST /v1/customers/search

        Filter keys (all optional):
            PageIndex   int   — 0-based page
            PageSize    int   — results per page
            Name        str   — partial name match
            Number      str   — customer number
            IsActive    bool  — filter active/inactive
        """
        return self._post("/customers/search", body=filters or {})
