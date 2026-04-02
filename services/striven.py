import base64
import os
import time
import requests

# Base URL for the Striven API
STRIVEN_BASE_URL = "https://api.striven.com/v1"
# Auth endpoint lives at the root — no /v1 prefix
STRIVEN_AUTH_URL = "https://api.striven.com/accesstoken"


class StrivenClient:
    """
    Client for authenticating with and querying the Striven API.
    Handles the client_credentials OAuth2 flow and token caching.
    """

    def __init__(self):
        self.client_id = os.environ["STRIVEN_CLIENT_ID"]
        self.client_secret = os.environ["STRIVEN_CLIENT_SECRET"]
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
        response.raise_for_status()
        payload = response.json()

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

            batch = response.get("data", [])
            total_count = response.get("totalCount", 0)

            all_records.extend(batch)

            # Stop when we've received every record or the batch is empty
            if not batch or len(all_records) >= total_count:
                break

            page_index += 1

        return all_records
