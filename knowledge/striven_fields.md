# WilliamSmith Fireplaces — Striven Field Reference

## API Overview

- **Base URL:** `https://api.striven.com/v1`
- **Auth:** OAuth2 client credentials (POST to `https://api.striven.com/accesstoken`)
- **Key resource:** Sales Orders = Estimates (same thing in Striven)

---

## Sales Order — Search Result Fields

Returned by `POST /v1/sales-orders/search` (TitleCase keys):

| Field | Type | Notes |
|-------|------|-------|
| Id | int | Striven internal ID |
| Number | string | Human-readable estimate number (e.g. "SO-4521") |
| Name | string | Estimate name — should follow naming convention |
| Customer.Id | int | Customer ID |
| Customer.Name | string | Customer display name |
| Status.Id | int | Status code (18/19/20/22/25/27) |
| Status.Name | string | Status label ("Approved", "In Progress", etc.) |
| SalesRep.Id | int | Sales rep user ID |
| SalesRep.Name | string | Sales rep display name |
| DateCreated | ISO datetime | When estimate was created |
| DateApproved | ISO datetime | When estimate was approved (null if not yet) |
| OrderTotal | decimal | Total estimate value (not always populated in search) |

---

## Sales Order — Full Detail Fields

Returned by `GET /v1/sales-orders/{id}` (camelCase keys):

| Field | Type | Notes |
|-------|------|-------|
| id | int | Same as search result Id |
| number | string | Estimate number |
| name | string | Estimate name |
| customer | object | {id, name} |
| status | object | {id, name} |
| salesRep | object | {id, name} |
| dateCreated | ISO datetime | |
| dateApproved | ISO datetime | |
| orderTotal | decimal | Full total including all line items |
| lineItems | array | See line item fields below |
| customFields | array | See custom fields below |
| notes | string | Internal notes on the estimate |

---

## Line Item Fields

Each element in `lineItems` array:

| Field | Type | Notes |
|-------|------|-------|
| item.name | string | Product/service name |
| item.id | int | Catalog item ID |
| description | string | Line item description |
| quantity | decimal | Quantity ordered |
| unitPrice | decimal | Price per unit |
| total | decimal | quantity × unitPrice |

**Important:** Line items are ONLY available on the full detail endpoint
(`GET /v1/sales-orders/{id}`). Search results do not include line items.

---

## Custom Fields

Custom fields are returned in the `customFields` array on the full detail endpoint.
Each element: `{ name: string, value: string | null }`

### Known Custom Fields

| Field Name | Values | Used For |
|---|---|---|
| Product Type | "Burner & Gas Logs", "Gas Fireplaces", "Wood Burning Fireplaces", "Electric Fireplaces", "Chimney Repair", "Fireplace Enhancements", "Service", "Warranty" | Classifying the job type; drives audit rules |
| Customer Portal | "Yes" / null | Whether customer can see the estimate in the portal |

**How to access:**
```python
custom_fields = raw.get("customFields") or []
product_type = next(
    (f.get("value") for f in custom_fields if f.get("name") == "Product Type"),
    None
)
```

**Note:** Custom fields require the full detail call. The search endpoint
does not return custom fields. This is why gas log audits and portal flag
audits must fetch each estimate individually.

---

## Tasks — /v2/tasks

Tasks live at `/v2/tasks` (not `/v1`).

**Search:** `POST /v2/tasks/search`
**Get one:** `GET /v2/tasks/{id}`

### Task Fields

| Field | Type | Notes |
|-------|------|-------|
| Id | int | Task ID |
| Name | string | Task name |
| TaskType.Name | string | Task type label (e.g. "Site Inspections/Preview", "Installation") |
| Status.Name | string | "Open", "On Hold", "Completed", "Cancelled" |
| AssignedTo.Name | string | Technician or rep the task is assigned to |
| DueDate | ISO datetime | Scheduled date for the task |
| DateCreated | ISO datetime | When task was created |
| RelatedEntity.Id | int | ID of the linked sales order (estimate) |
| RelatedEntity.Name | string | Name of the linked sales order |

### Linking Tasks to Estimates
Filter by `RelatedEntityId` in the search body:
```json
{ "RelatedEntityId": 4521, "PageSize": 25 }
```
This returns all tasks linked to estimate ID 4521.

### Known Task Types
| Task Type Name | Usage |
|---|---|
| Site Inspections/Preview | Pre-install site visit (required for remodel/new construction) |
| Installation | Primary install task |
| Service | Service call |
| Follow-up | Post-install check or callback |

---

## Customers — /v1/customers

**Search:** `POST /v1/customers/search` (body: `{ Name: "...", PageSize: 25 }`)
**Get one:** `GET /v1/customers/{id}`

Response uses camelCase: `{ id, name, number, email, phone }`

---

## Pagination

All search endpoints use:
- `PageIndex` — 0-based page number
- `PageSize` — records per page (max 100)
- Response: `{ data: [], totalCount: N }`

To get all records, paginate until `collected >= totalCount`.

---

## Status Code Quick Reference

| Code | Name |
|------|------|
| 18 | Incomplete |
| 19 | Quoted |
| 20 | Pending Approval |
| 22 | Approved |
| 25 | In Progress |
| 27 | Completed |
