# WilliamSmith Fireplaces — Customer Types & Key Accounts

## Two Customer Categories

### Homeowners
- Individual people purchasing for their own residence
- Typically one or two estimates per customer (one fireplace, maybe a service later)
- Shorter sales cycle — decisions made within days to a few weeks
- Higher callback rate — more likely to call about issues post-install
- Charleston-area residential neighborhoods

### Builders / General Contractors
- Companies contracting WilliamSmith to supply and install fireplaces in new builds or remodels
- High volume — a single builder may have 10–30 open estimates simultaneously
- Longer timeline — dependent on construction schedule (months, not weeks)
- Invoiced on net terms (Net 30 is common)
- "Stuck in Quoted" is expected behavior for builder jobs not yet broken ground
- Builder customers drive the majority of WilliamSmith's revenue volume

---

## Known Builder / Contractor Accounts

These are recurring contractor customers. When a user asks about "builders" or
"our contractor pipeline", these are the primary accounts to look for:

| Builder Name | Notes |
|---|---|
| Scenic Custom Homes | High-volume builder; frequent Isokern new construction |
| Artisan Custom Homes | Regular; mix of gas and wood-burning |
| Sunnyside Builders | Active; new construction focus |
| CL Structures | Active builder account |
| Spire Contracting | Regular; varied job types |
| Unique Constructors | Active |
| Ocean Homes | Charleston-area new construction |
| Isle Custom Homes | Island-area focus (Daniel Island, Sullivan's Island area) |
| New Beginnings Construction | Active builder |
| Allshore Builders | Active |
| CSC Residential | Regular account |
| BeachLife Development | Active; coastal area builds |
| Diament Building Corp | Active |

**Note:** This list is not exhaustive. Use `search_customers` or
`search_estimates_by_customer` to find all estimates for any customer.
New builder relationships should be added here when identified.

---

## Flags to Watch For

### Builder with Many Stuck Jobs
If a builder has 5+ estimates all sitting in "Quoted" for 60+ days:
- Either they have not broken ground yet (normal for new construction pipeline)
- OR they have gone quiet and the relationship needs attention
- Surface this in pipeline analysis by filtering on `search_estimates_by_customer`

### Builder with Many Approved Jobs and No Install Tasks
A builder may approve 8 jobs in a month; if half of them have no install
task scheduled, it means scheduling is behind and crews may be double-booked.
`analyze_job_pipeline` with status_ids=[22] will surface this.

### High AR Balance on a Builder
If a builder has $30,000+ in unpaid invoices, it is a cash flow and
relationship risk. `search_invoices` filtered by customer_id surfaces this.

---

## Geographic Notes

WilliamSmith serves the Charleston, SC metro area. Key areas:
- **Downtown Charleston** — historic homes; often masonry or Isokern
- **West Ashley** — suburban residential mix
- **Mount Pleasant** — high-growth area; lots of new construction
  - **North Mount Pleasant** (Highway 17 corridor, Dunes West, etc.) — farther out; grouping jobs here saves drive time
  - **South Mount Pleasant** (Old Town, Old Village, Shem Creek area) — closer to bridge
- **Daniel Island** — high-end new construction; major builder customer territory
- **James Island** — residential; moderate new construction
- **Johns Island** — growth area; new construction
- **Goose Creek / Summerville** — outer suburban growth; farther drive; schedule grouping matters

**Scheduling note:** Charleston traffic (especially the Ravenel Bridge and I-526)
means job geography matters. When scheduling installs, grouping by area on the
same day saves 1–2 hours of drive time per tech.

---

## Customer Lifetime Value Notes

- A builder who has been active for 3 years at 15 jobs/year × $3,000 average = $135,000/year
- Losing a builder account is a significant revenue event
- Homeowners rarely repeat (one fireplace), but referrals are common
- Service contracts / annual tune-ups are underexploited recurring revenue
