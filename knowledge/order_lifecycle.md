# WilliamSmith Fireplaces — Sales Order Lifecycle

## Status Flow

Every sales order (estimate) moves through these statuses in Striven:

```
Incomplete (18) → Quoted (19) → Pending Approval (20) → Approved (22) → In Progress (25) → Completed (27)
                                                                        ↘ Canceled
```

### Status Definitions

| ID | Name | Meaning |
|----|------|---------|
| 18 | Incomplete | Draft — estimate is being built, not yet sent to customer |
| 19 | Quoted | Sent to customer, awaiting their decision |
| 20 | Pending Approval | Customer has expressed intent; internal approval needed |
| 22 | Approved | Customer accepted; job is ready to schedule |
| 25 | In Progress | Job is actively scheduled or underway |
| 27 | Completed | Job finished, ready to invoice |
| — | Canceled | Job will not proceed |

---

## What Should Happen at Each Stage

### Incomplete → Quoted
- Estimate is built in Striven
- All required line items added (check audit rules)
- Customer Portal flag enabled
- Order name follows naming convention
- Sent to customer for review

### Quoted → Approved
- Customer accepts the estimate
- Status changed to Approved in Striven
- For remodel/new construction: preview task created within 3 days

### Approved → In Progress
- Site preview completed (if required)
- Install scheduled — an install task created with a due date
- Materials ordered if needed (purchase order created)
- Customer notified of install date

### In Progress → Completed
- Installation completed on site
- Job marked complete in Striven
- Invoice generated

---

## "Stuck" Job Definitions

A job is considered "stuck" when it has not moved from its current status
within the expected timeframe:

| Status | Stuck Threshold | Likely Cause |
|--------|----------------|--------------|
| Incomplete | > 7 days | Estimate not finished; rep has open work |
| Quoted | > 30 days | Customer hasn't responded; follow-up needed |
| Pending Approval | > 7 days | Internal bottleneck |
| Approved | > 14 days | Preview not scheduled or install not booked |
| In Progress | > 60 days | Job stalled on site; supply issue or coordination failure |

---

## Approved Jobs — Required Next Steps

When a job is approved, the following must happen in sequence:

1. **Preview task created** (within 3 days of approval)
   - Required for: Residential Remodel, Residential New Construction,
     Commercial Remodel, Commercial New Construction
   - Not required for: Enhancements, Chimney Repair, Service, Warranty

2. **Preview completed**
   - Tech visits site, confirms measurements and scope
   - Any scope changes trigger an estimate revision before install

3. **Install task created** (with scheduled due date)
   - All jobs regardless of type require an install task once approved

4. **Materials confirmed**
   - Product/fireplace ordered if not in stock

5. **Customer notified of install date**

---

## Builder vs Homeowner Jobs

Builder (contractor) jobs often have unique patterns:
- Higher volume (one builder may have 10–30 open orders simultaneously)
- Longer timelines (new construction takes months, not weeks)
- "Stuck in Quoted" is more common because builder hasn't broken ground yet
- Pipeline view for a specific builder: use `search_estimates_by_customer`

Homeowner jobs move faster but have higher service/warranty callback rates.

---

## Invoice Lifecycle

After job completion:
1. Invoice generated in Striven from the approved estimate
2. Sent to customer (email or portal)
3. Payment received → payment recorded in Striven
4. Job fully closed

For builder customers, invoicing often happens in batches or on net-30 terms.

---

## Key Dates on Every Order

| Field | Meaning |
|-------|---------|
| DateCreated | When the estimate was first entered in Striven |
| DateApproved | When customer accepted / status moved to Approved |
| DueDate | Target completion date (not always set) |

The gap between DateCreated and DateApproved is the **sales cycle length**.
The gap between DateApproved and install task due date is the **scheduling lag**.
