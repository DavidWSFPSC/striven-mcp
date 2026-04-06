# WilliamSmith Fireplaces — Audit Rules

These are the known estimating and data-quality rules that WilliamSmith audits
against. Every rule has a trigger condition, the correct expected state, and
the consequence of violation.

---

## RULE 1: Gas Log Removal Fee

**Rule ID:** gas_log_removal_fee
**Category:** Estimating — Missing Line Item
**Severity:** High — direct revenue impact ($200/job)

### Trigger
An estimate whose `customFields[Product Type]` equals **"Burner & Gas Logs"**,
OR whose line items contain any of the following keywords:
- "gas log"
- "gas logs"
- "burner"
- "burner log"
- "gas log install"

### Expected State
The estimate MUST contain a line item whose name includes the word **"removal"**.
The canonical item name is: **"Gas Log Removal Fee"**

### Violation
The estimate has gas log / burner products but NO removal fee line item.

### Business Reason
Every gas log installation requires removing the existing logs or burner
assembly. This is billable work ($200 per job). Omitting the fee is an
estimating error — the work still gets done but the customer is not charged.

### How to Audit
1. Search all active or recent estimates
2. For each, fetch full detail (GET /v1/sales-orders/{id}) to access line items and custom fields
3. Check if any line item name contains "gas log", "burner", or "burner log"
4. If yes: verify a line item containing "removal" exists
5. If no removal fee: flag as violation

### Output Format
- Total estimates checked
- Total with gas log / burner products found
- Total missing the removal fee
- Revenue at risk = missing count × $200
- List: Estimate # | Customer | Striven URL

---

## RULE 2: Customer Portal Flag

**Rule ID:** portal_flag
**Category:** Data Quality — Missing Configuration
**Severity:** Medium — customer experience impact

### Trigger
Any estimate (sales order) that is not in Canceled or Completed status.

### Expected State
The Customer Portal flag must be enabled on every active estimate so the
customer can view their quote online.

### Violation
The flag is not set, meaning the customer cannot log in and see their estimate.

### How to Audit
Use the `portal_flag_audit` tool — it scans all estimates and returns those
missing the flag.

### Output Format
- Total estimates checked
- Total missing the portal flag
- List: Estimate # | Customer | Status

---

## RULE 3: Order Naming Convention

**Rule ID:** order_naming
**Category:** Data Quality — Naming Format
**Severity:** Low — operational clarity

### Expected Format
`CustomerName - Address - City - Product/Service - Room`

**Example (correct):**
`Scenic Custom Homes - 244 Brailsford St - Daniel Island - 46" Isokern Fireplace - Family Room`

### Common Violations
- Missing address segment
- Missing room designation
- Missing product/service segment
- All segments present but not dash-separated
- Customer name abbreviated differently from their Striven record

### How to Audit
- Fetch estimates and check estimate name against the expected pattern
- Flag any that are missing two or more segments
- This is a data quality signal, not a billing issue

---

## RULE 4: Approved Estimate — No Preview Task (Remodel / New Construction Only)

**Rule ID:** approved_no_preview
**Category:** Operations — Missing Workflow Step
**Severity:** High — job cannot proceed without site preview

### Trigger
An estimate in **Approved** status whose job type is:
- Residential Remodel
- Residential New Construction
- Commercial Remodel
- Commercial New Construction

### Expected State
A task of type **"Site Inspections/Preview"** (or whose name starts with
"preview", contains "[preview]", "preview-", or "site preview") must exist
and be linked to the estimate, with status **Open** or **On Hold**.

### Exempt Job Types (do NOT flag these)
- Fireplace Enhancements
- Chimney Repair / Chimney work
- Service jobs
- Warranty / Callback jobs

### Violation
Approved remodel/new construction estimate with no qualifying preview task.

### Lateness Threshold
A preview task created MORE THAN 3 DAYS after the estimate approval date is
also flagged — it means scheduling was delayed.

### How to Audit
Use the `analyze_job_pipeline` tool — it handles classification and flags
automatically.

---

## RULE 5: Approved Estimate — No Install Task

**Rule ID:** approved_no_install
**Category:** Operations — Missing Workflow Step
**Severity:** High — unscheduled job

### Trigger
Any estimate in **Approved** or **In Progress** status.

### Expected State
A task whose name or type contains "install" or "installation" must be
linked to the estimate.

### Violation
No install task found — the job has been approved but has not been scheduled.

---

## HOW TO ADD A NEW RULE

When a new audit rule is identified, add a section here following this template:

```
## RULE N: [Rule Name]

**Rule ID:** snake_case_id
**Category:** [Estimating | Data Quality | Operations | Billing]
**Severity:** [High | Medium | Low]

### Trigger
[What condition makes this rule apply to an estimate?]

### Expected State
[What should be true when the estimate is correct?]

### Violation
[What does a failing estimate look like?]

### Business Reason
[Why does this matter in dollar or operational terms?]

### How to Audit
[Step-by-step detection logic]

### Output Format
[What should be reported when violations are found?]
```
