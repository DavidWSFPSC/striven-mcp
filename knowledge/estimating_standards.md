# WilliamSmith Fireplaces — Estimating Standards

What makes a complete, correct estimate for each job type.
Use this to audit estimates for completeness or flag missing items.

---

## UNIVERSAL REQUIREMENTS (Every Estimate)

These apply to ALL job types without exception:

| Item | Check |
|------|-------|
| Order name follows convention | CustomerName - Address - City - Product - Room |
| Customer Portal flag enabled | Customer can view estimate online |
| Sales rep assigned | SalesRep field populated |
| At least one line item | Estimate is not empty |
| OrderTotal > $0 | No $0-value estimates for billable work |

---

## Gas Log Installation (Burner & Gas Logs)

**Typical complete estimate includes:**

| Line Item | Required | Notes |
|-----------|----------|-------|
| Gas Log Set | Yes | Specify brand and model |
| Burner Assembly | Yes (if replacing) | Natural gas or LP |
| Gas Log Removal Fee | **YES — ALWAYS** | $200 — removing existing logs/burner |
| Gas Line Extension | If applicable | If existing gas line needs extending |
| Gas Valve / Key Valve | If applicable | If valve is being replaced |
| Labor / Installation | Yes | Should be itemized |

**Most common error:** Missing Gas Log Removal Fee.
Any estimate with a gas log or burner product that does NOT include a
"removal" line item is flagged as a violation. See `audit_rules.md` Rule 1.

**Preview required:** No
**Install task required:** Yes

---

## Gas Fireplace Installation (New)

**Typical complete estimate includes:**

| Line Item | Required | Notes |
|-----------|----------|-------|
| Fireplace Unit | Yes | Brand, model, BTU, vent type |
| Venting System | Yes | Liner, termination cap, elbows |
| Gas Line | If applicable | New run to firebox location |
| Mantel / Surround | If customer wants one | May be separate PO |
| Hearth Pad | If required by code | |
| Logs / Media | If included | Some units come with logs |
| Remote / Controls | If included | |
| Labor / Installation | Yes | |
| Permit | If applicable | Required in some jurisdictions |

**Preview required:** Yes (remodel/new construction)
**Install task required:** Yes

---

## Isokern / Wood-Burning Fireplace (New Construction)

**Typical complete estimate includes:**

| Line Item | Required | Notes |
|-----------|----------|-------|
| Isokern Firebox Modules | Yes | Correct size (28", 36", 46", 52") |
| Isokern Lintel | Yes | Spans firebox opening |
| Ash Dump | Yes | Standard Isokern component |
| Damper | Yes | Throat damper or top-sealing |
| Hearth Base / Slab | Yes | Per local code |
| Chimney System | Yes | Flue liner, surround, chase cover |
| Spark Arrestor / Cap | Yes | Top of chimney |
| Labor / Installation | Yes | |
| Mantel / Surround | If customer wants | May be separate line or PO |

**Preview required:** Yes (new construction)
**Install task required:** Yes

**Common errors:**
- Missing chimney system (just quoting the firebox)
- Missing spark arrestor
- Wrong firebox size specified (measure first)

---

## Chimney Repair

**Estimate contents vary by scope. Common items:**

| Line Item | When Applicable |
|-----------|----------------|
| Chimney Sweep / Cleaning | If included with repair |
| Crown Repair / Rebuild | If crown is cracked or failing |
| Tuckpointing | If mortar joints need repair |
| Firebox Refractory Panel Replacement | If panels are cracked |
| Chimney Liner Replacement | If liner is damaged |
| Cap / Chase Cover Replacement | If cap is rusted or missing |
| Waterproofing Treatment | Recommended on older chimneys |
| Labor | Yes |

**Preview required:** No — tech assesses on-site
**Install task required:** Yes (service appointment)

**Note:** Chimney repair is often quoted after a site assessment.
Initial quote may be range-based; finalized after inspection.
Flag any chimney repair estimate with OrderTotal = $0 or missing labor.

---

## Fireplace Enhancement

**Typical items (varies widely by enhancement type):**

| Enhancement Type | Expected Line Items |
|-----------------|-------------------|
| Glass Door Installation | Door unit, labor, gasket/seal if needed |
| Mantel Installation | Mantel unit, hardware, labor |
| Hearth Pad | Pad material, labor |
| Blower Installation | Blower kit, labor |
| Remote / Thermostat Upgrade | Control kit, labor |
| Log Lighter Addition | Lighter kit, gas line, labor |
| Surround / Tile Work | Materials, labor |

**Preview required:** No
**Install task required:** Yes

---

## Service / Annual Tune-Up

**Standard service estimate includes:**

| Line Item | Notes |
|-----------|-------|
| Service Call / Diagnostic | Flat rate or hourly |
| Parts (if known) | Thermocouple, valve, board, etc. |
| Labor | If billed separately from service call |

**Note:** Parts may not be known at quote time — service estimates often
have a diagnostic charge plus "TBD" or range for parts.
Flag service estimates with no service call line item.

**Preview required:** No
**Install task required:** Yes (service appointment)

---

## Electric Fireplace Installation

**Typical complete estimate:**

| Line Item | Required | Notes |
|-----------|----------|-------|
| Electric Fireplace Unit | Yes | Brand, model, size |
| Electrical Connection | If applicable | If new circuit needed |
| Mantel / Surround | If applicable | |
| Labor / Installation | Yes | |

**Preview required:** Only if remodel (wall opening required)
**Install task required:** Yes

---

## HOW TO AUDIT AN ESTIMATE FOR COMPLETENESS

Steps when asked "Does estimate #N include everything it should?":

1. Call `get_estimate_by_id` to get the full estimate with line items and custom fields
2. Read `customFields[Product Type]` to determine job type
3. Match job type to the relevant section above
4. Check that each "Required" line item is present
5. Run the gas log removal fee check if Product Type = "Burner & Gas Logs"
6. Check universal requirements (naming, portal flag, sales rep, total > $0)
7. Report: what's present, what's missing, severity of each gap

---

## COMMON ESTIMATING ERRORS (ALL JOB TYPES)

| Error | Frequency | Impact |
|-------|-----------|--------|
| Missing Gas Log Removal Fee | High | $200/job revenue loss |
| Missing portal flag | Medium | Customer can't see estimate |
| $0 line items (unbilled labor) | Low-Medium | Revenue loss |
| Wrong naming convention | Medium | Searchability, professionalism |
| Missing sales rep assignment | Low | Pipeline reporting gap |
| Estimate total not matching line item sum | Low | Billing error |
