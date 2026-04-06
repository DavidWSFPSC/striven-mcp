# WilliamSmith Fireplaces — Product & Service Categories

## Overview

WilliamSmith sells, installs, and services fireplace and hearth products
in the Charleston, SC area. Understanding what category a job falls into
determines: audit requirements, scheduling needs, and pricing expectations.

---

## Category 1: Gas Fireplace Installation

**Striven Product Type:** Gas Fireplaces / Direct Vent / B-Vent
**Preview Required:** Yes (Residential Remodel, New Construction)
**Install Task Required:** Yes
**Typical Value:** $3,000 – $12,000+

### Sub-types
- **Direct Vent** — sealed combustion, most common; vents horizontally through exterior wall
- **B-Vent / Natural Vent** — vents vertically through chimney; older technology
- **Vent-Free** — no venting required; limited to smaller installations

### Common Products
- Majestic, Heat & Glo, Valor, Kozy Heat, Mendota, Regency

### Audit Triggers
- High-value jobs: verify all line items (surround, mantel, liner, gas line if applicable)

---

## Category 2: Gas Log Installation

**Striven Product Type:** Burner & Gas Logs
**Preview Required:** No (typically an enhancement or replacement)
**Install Task Required:** Yes
**Typical Value:** $500 – $3,500

### Description
Gas logs are installed in existing fireboxes (masonry or prefab).
A complete gas log system consists of:
- Burner assembly
- Log set
- Gas valve and controls
- Remote kit (optional)

### KEY AUDIT RULE
Any gas log job MUST include a **Gas Log Removal Fee** ($200) if replacing
existing logs/burner. This is the most common estimating error.
See `audit_rules.md` Rule 1 for full details.

### Common Products
- Grand Canyon, Rasmussen, Real Fyre, Monessen, Empire

---

## Category 3: Wood-Burning Fireplace Installation

**Striven Product Type:** Wood Burning Fireplaces
**Preview Required:** Yes (Residential Remodel, New Construction)
**Install Task Required:** Yes
**Typical Value:** $4,000 – $20,000+

### Sub-types
- **Isokern** — pre-engineered modular masonry system; WilliamSmith's most common new-construction product
- **Traditional Masonry** — brick/mortar, built by mason
- **Zero-Clearance Wood-Burning Inserts** — factory-built units installed in framed chase

### Isokern Notes
- Isokern is a modular refractory masonry system
- Sizes: 28", 36", 46", 52" (firebox opening width)
- Always includes: firebox modules, lintel, ash dump, damper, hearth, chimney system
- Common order name format: `[Customer] - [Address] - [City] - [Size] Isokern Fireplace - [Room]`
- Example: `Scenic Custom Homes - 244 Brailsford St - Daniel Island - 46" Isokern Fireplace - Family Room`

---

## Category 4: Electric Fireplace Installation

**Striven Product Type:** Electric Fireplaces
**Preview Required:** Yes (if remodel/new construction); No (if replacement)
**Install Task Required:** Yes
**Typical Value:** $800 – $4,000

### Common Products
- Dimplex, Napoleon, Touchstone, Modern Flames

### Notes
- No gas line or venting required — electrical connection only
- Lower install complexity; shorter scheduling window

---

## Category 5: Chimney Repair & Cleaning

**Striven Product Type:** Chimney Repair / Chimney Services
**Preview Required:** No
**Install Task Required:** Yes (service appointment)
**Typical Value:** $150 – $5,000+ (depending on scope)

### Sub-types
- **Chimney Sweep / Cleaning** — annual maintenance, lower value
- **Chimney Liner Installation** — stainless steel flexible or rigid liner; medium-high value
- **Crown Repair / Rebuild** — top of chimney sealing and structural work
- **Firebox Repair** — refractory panel replacement, smoke damage repair
- **Tuckpointing** — mortar joint repair

### Notes
- These jobs do NOT require a preview task — a tech goes directly to assess and perform work
- Often billed as time + materials for unknown-scope repairs

---

## Category 6: Fireplace Enhancements

**Striven Product Type:** Fireplace Enhancements
**Preview Required:** No
**Install Task Required:** Yes
**Typical Value:** $200 – $3,000

### Includes
- Glass door installation or replacement
- Fireplace surround and mantel installation
- Hearth pad installation
- Log lighter / ignition system add-on
- Fireplace blower installation
- Remote control / thermostat upgrade

### Notes
- Enhancement jobs are add-ons to an existing or recently installed fireplace
- No structural work involved
- No preview needed — scope is well-defined at time of estimate

---

## Category 7: Service & Maintenance

**Striven Product Type:** Service / Warranty
**Preview Required:** No
**Install Task Required:** Yes (service appointment)
**Typical Value:** $100 – $1,500

### Includes
- Annual service / tune-up
- Ignition system repair
- Thermocouple / thermopile replacement
- Valve replacement
- Pilot light issues
- Electronic control board repair

### Warranty / Callback Jobs
- Jobs linked to a prior installation where something was not right
- Should be tracked separately from billable service
- Callback rate by product and technician is a key quality metric

---

## Category 8: Outdoor Fireplaces & Fire Features

**Striven Product Type:** Outdoor Fireplaces / Outdoor Fire Features
**Preview Required:** Yes (new construction / remodel)
**Install Task Required:** Yes
**Typical Value:** $3,000 – $30,000+

### Includes
- Outdoor masonry fireplace construction
- Isokern outdoor fireplace kits
- Fire pits (gas or wood)
- Fire bowls and outdoor fire tables
- Outdoor kitchen integration with fire features

---

## How Job Type Appears in Striven

The `Product Type` custom field on each estimate classifies the job type.
This field requires pulling the full estimate detail:
`GET /v1/sales-orders/{id}` — check `customFields` array.

In search results, the job type may also be inferred from:
1. The estimate name (follows `CustomerName - Address - City - Product - Room` convention)
2. Line item names
3. The `Type`, `Category`, or `SalesOrderType` field if populated

---

## Pricing Notes

Exact current pricing lives in the pricing guide (separate document).
General rules:
- Gas log jobs: most common, highest volume, lowest margin per job
- Isokern new construction: highest total value, strongest margins
- Service jobs: time-based, consistent but lower volume per tech
- Chimney repair: variable — always requires scope confirmation before quoting
