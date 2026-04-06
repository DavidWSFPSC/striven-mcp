# WilliamSmith Fireplaces — Order Naming Convention

## Standard Format

```
CustomerName - Address - City - Product/Service - Room
```

All five segments, separated by ` - ` (space-dash-space).

---

## Examples

**Correct:**
```
Scenic Custom Homes - 244 Brailsford St - Daniel Island - 46" Isokern Fireplace - Family Room
Smith - 1423 Maybank Hwy - Johns Island - Gas Log Install - Living Room
Artisan Custom Homes - 85 Darrell Creek Trail - Mount Pleasant - 36" Isokern Fireplace - Master Bedroom
Johnson - 312 Folly Rd - James Island - Chimney Repair - N/A
```

**Incorrect (and why):**
```
Scenic Custom Homes 244 Brailsford Daniel Island Isokern
→ Missing dashes; no room; address incomplete

Smith - Johns Island - Gas Logs
→ Missing street address; missing room

Gas Log Install - Living Room
→ Missing customer name and address entirely

Artisan - Mt Pleasant - 36" Isokern
→ Abbreviations; missing address; missing room
```

---

## Segment Definitions

### 1. CustomerName
- Use the customer's full name exactly as it appears in Striven
- For builders: use the company name ("Scenic Custom Homes" not "Scenic")
- For homeowners: use "LastName" or "FirstName LastName" — be consistent

### 2. Address
- Street number and street name
- Do not include city here (city is the next segment)
- Abbreviations are acceptable: "St", "Rd", "Dr", "Ln", "Blvd", "Hwy"
- Apartment/unit if applicable: "312 Folly Rd Unit 4B"

### 3. City
- Full city name or common abbreviation
- Examples: Daniel Island, Mount Pleasant, Charleston, James Island,
  Johns Island, West Ashley, Goose Creek, Summerville, Sullivan's Island

### 4. Product/Service
- What is being done
- Examples: "46" Isokern Fireplace", "Gas Log Install", "Chimney Repair",
  "Fireplace Enhancement", "Annual Service", "Electric Fireplace Install"
- For specific products, include model or size if known

### 5. Room
- Where in the house
- Examples: Family Room, Living Room, Master Bedroom, Bonus Room,
  Outdoor Patio, Great Room, Basement
- Use "N/A" if not applicable (e.g. chimney work applies to whole house)

---

## Why This Matters

1. **Searchability** — correct naming makes it easy to find all jobs at one address
2. **Scheduling** — address in the name lets schedulers group jobs geographically
3. **Auditing** — deviations are a data quality signal that something was entered in a hurry
4. **Customer communication** — portal-facing name should be professional

---

## How to Audit Names

When reviewing estimate names for quality:
1. Split by " - "
2. Expect at least 4 segments (customer, address, city, product)
3. Flag if fewer than 4 segments
4. Flag if city is missing (common error: address and city run together)
5. Flag if address is missing entirely

Severity: Low — does not affect billing, but affects searchability and professionalism.
