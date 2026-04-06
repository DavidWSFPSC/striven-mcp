# WilliamSmith Fireplaces — Role-Aware Response Guide

The same data means different things to different people.
This guide defines who asks what kinds of questions and what they need in a response.

---

## William Smith — CEO / Owner

**What he cares about:**
- Overall business health and revenue
- Red flags and risks (large unpaid invoices, stalled pipeline, losing a builder)
- Whether the business is growing or contracting
- Major operational problems bubbling up

**Response style:**
- Lead with the dollar number or the headline risk
- One-paragraph summary maximum before any detail
- Flag anything that requires his attention
- Skip operational minutiae — he does not need step counts

**Example questions:**
- "How's the pipeline looking?"
- "What's our outstanding AR?"
- "Are there any jobs I should be worried about?"
- "How much did we bill last month?"

---

## Operations / Project Managers

**What they care about:**
- What jobs are approved and not yet scheduled
- Which jobs are stuck and why
- Technician workload and availability
- Timeline from approval to install
- Where the process is breaking

**Response style:**
- Job-level detail; status, dates, what's missing
- Tables with: Estimate #, Customer, Status, Date Approved, Issue
- Grouped by issue type, not by customer
- Actionable: "These 7 jobs need a preview task created today"

**Example questions:**
- "Which approved jobs don't have a preview task?"
- "What's our scheduling backlog?"
- "Which jobs have been approved for more than 2 weeks with no install scheduled?"
- "Where are jobs breaking down in the pipeline?"

---

## Sales Reps

**What they care about:**
- Their own pipeline — what are they responsible for
- Customer status — where is a specific customer's job
- Which of their quotes are still open
- Whether their estimates are complete and correct

**Response style:**
- Filter by sales rep name (available in `search_estimates`)
- Focus on action: "You have 3 quotes sitting in Quoted for 30+ days — time to follow up"
- Flag their own audit issues (missing line items, wrong naming)

**Example questions:**
- "What's the status on the Smith job?"
- "How many of my estimates are still in Quoted?"
- "Did I miss anything on the Scenic Custom Homes estimate?"

---

## Schedulers

**What they care about:**
- Approved jobs that are ready to install but not yet scheduled
- Which jobs have a site preview completed and are ready for install date
- Geographic grouping of jobs (to minimize drive time)
- Technician availability and current task load

**Response style:**
- List of jobs ready to schedule with: Customer, City, Job Type, Approved Date
- Geographic clustering hints ("5 jobs in Mount Pleasant this week — consider batching")
- Technician task counts ("James has 12 open tasks; Chris has 4 — workload imbalance")

**Example questions:**
- "Which jobs are ready to schedule?"
- "What's in the Mount Pleasant area right now?"
- "Which techs are overloaded?"
- "Which jobs have had their preview done but no install date set?"

---

## Accounting / AR (David)

**What he cares about:**
- Unpaid invoices and who owes what
- Missing fees on estimates (revenue leakage)
- Payment history by customer
- Cash collected vs billed in a period

**Response style:**
- Dollar amounts always; sort by balance descending
- Separate overdue from not-yet-due
- Flag specific dollar amounts and customer names
- "Gas log audit" style: exact number of violations and total revenue at risk

**Example questions:**
- "Who owes us money?"
- "What invoices are overdue?"
- "How much did we collect this month?"
- "Which estimates are missing the gas log removal fee?"
- "What's the total outstanding balance across all customers?"

---

## Service Manager

**What they care about:**
- Open service calls and their status
- Which technician is assigned to what
- Callback / warranty jobs (post-install issues)
- Service history for a specific customer or address

**Response style:**
- Task-level detail: who has what, when it's due
- Flag overdue service tasks
- Group by technician
- Distinguish warranty (no charge) from billable service

**Example questions:**
- "What service calls are open right now?"
- "Which service jobs are overdue?"
- "What's the history on the Johnson job on Maybank Hwy?"
- "How many callbacks did we have this month?"

---

## How to Use This Guide

When a user's question maps to a role, tailor the response:
1. Lead with what that role cares about most
2. Use the appropriate level of detail (high-level for CEO; job-level for ops)
3. Surface role-relevant anomalies first
4. Offer a role-relevant follow-up question

If the user's role is not known, default to the Operations/Project Manager style —
it is the most commonly useful format for internal operational questions.
