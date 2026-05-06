# WSF SOP Cloudflare Worker

Notion proxy for the `/sop` page. The browser never receives `NOTION_TOKEN`.

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Liveness check |
| GET | `/steps` | Read all 21 step overlays from Notion |
| PATCH | `/steps/:stepId` | Update one field on one step (1–21) |

### PATCH body

```json
{ "field": "owner_person", "value": "Jane Smith" }
```

Allowed `field` values: `owner_person`, `backup_person`, `status`, `decision_answer`,
`discussion`, `done_means`, `required_inputs`, `required_outputs`,
`clean_handoff_to`, `friction_risk`, `updated_by`.

Any other field name is rejected with HTTP 400.

---

## Notion Database Setup

Create a database called **WSF SOP Steps** and add these properties:

| Property name | Notion type | Notes |
|---|---|---|
| `Step` | Title | Display name, e.g. "01 — Customer Inquiry" |
| `step_id` | Number | 1–21. **Primary join key — must be exact.** |
| `owner_person` | Rich text | |
| `backup_person` | Rich text | |
| `status` | Select | Options: `Draft` · `Review` · `Ratified` · `Deprecated` |
| `decision_answer` | Rich text | Leadership answer to the open decision |
| `discussion` | Rich text | Team notes / thread |
| `done_means` | Rich text | Editable override of Definition of Done |
| `required_inputs` | Rich text | What must arrive for this step to start |
| `required_outputs` | Rich text | What must leave this step |
| `clean_handoff_to` | Number | Step number receiving the handoff |
| `friction_risk` | Rich text | Known failure modes / blockers |
| `updated_by` | Rich text | Last editor name |

Seed 21 pages — one per step. Set `step_id` to the step number (1–21).
All other properties can start empty; the SOP page shows canonical data until
Notion overlays are populated.

After creating the database, copy the database ID from the Notion URL:
`https://www.notion.so/YOUR_WORKSPACE/<DATABASE_ID>?v=...`

---

## Deployment

### Prerequisites

```
npm install -g wrangler   # Wrangler v3+
wrangler login            # one-time browser auth
```

### wrangler.toml

`wrangler.toml` is already committed alongside this file. It pins the worker
name, entry point, and compatibility date so every `wrangler` command picks
them up automatically — no extra flags needed.

```toml
name = "wsf-sop-worker"
main = "worker.js"
compatibility_date = "2024-01-01"
```

### Steps

```bash
cd cloudflare

# 1. Add secrets — you will be prompted to paste each value
wrangler secret put NOTION_TOKEN
# Paste: the value of NOTION_API_KEY from Render striven-mcp-v2 env vars

wrangler secret put NOTION_DATABASE_ID
# Paste: the database ID from the Notion URL (32-char hex, before ?v=)

# 2. Deploy
wrangler deploy

# 3. Note the Worker URL printed after deploy, e.g.:
#    https://wsf-sop-worker.YOUR_ACCOUNT.workers.dev
```

### Verify — test GET /steps

```bash
curl https://wsf-sop-worker.YOUR_ACCOUNT.workers.dev/steps
```

Expected: a JSON object with a `steps` array of 21 items, each containing
`step_id` (1–21) and empty strings for all editable fields.

```json
{
  "steps": [
    { "notionPageId": "...", "step_id": 1, "owner_person": "", ... },
    ...
  ]
}
```

If you see 21 objects, the Worker and Notion database are wired correctly.

### Wire the Worker URL

After a successful `GET /steps`, set `WORKER_URL` in `static/sop/app.js`:

```js
const WORKER_URL = "https://wsf-sop-worker.YOUR_ACCOUNT.workers.dev";
```

The connection banner will disappear and editable fields will activate.

---

## CORS

Allowed origins are hardcoded in `worker.js`:

```js
const ALLOWED_ORIGINS = [
  "https://www.wsf-hub.com",
  "https://striven-mcp-v2.onrender.com",  // testing only
];
```

Remove the Render URL from the list once production is confirmed.

---

## Token naming

The Worker secret is named `NOTION_TOKEN`.
Its value is the same as `NOTION_API_KEY` on the Render `striven-mcp-v2` service.
These are isolated: Render's env var is not exposed to the Worker, and vice versa.
