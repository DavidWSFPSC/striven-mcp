/**
 * WSF SOP Worker — Notion proxy for the /sop page
 *
 * Secrets (set via `wrangler secret put`):
 *   NOTION_TOKEN        — same value as NOTION_API_KEY on Render
 *   NOTION_DATABASE_ID  — the WSF SOP Steps database ID (from Notion URL)
 *
 * Endpoints:
 *   GET  /steps            — read all 21 step overlays from Notion
 *   PATCH /steps/:stepId   — update one allowed field on one step
 *   GET  /health           — liveness check
 *
 * CORS:
 *   Allowed origins: https://www.wsf-hub.com
 *                    https://striven-mcp-v2.onrender.com  (testing only)
 */

const ALLOWED_ORIGINS = [
  "https://www.wsf-hub.com",
  "https://striven-mcp-v2.onrender.com",
];

// Strict allowlist — only these field names may be written to Notion.
// Any PATCH body containing a key not in this set is rejected 400.
const EDITABLE_FIELDS = new Set([
  "owner_person",
  "backup_person",
  "status",
  "decision_answer",
  "discussion",
  "done_means",
  "required_inputs",
  "required_outputs",
  "clean_handoff_to",
  "friction_risk",
  "updated_by",
]);

// Map from our field names → Notion property names in the database.
// Keep in sync with the database schema in cloudflare/README.md.
const FIELD_TO_NOTION = {
  owner_person:    "owner_person",
  backup_person:   "backup_person",
  status:          "status",
  decision_answer: "decision_answer",
  discussion:      "discussion",
  done_means:      "done_means",
  required_inputs: "required_inputs",
  required_outputs:"required_outputs",
  clean_handoff_to:"clean_handoff_to",
  friction_risk:   "friction_risk",
  updated_by:      "updated_by",
};

const NOTION_API = "https://api.notion.com/v1";
const NOTION_VERSION = "2022-06-28";

// ── helpers ──────────────────────────────────────────────────────────────────

function corsHeaders(origin) {
  const allowed = ALLOWED_ORIGINS.includes(origin) ? origin : ALLOWED_ORIGINS[0];
  return {
    "Access-Control-Allow-Origin":  allowed,
    "Access-Control-Allow-Methods": "GET, PATCH, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Max-Age":       "86400",
  };
}

function json(data, status = 200, origin = "") {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...corsHeaders(origin),
    },
  });
}

function notionHeaders(token) {
  return {
    "Authorization":  `Bearer ${token}`,
    "Notion-Version": NOTION_VERSION,
    "Content-Type":   "application/json",
  };
}

// Extract plain text from a Notion rich_text property value array.
function richText(prop) {
  if (!prop || !prop.rich_text) return "";
  return prop.rich_text.map(t => t.plain_text).join("");
}

// Extract select value.
function select(prop) {
  if (!prop || !prop.select) return "";
  return prop.select.name || "";
}

// Extract number.
function number(prop) {
  if (!prop || prop.number == null) return null;
  return prop.number;
}

// Build a Notion property patch object for one field + value.
function buildNotionPatch(field, value) {
  switch (field) {
    case "status":
      return { [FIELD_TO_NOTION[field]]: { select: value ? { name: value } : null } };
    case "clean_handoff_to":
      return { [FIELD_TO_NOTION[field]]: { number: value === "" ? null : Number(value) } };
    default:
      return {
        [FIELD_TO_NOTION[field]]: {
          rich_text: [{ type: "text", text: { content: String(value ?? "") } }],
        },
      };
  }
}

// Map a Notion page → a step overlay object.
function pageToOverlay(page) {
  const p = page.properties;
  return {
    notionPageId:    page.id,
    step_id:         number(p.step_id),
    owner_person:    richText(p.owner_person),
    backup_person:   richText(p.backup_person),
    status:          select(p.status),
    decision_answer: richText(p.decision_answer),
    discussion:      richText(p.discussion),
    done_means:      richText(p.done_means),
    required_inputs: richText(p.required_inputs),
    required_outputs:richText(p.required_outputs),
    clean_handoff_to:number(p.clean_handoff_to),
    friction_risk:   richText(p.friction_risk),
    updated_by:      richText(p.updated_by),
  };
}

// ── route handlers ────────────────────────────────────────────────────────────

async function handleGetSteps(env, origin) {
  // Query all pages in the SOP database, sorted by step_id.
  // Notion returns max 100 per request; paginate if needed (21 steps → one page).
  const results = [];
  let cursor = undefined;

  do {
    const body = {
      sorts: [{ property: "step_id", direction: "ascending" }],
      page_size: 100,
    };
    if (cursor) body.start_cursor = cursor;

    const res = await fetch(`${NOTION_API}/databases/${env.NOTION_DATABASE_ID}/query`, {
      method: "POST",
      headers: notionHeaders(env.NOTION_TOKEN),
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const err = await res.text();
      return json({ error: "Notion query failed", detail: err }, 502, origin);
    }

    const data = await res.json();
    results.push(...(data.results || []).map(pageToOverlay));
    cursor = data.has_more ? data.next_cursor : undefined;
  } while (cursor);

  return json({ steps: results }, 200, origin);
}

async function handlePatchStep(stepId, request, env, origin) {
  const stepNum = parseInt(stepId, 10);
  if (!stepNum || stepNum < 1 || stepNum > 21) {
    return json({ error: "Invalid step id. Must be 1–21." }, 400, origin);
  }

  let body;
  try {
    body = await request.json();
  } catch {
    return json({ error: "Invalid JSON body." }, 400, origin);
  }

  const { field, value } = body;

  if (!field || !EDITABLE_FIELDS.has(field)) {
    return json({
      error: `Field '${field}' is not editable. Allowed: ${[...EDITABLE_FIELDS].join(", ")}`,
    }, 400, origin);
  }

  // Find the Notion page for this step by querying the database filtered by step_id.
  const queryRes = await fetch(`${NOTION_API}/databases/${env.NOTION_DATABASE_ID}/query`, {
    method: "POST",
    headers: notionHeaders(env.NOTION_TOKEN),
    body: JSON.stringify({
      filter: { property: "step_id", number: { equals: stepNum } },
      page_size: 1,
    }),
  });

  if (!queryRes.ok) {
    const err = await queryRes.text();
    return json({ error: "Notion lookup failed", detail: err }, 502, origin);
  }

  const queryData = await queryRes.json();
  const page = queryData.results?.[0];

  if (!page) {
    return json({ error: `No Notion page found for step_id ${stepNum}.` }, 404, origin);
  }

  // Patch the property.
  const patch = buildNotionPatch(field, value);
  const patchRes = await fetch(`${NOTION_API}/pages/${page.id}`, {
    method: "PATCH",
    headers: notionHeaders(env.NOTION_TOKEN),
    body: JSON.stringify({ properties: patch }),
  });

  if (!patchRes.ok) {
    const err = await patchRes.text();
    return json({ error: "Notion update failed", detail: err }, 502, origin);
  }

  const updated = await patchRes.json();
  return json({ ok: true, overlay: pageToOverlay(updated) }, 200, origin);
}

// ── main fetch handler ────────────────────────────────────────────────────────

export default {
  async fetch(request, env) {
    const url    = new URL(request.url);
    const origin = request.headers.get("Origin") || "";
    const method = request.method.toUpperCase();

    // Preflight
    if (method === "OPTIONS") {
      return new Response(null, { status: 204, headers: corsHeaders(origin) });
    }

    // Health check
    if (url.pathname === "/health" && method === "GET") {
      return json({ ok: true }, 200, origin);
    }

    // GET /steps
    if (url.pathname === "/steps" && method === "GET") {
      return handleGetSteps(env, origin);
    }

    // PATCH /steps/:stepId
    const patchMatch = url.pathname.match(/^\/steps\/(\d+)$/);
    if (patchMatch && method === "PATCH") {
      return handlePatchStep(patchMatch[1], request, env, origin);
    }

    return json({ error: "Not found." }, 404, origin);
  },
};
