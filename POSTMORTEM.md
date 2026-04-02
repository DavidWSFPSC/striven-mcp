# Deployment Postmortem — Striven API on Render
**Date:** April 2, 2026
**Service:** `striven-mcp-v2` (Flask API, hosted on Render)
**Severity:** High — application failed to start; all endpoints unavailable
**Resolution Time:** ~2 days of iterative debugging

---

## What Happened

The Striven Flask API was deployed to Render but repeatedly failed to initialize.
The `StrivenClient` class raised an `EnvironmentError` on startup, preventing
Gunicorn from serving any requests. All endpoints returned 502 or 503 errors.

---

## Root Cause

**Environment variable name mismatch between the code and the hosting platform.**

The codebase alternated between two naming conventions at different points in
development:

| Convention | Variables |
|---|---|
| Prefixed (old) | `STRIVEN_CLIENT_ID`, `STRIVEN_CLIENT_SECRET` |
| Unprefixed (new) | `CLIENT_ID`, `CLIENT_SECRET` |

Render was configured with `CLIENT_ID` and `CLIENT_SECRET`.
Some code paths still called `os.getenv("STRIVEN_CLIENT_ID")`, which returned
`None`. The application interpreted `None` as missing credentials and aborted.

Logs showed *"credentials missing"* — which was technically true from the
application's perspective — but masked the real problem: the variable names
did not match. The credentials existed in Render; the code simply wasn't
asking for them by the right name.

---

## Contributing Factors

1. **Naming drift during development.** Variable names were renamed mid-project
   without a single pass to update every reference.

2. **No startup environment assertion.** The app printed a startup message but
   did not log the raw value (or `None` state) of each variable before failing.
   This delayed diagnosis by hours.

3. **Multiple active services caused confusion.** Two Render services existed
   simultaneously (`striven-mcp` and `striven-mcp-v2`). Logs from the wrong
   service were checked at least once, sending debugging in the wrong direction.

4. **`render.yaml` was out of sync.** The blueprint file still declared the old
   `STRIVEN_*` key names, so any re-deployment from the blueprint would have
   re-introduced the bug.

---

## Timeline

| Step | Finding |
|---|---|
| Initial deploy | App crashes on startup — "credentials missing" |
| Added `bool()` debug prints | Confirmed `client_id present: False` |
| Dumped all `os.environ.keys()` | Confirmed `CLIENT_ID` exists in environment |
| Grepped codebase for `STRIVEN_CLIENT` | Found stale references in `app.py`, `render.yaml` |
| Removed all `STRIVEN_*` references | Standardized to `CLIENT_ID` / `CLIENT_SECRET` |
| Redeployed | App initialised successfully |

---

## Fix Applied

1. Standardized all environment variable names to:
   - `CLIENT_ID`
   - `CLIENT_SECRET`
   - `BASE_URL`
   - `TOKEN_URL`

2. Removed every `STRIVEN_CLIENT_ID` / `STRIVEN_CLIENT_SECRET` reference from:
   - `services/striven.py`
   - `app.py`
   - `render.yaml`

3. Re-entered environment variables cleanly in the Render dashboard.

4. Added explicit debug logging that prints `repr()` of each variable
   before the guard check, so `None` vs `""` vs a real value is instantly
   visible in any future incident.

---

## What We Learned

> **The application reading an environment variable and the hosting platform
> providing it are two separate, independently verifiable facts.**
> Logs must confirm both — not just one.

A variable named `CLIENT_ID` in Render is invisible to code that calls
`os.getenv("STRIVEN_CLIENT_ID")`. No error is raised. Python silently returns
`None`. This class of bug produces no stack trace at the point of mismatch,
only a downstream failure that looks like a missing credential.

---

---

## Deployment Checklist

Use this checklist before every production deploy.

### Pre-Deploy

- [ ] All environment variables used in code are listed in `render.yaml`
- [ ] Every `os.getenv("KEY")` call in the codebase matches a key in `render.yaml` **exactly** (spelling, case, no prefix drift)
- [ ] No old variable names remain anywhere (run: `grep -r "STRIVEN_CLIENT" .`)
- [ ] Startup block logs the `bool()` state of every required credential
- [ ] `requirements.txt` is up to date
- [ ] Code has been pushed to the correct GitHub branch that Render watches

### Render Dashboard

- [ ] Correct service is open (`striven-mcp-v2`, not `striven-mcp`)
- [ ] All required environment variables are present and non-empty
- [ ] Values have been re-verified after any rename (delete old key, add new key)
- [ ] Auto-deploy is enabled for the correct branch, or a manual deploy was triggered

### Post-Deploy Verification

- [ ] Render logs show no startup errors
- [ ] Logs confirm: `[StrivenClient] Initialised — client_id=XXXXXX...`
- [ ] `GET /health` returns `{"status": "ok"}`
- [ ] `GET /search-estimates?pageSize=5` returns real estimate records
- [ ] MCP tool `api_health` responds correctly from Claude

---

---

## Rules for Environment Variables

These rules apply to every service in this project.

**1. One name, used everywhere.**
Pick a name once. Use it identically in the code, `render.yaml`, and the
hosting dashboard. Never abbreviate, prefix, or rename it in one place only.

**2. The code is the source of truth for names.**
If you rename a variable in code, update `render.yaml` and the hosting
dashboard in the same commit. These three must always be in sync.

**3. Never use prefixes inconsistently.**
Either all variables are prefixed (`STRIVEN_CLIENT_ID`) or none are
(`CLIENT_ID`). Mixing conventions across files guarantees a mismatch.

**4. Log raw values at startup — before any guard.**
Always print `repr(os.getenv("KEY"))` before the `if not value: raise` check.
This makes `None`, `""`, and `" "` (accidental space) immediately distinguishable.

**5. Treat a renamed variable as a new variable.**
When renaming: add the new key to the dashboard, verify the app starts, then
delete the old key. Never assume a rename is live until the deploy log confirms it.

**6. One service per purpose.**
Do not allow duplicate Render services for the same codebase to accumulate.
Stale services produce stale logs and cause confusion under pressure.
Delete unused services immediately.
