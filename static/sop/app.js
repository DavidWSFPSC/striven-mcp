// WSF SOP Framework — app.js
// Step 3B-2: Wired to Cloudflare Worker with live Notion persistence

const WORKER_URL = "https://wsf-sop-worker.david-warren.workers.dev";

const { useState, useMemo, useEffect, useRef, useCallback } = React;
const { TweaksPanel, useTweaks, TweakSection, TweakRadio, TweakToggle, TweakSelect } = window;

const TWEAK_DEFAULTS = /*EDITMODE-BEGIN*/{
  "view": "timeline",
  "density": "comfortable",
  "showSystems": true,
  "showGuardrails": true,
  "showSubsteps": true,
  "filterRole": "all",
  "filterPhase": "all"
}/*EDITMODE-END*/;

// ---------- notion data hook ----------
function useNotionData() {
  const [overlays, setOverlays] = useState({});
  const [loadState, setLoadState] = useState(WORKER_URL ? "loading" : "disabled");
  const [loadError, setLoadError] = useState("");
  const [saveStates, setSaveStates] = useState({});

  useEffect(() => {
    if (!WORKER_URL) return;
    fetch(`${WORKER_URL}/steps`)
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => {
        const map = {};
        (data.steps || []).forEach(s => { map[s.step_id] = s; });
        setOverlays(map);
        setLoadState("loaded");
      })
      .catch(err => {
        setLoadState("error");
        setLoadError(err.message);
      });
  }, []);

  const saveField = useCallback((stepId, field, value) => {
    const key = `${stepId}-${field}`;
    setSaveStates(prev => ({ ...prev, [key]: "saving" }));
    fetch(`${WORKER_URL}/steps/${stepId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ field, value }),
    })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => {
        if (data.overlay) {
          setOverlays(prev => ({ ...prev, [stepId]: data.overlay }));
        }
        setSaveStates(prev => ({ ...prev, [key]: "saved" }));
        setTimeout(() => setSaveStates(prev => {
          const next = { ...prev };
          if (next[key] === "saved") next[key] = "idle";
          return next;
        }), 2500);
      })
      .catch(() => {
        setSaveStates(prev => ({ ...prev, [key]: "error" }));
      });
  }, []);

  const appendField = useCallback((stepId, field, value) => {
    const key = `${stepId}-${field}`;
    setSaveStates(prev => ({ ...prev, [key]: "saving" }));
    fetch(`${WORKER_URL}/steps/${stepId}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ field, value, append: true }),
    })
      .then(r => { if (!r.ok) throw new Error(`HTTP ${r.status}`); return r.json(); })
      .then(data => {
        if (data.overlay) {
          setOverlays(prev => ({ ...prev, [stepId]: data.overlay }));
        }
        setSaveStates(prev => ({ ...prev, [key]: "saved" }));
        setTimeout(() => setSaveStates(prev => {
          const next = { ...prev };
          if (next[key] === "saved") next[key] = "idle";
          return next;
        }), 2500);
      })
      .catch(() => {
        setSaveStates(prev => ({ ...prev, [key]: "error" }));
      });
  }, []);

  const getSaveState = useCallback(
    (stepId, field) => saveStates[`${stepId}-${field}`] || "idle",
    [saveStates]
  );

  return { overlays, loadState, loadError, saveField, appendField, getSaveState };
}

// ---------- connection banner ----------
function ConnectionBanner({ loadState, loadError }) {
  if (loadState === "loaded") return null;
  if (loadState === "disabled") {
    return (
      <div className="connection-banner">
        Collaboration save is not connected — viewing local SOP framework only.
        Set <code>WORKER_URL</code> in app.js to enable live Notion persistence.
      </div>
    );
  }
  if (loadState === "loading") {
    return <div className="connection-banner loading">Loading Notion overlays…</div>;
  }
  return (
    <div className="connection-banner error">
      Notion overlay failed to load ({loadError}) — fields are read-only.
    </div>
  );
}

// ---------- save badge (legacy, kept for callers) ----------
function SaveBadge({ state }) {
  if (state === "saving") return <span className="save-badge saving">saving…</span>;
  if (state === "saved")  return <span className="save-badge saved">saved ✓</span>;
  if (state === "error")  return <span className="save-badge error">error</span>;
  return <span className="save-badge" />;
}

// ---------- save state DOT (new — anchors to field, no text) ----------
function SaveDot({ state }) {
  return <span className={`savestate-dot ${state || "idle"}`} aria-hidden="true" />;
}

// ---------- battery atom (radial) ----------
function BatteryRadial({ pct = 0, size = "md", showPct = true }) {
  const p = Math.max(0, Math.min(100, Math.round(pct)));
  const r = 9;
  const c = 2 * Math.PI * r;
  const offset = c * (1 - p / 100);
  const klass = p >= 100 ? "full" : p === 0 ? "empty" : "";
  return (
    <span className={`battery-radial ${size} ${klass}`} style={{ "--pct": p }} title={`${p}%`}>
      <svg viewBox="0 0 24 24">
        <circle className="track" cx="12" cy="12" r={r} strokeWidth="2.5" />
        <circle className="fill"  cx="12" cy="12" r={r} strokeWidth="2.5"
                strokeDasharray={c} strokeDashoffset={offset} />
      </svg>
      {showPct && <span className="pct">{p}</span>}
    </span>
  );
}

// ---------- estimate stage lookup (demo scaffold) ----------
async function lookupEstimateStage(estimateNumber) {
  // Demo mapping by last digit. Replace this body with a live Striven/Supabase call.
  const numeric = parseInt(String(estimateNumber).replace(/\D/g, ""), 10);
  if (!numeric) return null;
  const DEMO_MAP = { 1: 6, 2: 8, 3: 10, 4: 13, 5: 16, 6: 17 };
  return DEMO_MAP[numeric % 10] ?? 9;
}

function highlightStep(n, { setView, setExpanded, stepRefs }) {
  setView("timeline");
  setExpanded(prev => { const s = new Set(prev); s.add(n); return s; });
  setTimeout(() => {
    const el = (stepRefs && stepRefs.current && stepRefs.current[n])
      || document.querySelector(`[data-step="${n}"]`);
    if (el) el.scrollIntoView({ block: "center", behavior: "smooth" });
  }, 50);
}

// ---------- estimate lookup bar ----------
function EstimateLookup({ value, onChange, onClear }) {
  const [draft, setDraft] = useState("");
  useEffect(() => { if (!value) setDraft(""); }, [value]);
  const submit = () => {
    const v = draft.trim();
    if (v) onChange(v);
  };
  if (value) {
    return (
      <div className="lookup-row">
        <div className="lookup-chip">
          <span className="est-label">EST</span>
          <span>{value}</span>
          <button className="lookup-chip-clear" onClick={onClear} aria-label="Clear estimate">✕</button>
        </div>
        <span className="lookup-hint">Demo lookup — live Striven stage connection pending</span>
      </div>
    );
  }
  return (
    <div className="lookup-row">
      <span style={{ fontSize: "0.72rem", fontWeight: 600, letterSpacing: "0.06em", textTransform: "uppercase", color: "var(--ink-3)", whiteSpace: "nowrap", flexShrink: 0 }}>Find an estimate</span>
      <div className="lookup-bar">
        <button className="lookup-scan" title="Scan barcode" onClick={() => alert("Barcode scan — coming soon")}>
          <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.6">
            <path d="M3 7V5a2 2 0 0 1 2-2h2M21 7V5a2 2 0 0 0-2-2h-2M3 17v2a2 2 0 0 0 2 2h2M21 17v2a2 2 0 0 1-2 2h-2" />
            <line x1="7" y1="8" x2="7" y2="16" />
            <line x1="10" y1="8" x2="10" y2="16" />
            <line x1="13" y1="8" x2="13" y2="16" />
            <line x1="16" y1="8" x2="16" y2="16" />
          </svg>
        </button>
        <span className="lookup-prefix">EST</span>
        <input
          className="lookup-input"
          type="text"
          inputMode="numeric"
          placeholder="Type estimate number"
          value={draft}
          onChange={e => setDraft(e.target.value)}
          onKeyDown={e => { if (e.key === "Enter") submit(); }}
        />
        <button className="lookup-submit" onClick={submit} disabled={!draft.trim()}>Find</button>
      </div>
      <span className="lookup-hint">Demo lookup — live Striven stage connection pending</span>
    </div>
  );
}

// ---------- live status drill-down (visible inside expanded live-step card) ----------
function LiveStepStatus({ step, estimate }) {
  // Demo data — Pass D will wire Striven
  const checklist = step.proof.map((p, i) => ({ text: p, done: i === 0 }));
  const next = step.handoff ? STEPS.find(s => s.n === step.handoff.to) : null;
  return (
    <div className="live-status">
      <div className="live-status-head">
        <span className="live-status-title">Current step — live status</span>
        <span className="live-status-est">EST {estimate}</span>
      </div>
      <div className="live-status-grid">
        <div className="live-status-block">
          <span className="live-status-label">Owner now</span>
          <span className="live-status-value">
            <strong>{step.owner.person || "—"}</strong> · {ROLES[step.owner.role]?.label}
          </span>
        </div>
        <div className="live-status-block">
          <span className="live-status-label">SLA</span>
          <span className="live-status-value">{step.sla}</span>
        </div>
        <div className="live-checklist">
          <span className="live-status-label">Definition of Done</span>
          {checklist.map((c, i) => (
            <label key={i} className={`live-check-item ${c.done ? "done" : ""}`}>
              <span className="live-check-box">
                {c.done && (
                  <svg width="10" height="10" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M2 6l3 3 5-6" />
                  </svg>
                )}
              </span>
              <span className="live-check-text">{c.text}</span>
            </label>
          ))}
        </div>
        {next && (
          <div className="live-status-block" style={{ gridColumn: "1 / -1" }}>
            <span className="live-status-label">Hands off to</span>
            <span className="live-status-value">
              Step {String(next.n).padStart(2, "0")} · <strong>{next.title}</strong> — {next.owner.person} ({ROLES[next.owner.role]?.label})
            </span>
          </div>
        )}
      </div>
      <div className="live-actions">
        <button className="live-btn primary">Mark step complete</button>
        <button className="live-btn">Reassign</button>
        <button className="live-btn">Add note</button>
      </div>
    </div>
  );
}

// ---------- editable field ----------
function EditableField({ stepId, field, notionValue, placeholder, multiline, isSelect, isNumber, saveField, getSaveState, rows = 3 }) {
  const saveState = getSaveState(stepId, field);
  const [local, setLocal] = useState(() => {
    if (field === "updated_by" && !notionValue) {
      return localStorage.getItem("wsf_editor_name") || "";
    }
    return notionValue != null ? String(notionValue) : "";
  });
  const focused = useRef(false);

  useEffect(() => {
    if (!focused.current) {
      let v = notionValue != null ? String(notionValue) : "";
      if (field === "updated_by" && !v) v = localStorage.getItem("wsf_editor_name") || "";
      setLocal(v);
    }
  }, [notionValue, field]);

  if (isSelect) {
    const OPTIONS = ["", "Draft", "Review", "Ratified", "Deprecated"];
    return (
      <span className="savestate" style={{ width: "100%" }}>
        <select
          className="tx-select"
          value={local}
          onChange={e => {
            const val = e.target.value;
            setLocal(val);
            saveField(stepId, field, val);
          }}
        >
          {OPTIONS.map(o => (
            <option key={o} value={o}>{o || "— select —"}</option>
          ))}
        </select>
        <SaveDot state={saveState} />
      </span>
    );
  }

  const handleFocus = () => {
    focused.current = true;
    if (field === "updated_by" && !local) {
      setLocal(localStorage.getItem("wsf_editor_name") || "");
    }
  };

  const handleBlur = e => {
    focused.current = false;
    const val = e.target.value;
    const current = notionValue != null ? String(notionValue) : "";
    if (val !== current) {
      if (field === "updated_by") localStorage.setItem("wsf_editor_name", val);
      saveField(stepId, field, val);
    }
  };

  if (multiline) {
    return (
      <span className="savestate" style={{ width: "100%" }}>
        <textarea
          className="tx-textarea"
          value={local}
          placeholder={placeholder || ""}
          rows={rows}
          onChange={e => setLocal(e.target.value)}
          onFocus={handleFocus}
          onBlur={handleBlur}
        />
        <SaveDot state={saveState} />
      </span>
    );
  }

  return (
    <span className="savestate" style={{ width: "100%" }}>
      <input
        className="tx-input"
        type={isNumber ? "number" : "text"}
        value={local}
        placeholder={placeholder || ""}
        onChange={e => setLocal(e.target.value)}
        onFocus={handleFocus}
        onBlur={handleBlur}
      />
      <SaveDot state={saveState} />
    </span>
  );
}

// ---------- suggestion thread (rebuilt) ----------
function SuggestionThread({ step, overlay, appendField, getSaveState }) {
  const [input, setInput] = useState("");
  const appendState = getSaveState(step.n, "suggested_owner_person");

  const suggestions = (overlay?.suggested_owner_person || "")
    .split(/\r?\n/)
    .map(s => s.trim())
    .filter(Boolean)
    .map(line => {
      const m = line.match(/^([^-]+?)\s*-\s*(.+)$/);
      if (m) return { author: m[1].trim(), text: m[2].trim() };
      return { author: "", text: line };
    });

  const handleAdd = () => {
    const text = input.trim();
    if (!text) return;
    const editorName = localStorage.getItem("wsf_editor_name") || "";
    const entry = editorName ? `${editorName} - ${text}` : text;
    appendField(step.n, "suggested_owner_person", entry);
    setInput("");
  };

  return (
    <div className="discussion-block">
      <div className="discussion-head">
        <span>Owner suggestions</span>
        {suggestions.length > 0 && <span className="count">{suggestions.length}</span>}
      </div>
      <div className="thread">
        {suggestions.length === 0
          ? <div className="thread-empty">No suggestions yet — add one below.</div>
          : suggestions.map((s, i) => (
              <div key={i} className="thread-bubble">
                <span className={`thread-author ${s.author ? "" : "no-author"}`}>
                  {s.author || "unattributed"}
                </span>
                <div className="thread-text">{s.text}</div>
              </div>
            ))
        }
      </div>
      <div className="thread-add">
        <input
          type="text"
          value={input}
          placeholder="Suggest a role or person…"
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => { if (e.key === "Enter") { e.preventDefault(); handleAdd(); } }}
        />
        <button onClick={handleAdd} disabled={!input.trim() || appendState === "saving"}>
          {appendState === "saving" ? "…" : "Post"}
        </button>
      </div>
    </div>
  );
}

// ---------- notion fields section (TIERED — strip / discussion / grid) ----------
function NotionFieldsSection({ step, overlay, saveField, appendField, getSaveState, loadState }) {
  if (loadState === "disabled") return null;

  if (loadState === "loading") {
    return (
      <div className="notion-fields">
        <div className="notion-strip">
          <span className="notion-strip-title">Notion overlay</span>
          <span className="notion-strip-meta">loading…</span>
        </div>
      </div>
    );
  }

  const ov = overlay || {};
  const status = ov.status || "";
  const updatedBy = ov.updated_by || "";

  return (
    <div className="notion-fields">
      {/* HEADER STRIP */}
      <div className="notion-strip">
        <span className="notion-strip-title">Notion overlay</span>
        <span className="savestate" style={{ display: "inline-block" }}>
          <span
            className="status-pill"
            data-status={status || "none"}
            onClick={() => {
              const next = { "": "Draft", "Draft": "Review", "Review": "Ratified", "Ratified": "Deprecated", "Deprecated": "" }[status] || "Draft";
              saveField(step.n, "status", next);
            }}
            title="Click to advance status"
          >
            <span className="dot" />
            <span>{status || "No status"}</span>
          </span>
          <SaveDot state={getSaveState(step.n, "status")} />
        </span>
        <span className="notion-strip-meta">
          <span>edited by</span>
          <EditableField
            stepId={step.n}
            field="updated_by"
            notionValue={updatedBy}
            placeholder="your name"
            saveField={saveField}
            getSaveState={getSaveState}
          />
        </span>
        {loadState === "error" && <SaveDot state="error" />}
      </div>

      {/* DISCUSSION */}
      <div className="notion-discussion">
        <SuggestionThread
          step={step}
          overlay={ov}
          appendField={appendField}
          getSaveState={getSaveState}
        />
        <div className="discussion-block">
          <div className="discussion-head"><span>Team notes</span></div>
          <EditableField stepId={step.n} field="discussion" notionValue={ov.discussion} placeholder="Thread / team discussion…" multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
        </div>
        {step.decision && (
          <div className="discussion-block">
            <div className="discussion-head decision-head"><span>Decision answer</span></div>
            <EditableField stepId={step.n} field="decision_answer" notionValue={ov.decision_answer} placeholder="Leadership answer to the open decision…" multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
          </div>
        )}
      </div>

      {/* OVERRIDE GRID */}
      <div className="notion-grid">
        <div className="grid-row">
          <span className="grid-label">Decided owner</span>
          <EditableField stepId={step.n} field="decided_owner_person" notionValue={ov.decided_owner_person} placeholder={step.owner.person || ""} saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row">
          <span className="grid-label">Backup</span>
          <EditableField stepId={step.n} field="backup_person" notionValue={ov.backup_person} placeholder={step.owner.backup || ""} saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row">
          <span className="grid-label">Handoff to #</span>
          <EditableField stepId={step.n} field="clean_handoff_to" notionValue={ov.clean_handoff_to != null ? String(ov.clean_handoff_to) : ""} placeholder={step.handoff ? String(step.handoff.to) : ""} isNumber saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row">
          <span className="grid-label">Friction / risk</span>
          <EditableField stepId={step.n} field="friction_risk" notionValue={ov.friction_risk} placeholder="Known blockers…" saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row full">
          <span className="grid-label">Done means (override)</span>
          <EditableField stepId={step.n} field="done_means" notionValue={ov.done_means} placeholder={step.done || ""} multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row full">
          <span className="grid-label">Required inputs</span>
          <EditableField stepId={step.n} field="required_inputs" notionValue={ov.required_inputs} placeholder="What must arrive for this step to start…" multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="grid-row full">
          <span className="grid-label">Required outputs</span>
          <EditableField stepId={step.n} field="required_outputs" notionValue={ov.required_outputs} placeholder="What must leave this step…" multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
        </div>
      </div>
    </div>
  );
}

// ---------- atoms ----------
function RoleChip({ role, person, backup, size = "sm" }) {
  const r = ROLES[role];
  if (!r) return null;
  return (
    <div className={`role-chip ${size}`} style={{ "--role-color": r.color }}>
      <span className="role-dot" />
      <span className="role-label">{r.label}</span>
      {person && <span className="role-person">· {person}</span>}
      {backup && <span className="role-backup">↩ {backup}</span>}
    </div>
  );
}

function PhaseTag({ phase }) {
  const p = PHASES.find(x => x.id === phase);
  if (!p) return null;
  return <span className={`phase-tag phase-${p.color}`}>{p.label}</span>;
}

function StepNumber({ n, decision }) {
  return (
    <div className={`step-num ${decision ? "decision" : ""}`}>
      <span className="step-num-inner">{String(n).padStart(2, "0")}</span>
    </div>
  );
}

function CodeBadge({ children, kind }) {
  return <code className={`code-badge ${kind || ""}`}>{children}</code>;
}

function FieldRow({ label, children, mono }) {
  return (
    <div className="field-row">
      <div className="field-label">{label}</div>
      <div className={`field-value ${mono ? "mono" : ""}`}>{children}</div>
    </div>
  );
}

// ---------- step card ----------
function StepCard({ step, expanded, onToggle, showSystems, showGuardrails, showSubsteps, density, overlay, saveField, appendField, getSaveState, loadState, isLive, liveEstimate, registerStepRef }) {
  const next = step.handoff ? STEPS.find(s => s.n === step.handoff.to) : null;
  const [mobileTab, setMobileTab] = useState("details");
  const cardRef = useRef(null);

  useEffect(() => {
    if (registerStepRef && cardRef.current) registerStepRef(step.n, cardRef.current);
  }, [registerStepRef, step.n]);

  return (
    <div
      ref={cardRef}
      className={`step-card ${expanded ? "expanded" : ""} ${density} ${step.decision ? "has-decision" : ""} ${isLive ? "live-step" : ""}`}
      data-step={step.n}
    >
      <div className="step-head" onClick={onToggle}>
        <StepNumber n={step.n} decision={step.decision} />
        <div className="step-title-block">
          {isLive && (
            <div className="live-eyebrow">Current step · EST {liveEstimate}</div>
          )}
          <div className="step-title-row">
            <h3 className="step-title">{step.title}</h3>
            <PhaseTag phase={step.phase} />
            {step.decision && <span className="decision-flag">NEEDS DECISION</span>}
          </div>
          <div className="step-meta-row">
            <RoleChip role={step.owner.role} person={step.owner.person} backup={step.owner.backup} />
            <span className="meta-sep">·</span>
            <span className="step-sla">SLA: {step.sla}</span>
          </div>
        </div>
        <button className="expand-btn" aria-label="Toggle">
          <span className="caret">{expanded ? "▾" : "▸"}</span>
        </button>
      </div>

      {expanded && (
        <div className={`step-body tab-${mobileTab}`}>
          <div className="mobile-tabs" role="tablist">
            <button
              className={`mobile-tab ${mobileTab === "details" ? "on" : ""}`}
              onClick={() => setMobileTab("details")}
            >Details</button>
            <button
              className={`mobile-tab ${mobileTab === "edit" ? "on" : ""}`}
              onClick={() => setMobileTab("edit")}
            >Edit{step.decision && <span className="tab-count">!</span>}</button>
          </div>

          {isLive && <LiveStepStatus step={step} estimate={liveEstimate} />}

          <div className="step-cols">
            <div className="step-col">
              <FieldRow label="Trigger">{step.trigger}</FieldRow>
              <FieldRow label="Definition of Done">{step.done}</FieldRow>
              <FieldRow label="Proof">
                <ul className="proof-list">
                  {step.proof.map((p, i) => <li key={i}>{p}</li>)}
                </ul>
              </FieldRow>
              {showSubsteps && step.sub.length > 0 && (
                <FieldRow label="Sub-procedure">
                  <ol className="sub-list">
                    {step.sub.map((s, i) => (
                      <li key={i}>
                        <span className="sub-label">{s.label}</span>
                        <span className="sub-done">→ {s.done}</span>
                      </li>
                    ))}
                  </ol>
                </FieldRow>
              )}
            </div>
            <div className="step-col">
              {showGuardrails && (
                <FieldRow label="Guardrail">
                  <span className="guardrail-text">⚠ {step.guardrail}</span>
                </FieldRow>
              )}
              {showSystems && (
                <>
                  <FieldRow label="Striven" mono>
                    <div className="badge-stack">
                      {step.striven.map((s, i) => (
                        <CodeBadge key={i} kind="striven">{s.kind}: {s.id}</CodeBadge>
                      ))}
                    </div>
                  </FieldRow>
                  {step.mcp.length > 0 && (
                    <FieldRow label="MCP tool" mono>
                      <div className="badge-stack">
                        {step.mcp.map((m, i) => <CodeBadge key={i} kind="mcp">{m}()</CodeBadge>)}
                      </div>
                    </FieldRow>
                  )}
                  {step.kb.length > 0 && (
                    <FieldRow label="Knowledge base" mono>
                      <div className="badge-stack">
                        {step.kb.map((k, i) => <CodeBadge key={i} kind="kb">{k}</CodeBadge>)}
                      </div>
                    </FieldRow>
                  )}
                </>
              )}
              {step.notes && (
                <FieldRow label="Notes">
                  <span className="notes-text">{step.notes}</span>
                </FieldRow>
              )}
            </div>
          </div>

          {next && (
            <div className="handoff-card">
              <div className="handoff-from">
                <div className="handoff-label">FROM</div>
                <RoleChip role={step.owner.role} person={step.owner.person} size="sm" />
              </div>
              <div className="handoff-arrow">
                <div className="handoff-artifact">{step.handoff.artifact}</div>
                <svg width="100%" height="20" viewBox="0 0 200 20" preserveAspectRatio="none">
                  <line x1="0" y1="10" x2="190" y2="10" stroke="currentColor" strokeWidth="1" strokeDasharray="2 2" />
                  <polygon points="190,4 200,10 190,16" fill="currentColor" />
                </svg>
              </div>
              <div className="handoff-to">
                <div className="handoff-label">TO · STEP {String(next.n).padStart(2,"0")}</div>
                <RoleChip role={next.owner.role} person={next.owner.person} size="sm" />
              </div>
            </div>
          )}

          <NotionFieldsSection
            step={step}
            overlay={overlay}
            saveField={saveField}
            appendField={appendField}
            getSaveState={getSaveState}
            loadState={loadState}
          />
        </div>
      )}
    </div>
  );
}

// ---------- timeline view ----------
function TimelineView({ tweaks, expanded, setExpanded, filtered, overlays, saveField, appendField, getSaveState, loadState, liveStepN, liveEstimate, registerStepRef }) {
  const byPhase = useMemo(() => {
    const m = {};
    PHASES.forEach(p => { m[p.id] = []; });
    filtered.forEach(s => { m[s.phase].push(s); });
    return m;
  }, [filtered]);

  return (
    <div className="timeline-view">
      {PHASES.map(phase => {
        const steps = byPhase[phase.id];
        if (!steps.length) return null;
        return (
          <section key={phase.id} className={`phase-section phase-${phase.color}`}>
            <header className="phase-header">
              <div className="phase-meta">
                <span className="phase-range">{String(phase.range[0]).padStart(2, "0")}{phase.range[1] !== phase.range[0] ? `–${String(phase.range[1]).padStart(2, "0")}` : ""}</span>
                <h2 className="phase-title">{phase.label}</h2>
                <span className="phase-count">{steps.length} STEP{steps.length > 1 ? "S" : ""}</span>
              </div>
              <div className="phase-battery-wrap" title={`${steps.filter(s => (s.notion?.status || "") === "Ratified").length}/${steps.length} ratified`}>
                <BatteryRadial pct={steps.length ? (steps.filter(s => (s.notion?.status || "") === "Ratified").length / steps.length) * 100 : 0} size="sm" showPct={false} />
                <span className="phase-battery-label">{steps.filter(s => (s.notion?.status || "") === "Ratified").length}/{steps.length}</span>
              </div>
            </header>
            <div className="phase-steps">
              {steps.map(step => (
                <StepCard
                  key={step.n}
                  step={step}
                  expanded={expanded.has(step.n)}
                  onToggle={() => {
                    const n = new Set(expanded);
                    if (n.has(step.n)) n.delete(step.n); else n.add(step.n);
                    setExpanded(n);
                  }}
                  showSystems={tweaks.showSystems}
                  showGuardrails={tweaks.showGuardrails}
                  showSubsteps={tweaks.showSubsteps}
                  density={tweaks.density}
                  overlay={overlays[step.n]}
                  saveField={saveField}
                  appendField={appendField}
                  getSaveState={getSaveState}
                  loadState={loadState}
                  isLive={step.n === liveStepN}
                  liveEstimate={liveEstimate}
                  registerStepRef={registerStepRef}
                />
              ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}

// ---------- swim-lane drilldown popover (live estimate mock) ----------
function SwimDrilldown({ step, estimate, overlay, onClose, onJumpTimeline }) {
  const ov = overlay || {};
  const owner = ov.decided_owner_person || step.owner.person || "Unassigned";
  // Mock done-checklist: split step.done by '/' or use proof_required.
  const doneItems = (step.done || "").split(/[•;·\/\n]/).map(s => s.trim()).filter(Boolean).slice(0, 5);
  const items = doneItems.length ? doneItems : [step.done || "Step complete"];
  // First item arbitrarily "done" in mock so the checklist reads as live state.
  const next = step.handoff ? STEPS.find(s => s.n === step.handoff.to) : null;
  return (
    <div className="swim-drill" onClick={e => e.stopPropagation()} style={{ left: "100%", marginLeft: 8, top: 0 }}>
      <button className="swim-drill-close" onClick={onClose} aria-label="Close">✕</button>
      <div className="swim-drill-eyebrow">EST {estimate} · LIVE HERE</div>
      <h4>Step {String(step.n).padStart(2,"0")} · {step.title}</h4>
      <div className="drill-section">
        <div className="drill-label">Current owner</div>
        <div className="drill-owner">
          <RoleChip role={step.owner.role} person={owner} size="sm" />
        </div>
      </div>
      <div className="drill-section">
        <div className="drill-label">Done means · mocked</div>
        <ul className="drill-checklist">
          {items.map((it, i) => (
            <li key={i}>
              <span className={`drill-check ${i === 0 ? "checked" : ""}`}>{i === 0 ? "✓" : ""}</span>
              <span>{it}</span>
            </li>
          ))}
        </ul>
      </div>
      {next && (
        <div className="drill-section">
          <div className="drill-label">Hands off to</div>
          <div className="drill-handoff">
            <strong>Step {String(next.n).padStart(2,"0")}</strong> · {next.title} · <RoleChip role={next.owner.role} size="sm" />
          </div>
        </div>
      )}
      <div className="drill-section" style={{ display: "flex", gap: 8, marginTop: 14, paddingTop: 10, borderTop: "1px dashed var(--line-soft)" }}>
        <button className="action-btn" onClick={onJumpTimeline}>Open in timeline →</button>
      </div>
    </div>
  );
}

// ---------- swim-lane view ----------
function SwimLaneView({ filtered, setExpanded, setView, liveStepN, liveEstimate, overlays }) {
  const [drillN, setDrillN] = useState(null);
  useEffect(() => { setDrillN(liveStepN || null); }, [liveStepN]);
  const rolesUsed = useMemo(() => {
    const set = new Set(filtered.map(s => s.owner.role));
    return Object.keys(ROLES).filter(r => set.has(r));
  }, [filtered]);

  return (
    <div className="swim-view">
      <div className="swim-scroll">
        <div className={`swim-grid ${liveStepN ? "has-live" : ""}`} style={{ gridTemplateColumns: `180px repeat(${filtered.length}, 76px)` }}>
          <div className="swim-corner">ROLE / STEP →</div>
          {filtered.map(s => (
            <div key={s.n} className="swim-col-head">
              <div className="swim-col-num">{String(s.n).padStart(2,"0")}</div>
              <PhaseTag phase={s.phase} />
            </div>
          ))}
          {rolesUsed.map(role => (
            <React.Fragment key={role}>
              <div className="swim-row-head">
                <RoleChip role={role} size="sm" />
              </div>
              {filtered.map(s => {
                const active = s.owner.role === role;
                const next = s.handoff && STEPS.find(x => x.n === s.handoff.to);
                const handoff = next && next.owner.role !== role && s.owner.role === role;
                const isLive = active && s.n === liveStepN;
                return (
                  <div key={s.n} className={`swim-cell ${active ? "active" : ""} ${isLive ? "live-cell" : ""}`}>
                    {active && (
                      <button
                        className={`swim-dot ${isLive ? "live" : ""}`}
                        title={`${s.title}${isLive ? ` — EST ${liveEstimate} is here · click for details` : " — click to expand"}`}
                        onClick={() => {
                          if (isLive) { setDrillN(drillN === s.n ? null : s.n); return; }
                          setView("timeline");
                          setExpanded(new Set([s.n]));
                          setTimeout(() => {
                            const el = document.querySelector(`[data-step="${s.n}"]`);
                            if (el) el.scrollIntoView({ block: "center" });
                          }, 50);
                        }}
                        style={{ "--role-color": ROLES[role].color }}
                      >
                        <span>{String(s.n).padStart(2,"0")}</span>
                      </button>
                    )}
                    {isLive && <span className="live-est-chip">EST {liveEstimate}</span>}
                    {isLive && drillN === s.n && (
                      <SwimDrilldown step={s} estimate={liveEstimate} overlay={overlays && overlays[s.n]} onClose={() => setDrillN(null)} onJumpTimeline={() => {
                        setView("timeline"); setExpanded(new Set([s.n]));
                        setTimeout(() => { const el = document.querySelector(`[data-step="${s.n}"]`); if (el) el.scrollIntoView({ block: "center" }); }, 50);
                      }} />
                    )}
                    {handoff && <div className="swim-handoff" />}
                  </div>
                );
              })}
            </React.Fragment>
          ))}
        </div>
      </div>
      <div className="swim-legend">
        <span><span className="swim-dot-eg" /> Step owned by this role</span>
        <span><span className="swim-handoff-eg" /> Handoff out of this role</span>
        {liveStepN && <span style={{ color: "var(--accent)" }}><span className="swim-dot-eg" style={{ borderColor: "var(--accent)", background: "var(--accent)" }} /> Live estimate location</span>}
      </div>
    </div>
  );
}

// ---------- kanban view ----------
function KanbanView({ filtered, setExpanded, setView, liveStepN, liveEstimate }) {
  return (
    <div className="kanban-view">
      {PHASES.map(p => {
        const steps = filtered.filter(s => s.phase === p.id);
        if (!steps.length) return null;
        return (
          <div key={p.id} className={`kanban-col phase-${p.color}`}>
            <header className="kanban-head">
              <h3>{p.label}</h3>
              <span className="kanban-count">{steps.length}</span>
            </header>
            <div className="kanban-list">
              {steps.map(s => {
                const isLive = s.n === liveStepN;
                return (
                <button
                  key={s.n}
                  className={`kanban-card ${s.decision ? "has-decision" : ""} ${isLive ? "live-step" : ""}`}
                  onClick={() => {
                    setView("timeline");
                    setExpanded(new Set([s.n]));
                    setTimeout(() => {
                      const el = document.querySelector(`[data-step="${s.n}"]`);
                      if (el) el.scrollIntoView({ block: "center" });
                    }, 50);
                  }}
                >
                  {isLive && <div className="live-eyebrow">EST {liveEstimate} is here</div>}
                  <div className="kanban-num">{String(s.n).padStart(2,"0")}</div>
                  <div className="kanban-title">{s.title}</div>
                  <div className="kanban-meta">
                    <RoleChip role={s.owner.role} size="sm" />
                  </div>
                  {s.decision && <div className="kanban-flag">NEEDS DECISION</div>}
                </button>
                );
              })}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ---------- doc view ----------
function DocView({ filtered }) {
  return (
    <div className="doc-view">
      <article className="doc-page">
        <header className="doc-header">
          <div className="doc-eyebrow">William Smith Fireplaces · Operational SOP</div>
          <h1>The 21-Step Job Lifecycle</h1>
          <p className="doc-lede">
            A guided job-flow with guardrails. Every step has an owner, a definition of done, and a clean handoff. Where ownership is undecided, the step is flagged for leadership confirmation.
          </p>
        </header>
        {PHASES.map(p => {
          const steps = filtered.filter(s => s.phase === p.id);
          if (!steps.length) return null;
          return (
            <section key={p.id} className="doc-phase">
              <h2><span className="doc-phase-num">{p.range[0]}{p.range[1] !== p.range[0] ? `–${p.range[1]}` : ""}</span> {p.label}</h2>
              {steps.map(s => (
                <div key={s.n} className="doc-step">
                  <h3>{String(s.n).padStart(2,"0")}. {s.title}</h3>
                  <div className="doc-grid">
                    <div><strong>Owner:</strong> {ROLES[s.owner.role].label} · {s.owner.person} <em>(backup: {s.owner.backup})</em></div>
                    <div><strong>Trigger:</strong> {s.trigger}</div>
                    <div><strong>Done:</strong> {s.done}</div>
                    <div><strong>Guardrail:</strong> {s.guardrail}</div>
                    <div><strong>SLA:</strong> {s.sla}</div>
                    {s.handoff && <div><strong>Handoff:</strong> {s.handoff.artifact} → step {s.handoff.to}</div>}
                  </div>
                  {s.decision && <div className="doc-decision">⚠ NEEDS DECISION — {s.notes}</div>}
                </div>
              ))}
            </section>
          );
        })}
      </article>
    </div>
  );
}

// ---------- open decisions panel (slide-over, save-tracked) ----------
function DecisionsPanel({ open, onClose, overlays, saveField, getSaveState, loadState }) {
  if (!open) return null;
  const cycleStatus = (cur) => ({ "": "Draft", "Draft": "Review", "Review": "Ratified", "Ratified": "Deprecated", "Deprecated": "" }[cur] || "Draft");
  return (
    <div className="decisions-overlay" onClick={onClose}>
      <div className="decisions-panel" onClick={e => e.stopPropagation()}>
        <header>
          <h2>Open decisions <span className="count-pill">{OPEN_DECISIONS.length}</span></h2>
          <button className="close-btn" onClick={onClose} aria-label="Close">✕</button>
        </header>
        <p className="decisions-lede">
          Each step below needs leadership input before the SOP can be ratified. Answers save to Notion for the whole team.
          {loadState === "disabled" && <span className="decisions-notice"> Worker not connected — read-only.</span>}
          {loadState === "error"    && <span className="decisions-notice"> Notion unreachable — read-only.</span>}
        </p>
        <ol className="decisions-list">
          {OPEN_DECISIONS.map(d => {
            const ov = overlays[d.n] || {};
            const status = ov.status || "";
            return (
              <li key={d.n}>
                <div className="decision-num">STEP {String(d.n).padStart(2,"0")}</div>
                <div className="decision-body">
                  <h4>{d.title}</h4>
                  <p>{d.question}</p>
                  {loadState === "loaded" && (
                    <div className="decision-fieldgroup">
                      <span className="field-label-mini">Answer</span>
                      <div className="field-with-dot">
                        <EditableField stepId={d.n} field="decision_answer" notionValue={ov.decision_answer} placeholder="Leadership answer…" multiline rows={2} saveField={saveField} getSaveState={getSaveState} />
                        <SaveDot state={getSaveState(d.n, "decision_answer")} />
                      </div>
                      <span className="field-label-mini">Owner</span>
                      <div className="field-with-dot">
                        <EditableField stepId={d.n} field="decided_owner_person" notionValue={ov.decided_owner_person} placeholder={d.suggestedOwner || "Decided owner…"} saveField={saveField} getSaveState={getSaveState} />
                        <SaveDot state={getSaveState(d.n, "decided_owner_person")} />
                      </div>
                      <span className="field-label-mini">Status</span>
                      <div className="field-with-dot">
                        <span
                          className="decision-status-pill"
                          data-status={status}
                          onClick={() => saveField(d.n, "status", cycleStatus(status))}
                          title="Click to advance status"
                        >
                          <span className="status-dot-inline" />
                          <span>{status || "Draft →"}</span>
                        </span>
                        <SaveDot state={getSaveState(d.n, "status")} />
                      </div>
                    </div>
                  )}
                </div>
              </li>
            );
          })}
        </ol>
      </div>
    </div>
  );
}

// ---------- header ----------
function AppHeader({ tweaks, setTweak, view, setView, openDecisions, loadState }) {
  const connLabel = { loaded: "live", loading: "loading…", error: "offline", disabled: "local" }[loadState] || "";
  const connTitle = { loaded: "Connected to Notion via Worker", loading: "Loading overlays…", error: "Notion unreachable — read-only", disabled: "Worker not configured — read-only" }[loadState] || "";
  return (
    <header className="app-header">
      <div className="brand">
        <div className="brand-mark">
          <svg width="28" height="28" viewBox="0 0 28 28">
            <rect x="3" y="3" width="22" height="22" fill="none" stroke="currentColor" strokeWidth="1.2" />
            <line x1="3" y1="11" x2="25" y2="11" stroke="currentColor" strokeWidth="1.2" />
            <line x1="3" y1="19" x2="25" y2="19" stroke="currentColor" strokeWidth="1.2" />
            <line x1="11" y1="3" x2="11" y2="25" stroke="currentColor" strokeWidth="1.2" />
            <circle cx="18" cy="15" r="2" fill="var(--accent)" />
          </svg>
        </div>
        <div className="brand-text">
          <div className="brand-line1">William Smith Fireplaces</div>
          <div className="brand-line2">Operational SOP · v1.0 working draft</div>
        </div>
      </div>
      <nav className="view-switcher">
        {[
          { id: "timeline", label: "Timeline" },
          { id: "swim",     label: "Swim-lane" },
          { id: "kanban",   label: "Kanban" },
          { id: "doc",      label: "Document" },
        ].map(v => (
          <button
            key={v.id}
            className={`view-btn ${view === v.id ? "active" : ""}`}
            onClick={() => setView(v.id)}
          >{v.label}</button>
        ))}
      </nav>
      <div className="header-actions">
        <span className="conn-status" data-state={loadState} title={connTitle}>
          <span className="conn-dot" />
          <span className="conn-label">{connLabel}</span>
        </span>
        <button className="action-btn decisions-btn" onClick={openDecisions}>
          <span className="action-dot" />
          {OPEN_DECISIONS.length} open decisions
        </button>
      </div>
    </header>
  );
}

// ---------- filter bar ----------
function FilterBar({ tweaks, setTweak }) {
  return (
    <div className="filter-bar">
      <div className="filter-group">
        <label>ROLE</label>
        <select value={tweaks.filterRole} onChange={e => setTweak("filterRole", e.target.value)}>
          <option value="all">All roles</option>
          {Object.entries(ROLES).map(([k, r]) => (
            <option key={k} value={k}>{r.label}</option>
          ))}
        </select>
      </div>
      <div className="filter-group">
        <label>PHASE</label>
        <select value={tweaks.filterPhase} onChange={e => setTweak("filterPhase", e.target.value)}>
          <option value="all">All phases</option>
          {PHASES.map(p => <option key={p.id} value={p.id}>{p.label}</option>)}
        </select>
      </div>
      <div className="filter-group">
        <label>DENSITY</label>
        <div className="seg">
          {["comfortable", "compact"].map(d => (
            <button key={d} className={tweaks.density === d ? "on" : ""} onClick={() => setTweak("density", d)}>{d}</button>
          ))}
        </div>
      </div>
      <div className="filter-group inline-toggles">
        <label className="inline-toggle">
          <input type="checkbox" checked={tweaks.showSystems} onChange={e => setTweak("showSystems", e.target.checked)} />
          System links
        </label>
        <label className="inline-toggle">
          <input type="checkbox" checked={tweaks.showGuardrails} onChange={e => setTweak("showGuardrails", e.target.checked)} />
          Guardrails
        </label>
        <label className="inline-toggle">
          <input type="checkbox" checked={tweaks.showSubsteps} onChange={e => setTweak("showSubsteps", e.target.checked)} />
          Sub-procedures
        </label>
      </div>
    </div>
  );
}

// ---------- root ----------
function App() {
  const [tweaks, setTweak] = useTweaks(TWEAK_DEFAULTS);
  const [view, setView] = useState(tweaks.view || "timeline");
  const [expanded, setExpanded] = useState(new Set([1]));
  const [decisionsOpen, setDecisionsOpen] = useState(false);
  const [liveEstimate, setLiveEstimate] = useState("");
  const { overlays, loadState, loadError, saveField, appendField, getSaveState } = useNotionData();

  const [liveStepN, setLiveStepN] = useState(null);
  useEffect(() => {
    if (!liveEstimate) { setLiveStepN(null); return; }
    lookupEstimateStage(liveEstimate).then(setLiveStepN);
  }, [liveEstimate]);

  const stepRefs = useRef({});
  const registerStepRef = useCallback((n, el) => { stepRefs.current[n] = el; }, []);

  // When estimate is set, jump to swim-lane view (per spec)
  useEffect(() => {
    if (liveEstimate && liveStepN) {
      setView("swim");
    }
  }, [liveEstimate, liveStepN]);

  const filtered = useMemo(() => {
    return STEPS.filter(s => {
      if (tweaks.filterRole !== "all" && s.owner.role !== tweaks.filterRole) return false;
      if (tweaks.filterPhase !== "all" && s.phase !== tweaks.filterPhase) return false;
      return true;
    });
  }, [tweaks.filterRole, tweaks.filterPhase]);

  return (
    <div className={`app theme-blueprint`} data-screen-label="SOP Framework">
      <AppHeader
        tweaks={tweaks}
        setTweak={setTweak}
        view={view}
        setView={setView}
        openDecisions={() => setDecisionsOpen(true)}
        loadState={loadState}
      />
      <ConnectionBanner loadState={loadState} loadError={loadError} />
      <EstimateLookup
        value={liveEstimate}
        onChange={setLiveEstimate}
        onClear={() => setLiveEstimate("")}
      />
      <FilterBar tweaks={tweaks} setTweak={setTweak} />
      <main className="app-main">
        {view === "timeline" && (
          <TimelineWrap>
            <TimelineView
              tweaks={tweaks}
              expanded={expanded}
              setExpanded={setExpanded}
              filtered={filtered}
              overlays={overlays}
              saveField={saveField}
              appendField={appendField}
              getSaveState={getSaveState}
              loadState={loadState}
              liveStepN={liveStepN}
              liveEstimate={liveEstimate}
              registerStepRef={registerStepRef}
            />
          </TimelineWrap>
        )}
        {view === "swim"   && <SwimLaneView filtered={filtered} setExpanded={setExpanded} setView={setView} liveStepN={liveStepN} liveEstimate={liveEstimate} overlays={overlays} />}
        {view === "kanban" && <KanbanView   filtered={filtered} setExpanded={setExpanded} setView={setView} liveStepN={liveStepN} liveEstimate={liveEstimate} />}
        {view === "doc"    && <DocView      filtered={filtered} />}
      </main>
      <DecisionsPanel
        open={decisionsOpen}
        onClose={() => setDecisionsOpen(false)}
        overlays={overlays}
        saveField={saveField}
        getSaveState={getSaveState}
        loadState={loadState}
      />
      <TweaksPanel title="Tweaks">
        <TweakSection title="View">
          <TweakSelect tweaks={tweaks} setTweak={setTweak} k="view" label="Mode" options={[
            { value: "timeline", label: "Timeline" },
            { value: "swim", label: "Swim-lane" },
            { value: "kanban", label: "Kanban" },
            { value: "doc", label: "Document" },
          ]} onChange={v => setView(v)} />
          <TweakRadio tweaks={tweaks} setTweak={setTweak} k="density" label="Density" options={["comfortable", "compact"]} />
        </TweakSection>
        <TweakSection title="Show on cards">
          <TweakToggle tweaks={tweaks} setTweak={setTweak} k="showSystems"    label="System links (Striven, MCP, KB)" />
          <TweakToggle tweaks={tweaks} setTweak={setTweak} k="showGuardrails" label="Guardrails" />
          <TweakToggle tweaks={tweaks} setTweak={setTweak} k="showSubsteps"   label="Sub-procedures" />
        </TweakSection>
      </TweaksPanel>
    </div>
  );
}

function TimelineWrap({ children }) { return <div className="timeline-wrap">{children}</div>; }

ReactDOM.createRoot(document.getElementById("root")).render(<App />);
