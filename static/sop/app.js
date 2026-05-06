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

// ---------- save badge ----------
function SaveBadge({ state }) {
  if (state === "saving") return <span className="save-badge saving">saving…</span>;
  if (state === "saved")  return <span className="save-badge saved">saved ✓</span>;
  if (state === "error")  return <span className="save-badge error">error</span>;
  return <span className="save-badge" />;
}

// ---------- editable field ----------
function EditableField({ stepId, field, notionValue, placeholder, multiline, isSelect, isNumber, saveField, getSaveState }) {
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
      <div className="editable-wrap">
        <select
          className="editable-select"
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
        <SaveBadge state={saveState} />
      </div>
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
      <div className="editable-wrap">
        <textarea
          className="editable-textarea"
          value={local}
          placeholder={placeholder || ""}
          rows={3}
          onChange={e => setLocal(e.target.value)}
          onFocus={handleFocus}
          onBlur={handleBlur}
        />
        <SaveBadge state={saveState} />
      </div>
    );
  }

  return (
    <div className="editable-wrap">
      <input
        className="editable-input"
        type={isNumber ? "number" : "text"}
        value={local}
        placeholder={placeholder || ""}
        onChange={e => setLocal(e.target.value)}
        onFocus={handleFocus}
        onBlur={handleBlur}
      />
      <SaveBadge state={saveState} />
    </div>
  );
}

// ---------- owner suggestion field ----------
function OwnerSuggestionField({ step, overlay, saveField, appendField, getSaveState }) {
  const [input, setInput] = useState("");
  const appendState = getSaveState(step.n, "suggested_owner_person");

  const suggestions = (overlay?.suggested_owner_person || "")
    .split("\n")
    .map(s => s.trim())
    .filter(Boolean);

  const handleAdd = () => {
    const text = input.trim();
    if (!text) return;
    const editorName = localStorage.getItem("wsf_editor_name") || "";
    const entry = editorName ? `${editorName} — ${text}` : text;
    appendField(step.n, "suggested_owner_person", entry);
    setInput("");
  };

  const handleKeyDown = e => {
    if (e.key === "Enter") { e.preventDefault(); handleAdd(); }
  };

  return (
    <div className="owner-suggestion-wrap">
      <div className="notion-field-row">
        <span className="notion-field-label">Suggested Step Owner</span>
        <div className="suggestion-list">
          {suggestions.length === 0
            ? <span className="suggestion-list-empty">No suggestions yet — add one below</span>
            : suggestions.map((s, i) => (
                <div key={i} className="suggestion-entry">· {s}</div>
              ))
          }
        </div>
        <div className="suggestion-input-row">
          <input
            className="suggestion-input"
            type="text"
            value={input}
            placeholder="Role or name suggestion…"
            onChange={e => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
          />
          <button
            className="suggestion-btn"
            onClick={handleAdd}
            disabled={!input.trim() || appendState === "saving"}
          >Add</button>
          <SaveBadge state={appendState} />
        </div>
      </div>
      <div className="notion-field-row decided-owner-field">
        <span className="notion-field-label decided-owner-label">Decided Step Owner</span>
        <EditableField
          stepId={step.n}
          field="decided_owner_person"
          notionValue={overlay?.decided_owner_person}
          placeholder={step.owner.person || ""}
          saveField={saveField}
          getSaveState={getSaveState}
        />
      </div>
    </div>
  );
}

// ---------- notion fields section ----------
function NotionFieldsSection({ step, overlay, saveField, appendField, getSaveState, loadState }) {
  if (loadState === "disabled") return null;

  if (loadState === "loading") {
    return (
      <div className="notion-fields">
        <div className="notion-fields-header">
          <span className="notion-fields-title">Notion overlay</span>
          <span className="notion-fields-hint">loading…</span>
        </div>
      </div>
    );
  }

  const ov = overlay || {};

  return (
    <div className="notion-fields">
      <div className="notion-fields-header">
        <span className="notion-fields-title">Notion overlay</span>
        {loadState === "error" && (
          <span className="save-badge error" style={{ visibility: "visible", opacity: 1 }}>Notion unreachable</span>
        )}
      </div>
      <div className="notion-fields-grid">
        <OwnerSuggestionField
          step={step}
          overlay={ov}
          saveField={saveField}
          appendField={appendField}
          getSaveState={getSaveState}
        />
        <div className="notion-field-row">
          <span className="notion-field-label">Status</span>
          <EditableField stepId={step.n} field="status" notionValue={ov.status} isSelect saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Updated By</span>
          <EditableField stepId={step.n} field="updated_by" notionValue={ov.updated_by} placeholder="Your name" saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Backup</span>
          <EditableField stepId={step.n} field="backup_person" notionValue={ov.backup_person} placeholder={step.owner.backup || ""} saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Handoff To Step #</span>
          <EditableField stepId={step.n} field="clean_handoff_to" notionValue={ov.clean_handoff_to != null ? String(ov.clean_handoff_to) : ""} placeholder={step.handoff ? String(step.handoff.to) : ""} isNumber saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Friction / Risk</span>
          <EditableField stepId={step.n} field="friction_risk" notionValue={ov.friction_risk} placeholder="Known blockers…" saveField={saveField} getSaveState={getSaveState} />
        </div>
      </div>
      <div className="notion-fields-wide">
        <div className="notion-field-row">
          <span className="notion-field-label">Done Means (override)</span>
          <EditableField stepId={step.n} field="done_means" notionValue={ov.done_means} placeholder={step.done || ""} multiline saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Required Inputs</span>
          <EditableField stepId={step.n} field="required_inputs" notionValue={ov.required_inputs} placeholder="What must arrive for this step to start…" multiline saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Required Outputs</span>
          <EditableField stepId={step.n} field="required_outputs" notionValue={ov.required_outputs} placeholder="What must leave this step…" multiline saveField={saveField} getSaveState={getSaveState} />
        </div>
        <div className="notion-field-row">
          <span className="notion-field-label">Team Notes</span>
          <EditableField stepId={step.n} field="discussion" notionValue={ov.discussion} placeholder="Thread / team discussion…" multiline saveField={saveField} getSaveState={getSaveState} />
        </div>
        {step.decision && (
          <div className="notion-field-row decision-answer-field">
            <span className="notion-field-label decision-answer-label">Decision Answer</span>
            <EditableField stepId={step.n} field="decision_answer" notionValue={ov.decision_answer} placeholder="Leadership answer to the open decision…" multiline saveField={saveField} getSaveState={getSaveState} />
          </div>
        )}
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
function StepCard({ step, expanded, onToggle, showSystems, showGuardrails, showSubsteps, density, overlay, saveField, appendField, getSaveState, loadState }) {
  const next = step.handoff ? STEPS.find(s => s.n === step.handoff.to) : null;
  return (
    <div
      className={`step-card ${expanded ? "expanded" : ""} ${density} ${step.decision ? "has-decision" : ""}`}
      data-step={step.n}
    >
      <div className="step-head" onClick={onToggle}>
        <StepNumber n={step.n} decision={step.decision} />
        <div className="step-title-block">
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
        <div className="step-body">
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
function TimelineView({ tweaks, expanded, setExpanded, filtered, overlays, saveField, appendField, getSaveState, loadState }) {
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
              <div className="phase-rail" />
              <div className="phase-meta">
                <div className="phase-range">STEPS {phase.range[0]}{phase.range[1] !== phase.range[0] ? `–${phase.range[1]}` : ""}</div>
                <h2 className="phase-title">{phase.label}</h2>
                <div className="phase-count">{steps.length} step{steps.length > 1 ? "s" : ""}</div>
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
                />
              ))}
            </div>
          </section>
        );
      })}
    </div>
  );
}

// ---------- swim-lane view ----------
function SwimLaneView({ filtered, setExpanded, setView }) {
  const rolesUsed = useMemo(() => {
    const set = new Set(filtered.map(s => s.owner.role));
    return Object.keys(ROLES).filter(r => set.has(r));
  }, [filtered]);

  return (
    <div className="swim-view">
      <div className="swim-scroll">
        <div className="swim-grid" style={{ gridTemplateColumns: `180px repeat(${filtered.length}, 76px)` }}>
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
                return (
                  <div key={s.n} className={`swim-cell ${active ? "active" : ""}`}>
                    {active && (
                      <button
                        className="swim-dot"
                        title={`${s.title} — click to expand`}
                        onClick={() => {
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
      </div>
    </div>
  );
}

// ---------- kanban view ----------
function KanbanView({ filtered, setExpanded, setView }) {
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
              {steps.map(s => (
                <button
                  key={s.n}
                  className={`kanban-card ${s.decision ? "has-decision" : ""}`}
                  onClick={() => {
                    setView("timeline");
                    setExpanded(new Set([s.n]));
                    setTimeout(() => {
                      const el = document.querySelector(`[data-step="${s.n}"]`);
                      if (el) el.scrollIntoView({ block: "center" });
                    }, 50);
                  }}
                >
                  <div className="kanban-num">{String(s.n).padStart(2,"0")}</div>
                  <div className="kanban-title">{s.title}</div>
                  <div className="kanban-meta">
                    <RoleChip role={s.owner.role} size="sm" />
                  </div>
                  {s.decision && <div className="kanban-flag">NEEDS DECISION</div>}
                </button>
              ))}
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

// ---------- open decisions panel ----------
function DecisionsPanel({ open, onClose, overlays, saveField, getSaveState, loadState }) {
  if (!open) return null;
  return (
    <div className="decisions-overlay" onClick={onClose}>
      <div className="decisions-panel" onClick={e => e.stopPropagation()}>
        <header>
          <h2>Open Decisions</h2>
          <button className="close-btn" onClick={onClose}>✕</button>
        </header>
        <p className="decisions-lede">
          Steps below need leadership input before the SOP can be considered "ratified." Each is a single, focused question.
          {loadState === "disabled" && (
            <span className="decisions-notice"> Answers cannot be saved until Worker is connected.</span>
          )}
          {loadState === "error" && (
            <span className="decisions-notice"> Notion unreachable — answers cannot be saved.</span>
          )}
        </p>
        <ol className="decisions-list">
          {OPEN_DECISIONS.map(d => (
            <li key={d.n}>
              <div className="decision-num">STEP {String(d.n).padStart(2,"0")}</div>
              <div className="decision-body">
                <h4>{d.title}</h4>
                <p>{d.question}</p>
                {loadState === "loaded" && (
                  <div className="decision-answer-field">
                    <span className="notion-field-label decision-answer-label">Answer</span>
                    <EditableField
                      stepId={d.n}
                      field="decision_answer"
                      notionValue={overlays[d.n] ? overlays[d.n].decision_answer : ""}
                      placeholder="Leadership answer to the open decision…"
                      multiline
                      saveField={saveField}
                      getSaveState={getSaveState}
                    />
                  </div>
                )}
              </div>
            </li>
          ))}
        </ol>
      </div>
    </div>
  );
}

// ---------- header ----------
function AppHeader({ tweaks, setTweak, view, setView, openDecisions }) {
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
  const { overlays, loadState, loadError, saveField, appendField, getSaveState } = useNotionData();

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
      />
      <ConnectionBanner loadState={loadState} loadError={loadError} />
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
            />
          </TimelineWrap>
        )}
        {view === "swim"   && <SwimLaneView filtered={filtered} setExpanded={setExpanded} setView={setView} />}
        {view === "kanban" && <KanbanView   filtered={filtered} setExpanded={setExpanded} setView={setView} />}
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
