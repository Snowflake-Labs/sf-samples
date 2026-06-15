import React, { useState, useEffect, useRef } from 'react';
import {
  Brain, Layers, Database, Timer, Server, SlidersHorizontal,
  ArrowUp, ArrowDown, Info, Plus, Container, Cpu,
  CheckCircle2, XCircle, RefreshCw, AlertTriangle
} from 'lucide-react';
import {
  fetchLLMThresholds, fetchMatchingRules, fetchSources,
  fetchPipelineTasks, fetchInfrastructure, fetchDisplayConfig,
  type LLMThresholds, type MatchingRule, type SourceConfig,
  type PipelineTask, type InfraConfig, type DisplayConfig,
} from '../api';
import RichSelect, { type RichSelectOption } from '../components/RichSelect';

/* ──────────────────────────────────────────────
   Types for internal section state
   ────────────────────────────────────────────── */

interface LLMConfigState {
  LLM_REVIEW_SCORE_LOW: string;
  LLM_REVIEW_SCORE_HIGH: string;
  LLM_REVIEW_BATCH_SIZE: string;
  LLM_REVIEW_MODEL: string;
}

interface RuleRow {
  id: string;
  name: string;
  type: string;
  base_score: number;
  active: boolean;
  description: string;
  priority: number | null;
  anchor_field: string | null;
  exact_match_fields: string | null;
  fuzzy_match_field: string | null;
  fuzzy_algorithm: string | null;
  fuzzy_threshold: number | null;
  use_fuzzy_score: boolean;
  require_cross_source: boolean;
  require_different_record: boolean;
}

interface SettingsData {
  llmConfig: LLMConfigState | null;
  availableModels: string[];
  rules: RuleRow[] | null;
  sources: SourceConfig[] | null;
  tasks: PipelineTask[] | null;
  infra: InfraConfig | null;
  display: DisplayConfig | null;
  loading: boolean;
  errors: Partial<Record<string, string>>;
}

function toLLMConfigState(t: LLMThresholds): LLMConfigState {
  return {
    LLM_REVIEW_SCORE_LOW: String(t.score_low),
    LLM_REVIEW_SCORE_HIGH: String(t.score_high),
    LLM_REVIEW_BATCH_SIZE: String(t.batch_size),
    LLM_REVIEW_MODEL: t.model,
  };
}

function toRuleRows(rules: MatchingRule[]): RuleRow[] {
  return rules.map(r => ({
    id: r.rule_id,
    name: r.rule_name,
    type: r.match_type || 'DETERMINISTIC',
    base_score: r.base_score ?? 1.0,
    active: r.is_active ?? true,
    description: r.rule_description || '',
    priority: r.rule_priority,
    anchor_field: r.anchor_field,
    exact_match_fields: r.exact_match_fields,
    fuzzy_match_field: r.fuzzy_match_field,
    fuzzy_algorithm: r.fuzzy_algorithm,
    fuzzy_threshold: r.fuzzy_threshold,
    use_fuzzy_score: r.use_fuzzy_score ?? false,
    require_cross_source: r.require_cross_source ?? false,
    require_different_record: r.require_different_record ?? true,
  }));
}

type SectionId = 'ai' | 'rules' | 'sources' | 'pipeline' | 'infra' | 'display';

// Statuses that mean a component is up/available (vs suspended/stopped/failed).
const INFRA_UP_STATUSES = new Set(['STARTED', 'ACTIVE', 'IDLE', 'RUNNING']);

function isInfraUp(status: string | undefined | null): boolean {
  return !!status && INFRA_UP_STATUSES.has(status.toUpperCase());
}

// Compact, accurate rollup of infra health: counts components that are actually
// up across warehouse + compute pool + service, e.g. "2/3 up". Avoids the
// misleading single-component status the sidebar previously showed.
function infraSummary(infra: InfraConfig | null): string | null {
  if (!infra) return null;
  const statuses = [
    infra.warehouse?.status,
    infra.compute_pool?.status,
    infra.service?.status,
  ].filter((s): s is string => !!s);
  if (statuses.length === 0) return null;
  const up = statuses.filter(s => INFRA_UP_STATUSES.has(s.toUpperCase())).length;
  return `${up} out of ${statuses.length} Active`;
}

function buildNavItems(data: SettingsData): { id: SectionId; label: string; icon: typeof Brain; summary: string }[] {
  const { llmConfig, rules, sources, tasks, infra, display, errors } = data;
  return [
    { id: 'ai', label: 'AI Adjudication', icon: Brain, summary: llmConfig ? `${llmConfig.LLM_REVIEW_MODEL.split('-').slice(-1)[0]} · Batch ${llmConfig.LLM_REVIEW_BATCH_SIZE}` : (errors.ai ? 'Unavailable' : 'Loading…') },
    { id: 'rules', label: 'Matching Rules', icon: Layers, summary: rules ? `${rules.length} rules · ${rules.filter(r => r.active).length} active` : (errors.rules ? 'Unavailable' : 'Loading…') },
    { id: 'sources', label: 'Source Priority', icon: Database, summary: sources ? `${sources.length} sources` : (errors.sources ? 'Unavailable' : 'Loading…') },
    { id: 'pipeline', label: 'Pipeline & Tasks', icon: Timer, summary: tasks ? `${tasks.length} tasks · ${tasks.filter(t => t.state === 'started').length} running` : (errors.pipeline ? 'Unavailable' : 'Loading…') },
    { id: 'infra', label: 'Infrastructure', icon: Server, summary: infraSummary(infra) ?? (errors.infra ? 'Unavailable' : 'Loading…') },
  ];
}

/* ──────────────────────────────────────────────
   Info Callout component
   ────────────────────────────────────────────── */
function InfoCallout({ children }: { children: React.ReactNode }) {
  return (
    <div className="stg-info-callout">
      <Info size={14} />
      <span>{children}</span>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Visual Range Indicator (0-1 scale)
   ────────────────────────────────────────────── */
function RangeIndicator({ low, high }: { low: number; high: number }) {
  const leftPct = Math.max(0, Math.min(100, low * 100));
  const rightPct = Math.max(0, Math.min(100, high * 100));
  return (
    <div className="stg-range-indicator">
      <span className="stg-range-label">0</span>
      <div className="stg-range-track">
        <div
          className="stg-range-zone"
          style={{ left: `${leftPct}%`, width: `${rightPct - leftPct}%` }}
        />
        <div className="stg-range-marker" style={{ left: `${leftPct}%` }} title={`Low: ${low}`} />
        <div className="stg-range-marker" style={{ left: `${rightPct}%` }} title={`High: ${high}`} />
      </div>
      <span className="stg-range-label">1</span>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Model options with descriptions
   ────────────────────────────────────────────── */
const MODEL_DESCRIPTIONS: Record<string, string> = {
  'claude-4-sonnet': 'High accuracy with strong reasoning. Best for nuanced identity resolution decisions.',
  'claude-4-haiku': 'Fast and cost-efficient. Good for high-volume batch processing with simpler pairs.',
  'mistral-large2': 'Open-weight alternative with strong multilingual support and competitive accuracy.',
  'llama3.3-70b': 'Open-source model with good general-purpose reasoning at lower cost.',
  'llama4-maverick': 'Latest generation open model with improved instruction following and reasoning.',
};

function buildModelOptions(models: string[]): RichSelectOption[] {
  return models.map(m => ({
    value: m,
    label: m,
    description: MODEL_DESCRIPTIONS[m],
  }));
}

/* ──────────────────────────────────────────────
   AI Adjudication Section
   ────────────────────────────────────────────── */
function AIAdjudicationSection({ initial, modelOptions }: { initial: LLMConfigState; modelOptions: string[] }) {
  const [config, setConfig] = useState(initial);

  return (
    <div className="stg-section-content">
      <InfoCallout>
        Pairs with ML scores between <strong>Score Low</strong> and <strong>Score High</strong> are routed to the AI model for adjudication. The model processes them in batches of <strong>{config.LLM_REVIEW_BATCH_SIZE}</strong>.
      </InfoCallout>

      <div className="stg-form-grid">
        <div className="stg-field">
          <label className="stg-field-label">Model</label>
          <RichSelect
            value={config.LLM_REVIEW_MODEL}
            options={buildModelOptions(modelOptions)}
            onChange={v => setConfig(c => ({ ...c, LLM_REVIEW_MODEL: v }))}
            icon={<Brain size={14} />}
          />
          <span className="stg-field-help">Cortex model used for pair adjudication</span>
        </div>

        <div className="stg-field">
          <label className="stg-field-label">Batch Size</label>
          <input
            type="number"
            className="stg-input"
            value={config.LLM_REVIEW_BATCH_SIZE}
            min={1}
            max={200}
            onChange={e => setConfig(c => ({ ...c, LLM_REVIEW_BATCH_SIZE: e.target.value }))}
          />
          <span className="stg-field-help">Max pairs processed per task invocation</span>
        </div>

        <div className="stg-field">
          <label className="stg-field-label">Score Low (review zone floor)</label>
          <input
            type="number"
            className="stg-input"
            value={config.LLM_REVIEW_SCORE_LOW}
            step="0.01"
            min={0}
            max={1}
            onChange={e => setConfig(c => ({ ...c, LLM_REVIEW_SCORE_LOW: e.target.value }))}
          />
          <span className="stg-field-help">Minimum ML score to enter LLM review zone</span>
        </div>

        <div className="stg-field">
          <label className="stg-field-label">Score High (review zone ceiling)</label>
          <input
            type="number"
            className="stg-input"
            value={config.LLM_REVIEW_SCORE_HIGH}
            step="0.01"
            min={0}
            max={1}
            onChange={e => setConfig(c => ({ ...c, LLM_REVIEW_SCORE_HIGH: e.target.value }))}
          />
          <span className="stg-field-help">Maximum ML score for LLM review zone</span>
        </div>
      </div>

      <div className="stg-range-section">
        <label className="stg-field-label">Review Zone Visualization</label>
        <RangeIndicator low={parseFloat(config.LLM_REVIEW_SCORE_LOW) || 0} high={parseFloat(config.LLM_REVIEW_SCORE_HIGH) || 1} />
        <span className="stg-field-help">Scores in the highlighted zone are sent to AI for review</span>
      </div>

      <div className="stg-save-footer">
        <span className="stg-save-status">All changes saved</span>
        <div className="stg-save-actions">
          <button type="button" className="stg-btn stg-btn--ghost" onClick={() => setConfig(initial)}>Reset</button>
          <button type="button" className="stg-btn stg-btn--primary">Save Changes</button>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Rule Type options for RichSelect
   ────────────────────────────────────────────── */
const RULE_TYPE_OPTIONS: RichSelectOption[] = [
  { value: 'DETERMINISTIC', label: 'Deterministic', description: 'Exact field matching with a fixed confidence score.' },
  { value: 'VECTOR', label: 'Vector', description: 'Embedding-based similarity using cosine distance and a fuzzy threshold.' },
  { value: 'PROBABILISTIC', label: 'Probabilistic', description: 'Statistical scoring with weighted evidence accumulation.' },
];

/* ──────────────────────────────────────────────
   Rule Detail Card (right panel)
   ────────────────────────────────────────────── */
function RuleDetailCard({ rule, onCancel }: { rule: RuleRow; onCancel: () => void }) {
  const [ruleType, setRuleType] = useState(rule.type);
  const [fuzzyAlgo, setFuzzyAlgo] = useState(rule.fuzzy_algorithm ?? 'NONE');
  const showFuzzy = fuzzyAlgo !== 'NONE' && fuzzyAlgo !== '';

  return (
    <div className="stg-rules-detail-card">
      <div className="stg-rules-detail-header">
        <h3>{rule.name}</h3>
        <span className="stg-rules-detail-id">{rule.id}</span>
      </div>

      <div className="stg-rules-detail-fields">
        <div className="stg-field">
          <label className="stg-field-label">Rule Name</label>
          <input type="text" className="stg-input" defaultValue={rule.name} />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Type</label>
          <RichSelect
            value={ruleType}
            options={RULE_TYPE_OPTIONS}
            onChange={setRuleType}
            icon={<Layers size={14} />}
          />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Base Score</label>
          <input type="number" className="stg-input" defaultValue={rule.base_score} step="0.01" min={0} max={1} />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Priority</label>
          <input type="number" className="stg-input" defaultValue={rule.priority ?? ''} min={1} />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Anchor Field</label>
          <input type="text" className="stg-input" defaultValue={rule.anchor_field ?? ''} />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Exact Match Fields</label>
          <input type="text" className="stg-input" defaultValue={rule.exact_match_fields ?? ''} placeholder="e.g. EMAIL, PHONE_E164, LAST_NAME_STD" />
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Fuzzy Algorithm</label>
          <input type="text" className="stg-input" defaultValue={fuzzyAlgo} onChange={e => setFuzzyAlgo(e.target.value)} />
        </div>
        {showFuzzy && (
          <>
            <div className="stg-field">
              <label className="stg-field-label">Fuzzy Match Field</label>
              <input type="text" className="stg-input" defaultValue={rule.fuzzy_match_field ?? ''} />
            </div>
            <div className="stg-field">
              <label className="stg-field-label">Fuzzy Threshold</label>
              <input type="number" className="stg-input" defaultValue={rule.fuzzy_threshold ?? 0.8} step="0.01" min={0} max={1} />
            </div>
            <div className="stg-field">
              <label className="stg-field-label">Use Fuzzy Score</label>
              <label className="stg-toggle">
                <input type="checkbox" defaultChecked={rule.use_fuzzy_score} />
                <span className="stg-toggle-track" />
              </label>
            </div>
          </>
        )}
        <div className="stg-field">
          <label className="stg-field-label">Cross-Source Required</label>
          <label className="stg-toggle">
            <input type="checkbox" defaultChecked={rule.require_cross_source} />
            <span className="stg-toggle-track" />
          </label>
        </div>
        <div className="stg-field">
          <label className="stg-field-label">Active</label>
          <label className="stg-toggle">
            <input type="checkbox" defaultChecked={rule.active} />
            <span className="stg-toggle-track" />
          </label>
        </div>
        <div className="stg-field stg-field--full">
          <label className="stg-field-label">Description</label>
          <textarea className="stg-textarea" rows={3} defaultValue={rule.description} />
        </div>
      </div>

      <div className="stg-save-footer">
        <div className="stg-save-actions">
          <button type="button" className="stg-btn stg-btn--ghost" onClick={onCancel}>Cancel</button>
          <button type="button" className="stg-btn stg-btn--primary">Save</button>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Matching Rules Section (Split Panel)
   ────────────────────────────────────────────── */
function MatchingRulesSection({ initial }: { initial: RuleRow[] }) {
  const [rules, setRules] = useState(initial);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selectedRule = rules.find(r => r.id === selectedId) || null;

  // Resizable split panel
  const splitRef = useRef<HTMLDivElement>(null);
  const [detailWidth, setDetailWidth] = useState(380);
  const dragging = useRef(false);

  const onMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const startX = e.clientX;
    const startWidth = detailWidth;
    const containerWidth = splitRef.current?.getBoundingClientRect().width ?? 1200;
    const maxRight = containerWidth - 400; // leave at least 400px for the left table

    const onMouseMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const delta = startX - ev.clientX;
      const newWidth = Math.min(Math.max(startWidth + delta, 380), Math.min(600, maxRight));
      setDetailWidth(newWidth);
    };
    const onMouseUp = () => {
      dragging.current = false;
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  };

  return (
    <div className="stg-section-content stg-section-content--split">
      <div className="stg-rules-split" ref={splitRef} style={{ gridTemplateColumns: `1fr auto ${detailWidth}px` }}>
        {/* Left: Table */}
        <div className="stg-rules-list">
          <div className="stg-rules-list-header">
            <span className="stg-rules-count">{rules.length} rules · {rules.filter(r => r.active).length} active</span>
          </div>
          <div className="stg-rules-table">
            <div className="stg-rules-table-header">
              <span className="stg-rules-col stg-rules-col--id">ID</span>
              <span className="stg-rules-col stg-rules-col--name">Rule Name</span>
              <span className="stg-rules-col stg-rules-col--type">Type</span>
              <span className="stg-rules-col stg-rules-col--score">Score</span>
              <span className="stg-rules-col stg-rules-col--active">Active</span>
            </div>
            <div className="stg-rules-table-body">
              {rules.map((rule, idx) => (
                <div
                  key={rule.id}
                  className={`stg-rules-row ${selectedId === rule.id ? 'stg-rules-row--selected' : ''}`}
                  onClick={() => setSelectedId(rule.id)}
                >
                  <span className="stg-rules-col stg-rules-col--id">{rule.id}</span>
                  <span className="stg-rules-col stg-rules-col--name">{rule.name}</span>
                  <span className="stg-rules-col stg-rules-col--type">
                    <span className={`stg-type-badge stg-type-badge--${rule.type.toLowerCase()}`}>{rule.type}</span>
                  </span>
                  <span className="stg-rules-col stg-rules-col--score">{rule.base_score.toFixed(2)}</span>
                  <span className="stg-rules-col stg-rules-col--active" onClick={e => e.stopPropagation()}>
                    <label className="stg-toggle">
                      <input
                        type="checkbox"
                        checked={rule.active}
                        onChange={() => {
                          const copy = [...rules];
                          copy[idx] = { ...copy[idx], active: !copy[idx].active };
                          setRules(copy);
                        }}
                      />
                      <span className="stg-toggle-track" />
                    </label>
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Resize Handle */}
        <div className="stg-rules-resize-handle" onMouseDown={onMouseDown} />

        {/* Right: Detail */}
        <div className="stg-rules-detail">
          {selectedRule ? (
            <RuleDetailCard key={selectedRule.id} rule={selectedRule} onCancel={() => setSelectedId(null)} />
          ) : (
            <div className="stg-rules-detail-empty">
              <Layers size={32} />
              <p>Select a rule to view details</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Source Priority Section
   ────────────────────────────────────────────── */
function SourcePrioritySection({ initial }: { initial: SourceConfig[] }) {
  const [sources, setSources] = useState(initial);

  const moveSource = (index: number, direction: -1 | 1) => {
    const newIdx = index + direction;
    if (newIdx < 0 || newIdx >= sources.length) return;
    const copy = [...sources];
    [copy[index], copy[newIdx]] = [copy[newIdx], copy[index]];
    setSources(copy.map((s, i) => ({ ...s, priority: i + 1 })));
  };

  return (
    <div className="stg-section-content">
      <InfoCallout>
        Sources are evaluated in priority order. When identifiers conflict, the higher-priority source wins. Use arrows to reorder.
      </InfoCallout>

      <div className="stg-sources-table">
        <div className="stg-sources-header">
          <span className="stg-sources-col stg-sources-col--grip"></span>
          <span className="stg-sources-col stg-sources-col--priority">#</span>
          <span className="stg-sources-col stg-sources-col--source">Source</span>
          <span className="stg-sources-col stg-sources-col--desc">Description</span>
          <span className="stg-sources-col stg-sources-col--pk">PK Column</span>
          <span className="stg-sources-col stg-sources-col--prefix">Prefix</span>
          <span className="stg-sources-col stg-sources-col--loc">Location</span>
        </div>
        {sources.map((s, idx) => (
          <div key={s.source} className="stg-sources-row">
            <span className="stg-sources-col stg-sources-col--grip">
              <div className="stg-reorder-btns">
                <button
                  type="button"
                  className="stg-reorder-btn"
                  disabled={idx === 0}
                  onClick={() => moveSource(idx, -1)}
                  title="Move up"
                >
                  <ArrowUp size={12} />
                </button>
                <button
                  type="button"
                  className="stg-reorder-btn"
                  disabled={idx === sources.length - 1}
                  onClick={() => moveSource(idx, 1)}
                  title="Move down"
                >
                  <ArrowDown size={12} />
                </button>
              </div>
            </span>
            <span className="stg-sources-col stg-sources-col--priority">{s.priority}</span>
            <span className="stg-sources-col stg-sources-col--source">{s.source}</span>
            <span className="stg-sources-col stg-sources-col--desc">{s.description}</span>
            <span className="stg-sources-col stg-sources-col--pk"><code>{s.pk_column}</code></span>
            <span className="stg-sources-col stg-sources-col--prefix"><code>{s.prefix}</code></span>
            <span className="stg-sources-col stg-sources-col--loc">
              {s.has_location ? <CheckCircle2 size={14} className="stg-icon--success" /> : <XCircle size={14} className="stg-icon--muted" />}
            </span>
          </div>
        ))}
      </div>

      <div className="stg-save-footer">
        <button type="button" className="stg-btn stg-btn--secondary">
          <Plus size={14} /> Add Source
        </button>
        <div className="stg-save-actions">
          <button type="button" className="stg-btn stg-btn--ghost" onClick={() => setSources(initial)}>Reset</button>
          <button type="button" className="stg-btn stg-btn--primary">Save Order</button>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Pipeline & Tasks Section
   ────────────────────────────────────────────── */
function PipelineSection({ tasks }: { tasks: PipelineTask[] }) {
  const [confirmTask, setConfirmTask] = useState<PipelineTask | null>(null);

  const handleConfirm = () => {
    // TODO: call backend to suspend/resume task
    setConfirmTask(null);
  };

  return (
    <div className="stg-section-content">
      <div className="stg-task-grid">
        {tasks.map(task => (
          <div key={task.task_name} className="stg-task-card">
            <div className="stg-task-card-header">
              <Timer size={16} />
              <h4>{task.display_name}</h4>
            </div>
            <div className="stg-task-card-body">
              <div className="stg-task-kv">
                <span className="stg-task-kv-label">Path</span>
                <span className="stg-task-kv-value stg-task-kv-value--mono">{task.task_name}</span>
              </div>
              <div className="stg-task-kv">
                <span className="stg-task-kv-label">Schedule</span>
                <span className="stg-task-kv-value">{task.schedule}</span>
              </div>
              <div className="stg-task-kv">
                <span className="stg-task-kv-label">State</span>
                <span className="stg-task-kv-value">
                  <span className={`stg-status-dot stg-status-dot--${task.state === 'started' ? 'active' : 'inactive'}`} />
                  {task.state.charAt(0).toUpperCase() + task.state.slice(1)}
                </span>
              </div>
              <div className="stg-task-kv">
                <span className="stg-task-kv-label">Warehouse</span>
                <span className="stg-task-kv-value"><span className="stg-badge-mono">{task.warehouse}</span></span>
              </div>
            </div>
            <div className="stg-task-card-footer">
              <button type="button" className={`stg-btn stg-btn--small ${task.state === 'started' ? 'stg-btn--danger' : 'stg-btn--ghost'}`} onClick={() => setConfirmTask(task)}>
                {task.state === 'started' ? 'Suspend' : 'Resume'}
              </button>
            </div>
          </div>
        ))}
      </div>

      {confirmTask && (
        <div className="modal-overlay" onClick={() => setConfirmTask(null)}>
          <div className="stg-confirm-dialog" onClick={e => e.stopPropagation()}>
            <div className="stg-confirm-icon">
              <AlertTriangle size={24} />
            </div>
            <h3>{confirmTask.state === 'started' ? 'Suspend Task' : 'Resume Task'}</h3>
            <p>
              Are you sure you want to {confirmTask.state === 'started' ? 'suspend' : 'resume'}{' '}
              <strong>{confirmTask.display_name}</strong>?
              {confirmTask.state === 'started' && ' This will stop the scheduled pipeline from running.'}
            </p>
            <div className="stg-confirm-actions">
              <button type="button" className="stg-btn stg-btn--ghost" onClick={() => setConfirmTask(null)}>Cancel</button>
              <button type="button" className={`stg-btn ${confirmTask.state === 'started' ? 'stg-btn--danger' : 'stg-btn--primary'}`} onClick={handleConfirm}>
                {confirmTask.state === 'started' ? 'Suspend' : 'Resume'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

/* ──────────────────────────────────────────────
   Infrastructure Section
   ────────────────────────────────────────────── */
function InfrastructureSection({ infra }: { infra: InfraConfig }) {
  return (
    <div className="stg-section-content">
      <div className="stg-infra-grid">
        {/* Warehouse */}
        {infra.warehouse ? (
          <div className="stg-infra-card">
            <div className="stg-infra-card-header">
              <Server size={16} />
              <h4>Warehouse</h4>
            </div>
            <div className="stg-infra-card-body">
              <div className="stg-task-kv"><span className="stg-task-kv-label">Name</span><span className="stg-task-kv-value stg-task-kv-value--mono">{infra.warehouse.name}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Size</span><span className="stg-task-kv-value">{infra.warehouse.size}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Auto-suspend</span><span className="stg-task-kv-value">{infra.warehouse.auto_suspend}s</span></div>
            </div>
            <div className={`stg-infra-card-status stg-infra-card-status--${isInfraUp(infra.warehouse.status) ? 'active' : 'suspended'}`}>
              <span className={`stg-status-dot stg-status-dot--${isInfraUp(infra.warehouse.status) ? 'active' : 'suspended'}`} />{infra.warehouse.status}
            </div>
          </div>
        ) : (
          <div className="stg-infra-card stg-infra-card--empty">
            <Server size={24} />
            <p>Warehouse not found</p>
          </div>
        )}

        {/* Compute Pool */}
        {infra.compute_pool ? (
          <div className="stg-infra-card">
            <div className="stg-infra-card-header">
              <Cpu size={16} />
              <h4>Compute Pool</h4>
            </div>
            <div className="stg-infra-card-body">
              <div className="stg-task-kv"><span className="stg-task-kv-label">Name</span><span className="stg-task-kv-value stg-task-kv-value--mono">{infra.compute_pool.name}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Instance</span><span className="stg-task-kv-value">{infra.compute_pool.instance_family}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Nodes</span><span className="stg-task-kv-value">{infra.compute_pool.min_nodes}–{infra.compute_pool.max_nodes}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Auto-suspend</span><span className="stg-task-kv-value">{infra.compute_pool.auto_suspend}s</span></div>
            </div>
            <div className={`stg-infra-card-status stg-infra-card-status--${isInfraUp(infra.compute_pool.status) ? 'active' : 'suspended'}`}>
              <span className={`stg-status-dot stg-status-dot--${isInfraUp(infra.compute_pool.status) ? 'active' : 'suspended'}`} />{infra.compute_pool.status}
            </div>
          </div>
        ) : (
          <div className="stg-infra-card stg-infra-card--empty">
            <Cpu size={24} />
            <p>No compute pool configured</p>
          </div>
        )}

        {/* SPCS Service */}
        {infra.service ? (
          <div className="stg-infra-card">
            <div className="stg-infra-card-header">
              <Container size={16} />
              <h4>SPCS Service</h4>
            </div>
            <div className="stg-infra-card-body">
              <div className="stg-task-kv"><span className="stg-task-kv-label">Name</span><span className="stg-task-kv-value stg-task-kv-value--mono">{infra.service.name}</span></div>
              <div className="stg-task-kv"><span className="stg-task-kv-label">Instances</span><span className="stg-task-kv-value">{infra.service.min_instances}–{infra.service.max_instances}</span></div>
            </div>
            <div className={`stg-infra-card-status stg-infra-card-status--${isInfraUp(infra.service.status) ? 'active' : 'suspended'}`}>
              <span className={`stg-status-dot stg-status-dot--${isInfraUp(infra.service.status) ? 'active' : 'suspended'}`} />{infra.service.status}
            </div>
          </div>
        ) : (
          <div className="stg-infra-card stg-infra-card--empty">
            <Container size={24} />
            <p>No SPCS service deployed</p>
          </div>
        )}
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Display & Thresholds Section
   ────────────────────────────────────────────── */
function DisplaySection({ initial }: { initial: DisplayConfig }) {
  const [display, setDisplay] = useState(initial);
  const [hasChanges, setHasChanges] = useState(false);

  const update = (key: keyof DisplayConfig, value: number) => {
    setDisplay(d => ({ ...d, [key]: value }));
    setHasChanges(true);
  };

  const fields: { key: keyof DisplayConfig; label: string; category: 'confidence' | 'health' | 'ui'; step?: number; min: number; max: number }[] = [
    { key: 'confidence_high', label: 'Confidence High Threshold', category: 'confidence', step: 0.05, min: 0, max: 1 },
    { key: 'confidence_medium', label: 'Confidence Medium Threshold', category: 'confidence', step: 0.05, min: 0, max: 1 },
    { key: 'health_low_confidence_weight', label: 'Health: Low-Confidence Weight', category: 'health', min: 0, max: 100 },
    { key: 'health_vendor_tension_weight', label: 'Health: Vendor Tension Weight', category: 'health', min: 0, max: 100 },
    { key: 'health_probabilistic_weight', label: 'Health: Probabilistic Weight', category: 'health', min: 0, max: 100 },
    { key: 'page_size_default', label: 'Identity List Page Size', category: 'ui', min: 10, max: 100 },
  ];

  return (
    <div className="stg-section-content">
      {hasChanges && (
        <div className="stg-staged-banner">
          <Info size={14} />
          Changes are staged — save to apply
        </div>
      )}

      <div className="stg-display-grid">
        {fields.map(f => (
          <div key={f.key} className="stg-display-field">
            <span className={`stg-category-dot stg-category-dot--${f.category}`} />
            <div className="stg-field">
              <label className="stg-field-label">{f.label}</label>
              <input
                type="number"
                className="stg-input"
                value={display[f.key]}
                step={f.step || 1}
                min={f.min}
                max={f.max}
                onChange={e => update(f.key, parseFloat(e.target.value))}
              />
            </div>
          </div>
        ))}
      </div>

      <div className="stg-save-footer">
        <span className="stg-save-status">{hasChanges ? '' : 'All changes saved'}</span>
        <div className="stg-save-actions">
          <button type="button" className="stg-btn stg-btn--ghost" onClick={() => { setDisplay(initial); setHasChanges(false); }}>Reset</button>
          <button type="button" className="stg-btn stg-btn--primary" onClick={() => setHasChanges(false)}>Save Changes</button>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────────────────────────────────
   Settings Page (main)
   ────────────────────────────────────────────── */
export default function Settings() {
  const [activeSection, setActiveSection] = useState<SectionId>('ai');
  const [data, setData] = useState<SettingsData>({
    llmConfig: null, availableModels: [], rules: null, sources: null,
    tasks: null, infra: null, display: null,
    loading: true, errors: {},
  });

  useEffect(() => {
    let cancelled = false;
    async function load() {
      const errors: Partial<Record<string, string>> = {};
      const [llmRes, rulesRes, sourcesRes, displayRes] = await Promise.allSettled([
        fetchLLMThresholds(),
        fetchMatchingRules(),
        fetchSources(),
        fetchDisplayConfig(),
      ]);

      if (cancelled) return;

      const llmConfig = llmRes.status === 'fulfilled' ? toLLMConfigState(llmRes.value) : null;
      const availableModels = llmRes.status === 'fulfilled' ? llmRes.value.available_models : [];
      if (llmRes.status === 'rejected') errors.ai = 'Failed to load';

      const rules = rulesRes.status === 'fulfilled' ? toRuleRows(rulesRes.value) : null;
      if (rulesRes.status === 'rejected') errors.rules = 'Failed to load';

      const sources = sourcesRes.status === 'fulfilled' ? sourcesRes.value : null;
      if (sourcesRes.status === 'rejected') errors.sources = 'Failed to load';

      const display = displayRes.status === 'fulfilled' ? displayRes.value : null;
      if (displayRes.status === 'rejected') errors.display = 'Failed to load';

      // Functional merge so background tasks/infra fetches (below) are not clobbered
      // if they resolve before this primary load completes.
      setData(prev => ({
        ...prev,
        llmConfig, availableModels, rules, sources, display,
        loading: false,
        errors: { ...prev.errors, ...errors },
      }));
    }
    load();

    // Load live pipeline + infrastructure state in the background so the sidebar
    // summaries reflect real values without requiring the user to open each tab.
    fetchPipelineTasks().then(tasks => {
      if (cancelled) return;
      setData(prev => ({ ...prev, tasks, errors: { ...prev.errors, pipeline: undefined } }));
    }).catch(() => {
      if (cancelled) return;
      setData(prev => ({ ...prev, errors: { ...prev.errors, pipeline: 'Failed to load' } }));
    });
    fetchInfrastructure().then(infra => {
      if (cancelled) return;
      setData(prev => ({ ...prev, infra, errors: { ...prev.errors, infra: undefined } }));
    }).catch(() => {
      if (cancelled) return;
      setData(prev => ({ ...prev, errors: { ...prev.errors, infra: 'Failed to load' } }));
    });

    return () => { cancelled = true; };
  }, []);

  // Re-fetch live state when switching to Pipeline or Infrastructure tabs
  useEffect(() => {
    if (activeSection === 'pipeline') {
      fetchPipelineTasks().then(tasks => {
        setData(prev => ({ ...prev, tasks, errors: { ...prev.errors, pipeline: undefined } }));
      }).catch(() => {
        setData(prev => ({ ...prev, errors: { ...prev.errors, pipeline: 'Failed to load' } }));
      });
    } else if (activeSection === 'infra') {
      fetchInfrastructure().then(infra => {
        setData(prev => ({ ...prev, infra, errors: { ...prev.errors, infra: undefined } }));
      }).catch(() => {
        setData(prev => ({ ...prev, errors: { ...prev.errors, infra: 'Failed to load' } }));
      });
    }
  }, [activeSection]);

  if (data.loading) {
    return <div className="loading">Loading settings…</div>;
  }

  const navItems = buildNavItems(data);

  const renderSection = () => {
    const err = data.errors[activeSection];
    if (err) {
      return (
        <div className="stg-section-content" style={{ alignItems: 'center', justifyContent: 'center', textAlign: 'center', gap: 12 }}>
          <XCircle size={32} style={{ color: 'var(--text-muted)' }} />
          <p style={{ color: 'var(--text-secondary)', fontSize: 14 }}>{err}</p>
          <button className="stg-btn stg-btn--secondary" onClick={() => window.location.reload()}>
            <RefreshCw size={14} /> Retry
          </button>
        </div>
      );
    }
    switch (activeSection) {
      case 'ai': return data.llmConfig ? <AIAdjudicationSection initial={data.llmConfig} modelOptions={data.availableModels} /> : null;
      case 'rules': return data.rules ? <MatchingRulesSection initial={data.rules} /> : null;
      case 'sources': return data.sources ? <SourcePrioritySection initial={data.sources} /> : null;
      case 'pipeline': return data.tasks ? <PipelineSection tasks={data.tasks} /> : null;
      case 'infra': return data.infra ? <InfrastructureSection infra={data.infra} /> : null;
      case 'display': return data.display ? <DisplaySection initial={data.display} /> : null;
    }
  };

  const activeMeta = navItems.find(n => n.id === activeSection)!;

  return (
    <div className="settings-page">
      <div className="stg-header">
        <h1>Settings</h1>
        <p className="page-subtitle">Configure matching rules, AI adjudication, pipeline parameters, and display preferences.</p>
      </div>

      <div className="stg-layout">
        {/* Sidebar nav */}
        <nav className="stg-nav">
          {navItems.map(item => {
            const Icon = item.icon;
            const isActive = activeSection === item.id;
            return (
              <button
                key={item.id}
                type="button"
                className={`stg-nav-item ${isActive ? 'stg-nav-item--active' : ''}`}
                onClick={() => setActiveSection(item.id)}
              >
                <Icon size={16} className="stg-nav-icon" />
                <div className="stg-nav-text">
                  <span className="stg-nav-label">{item.label}</span>
                  <span className="stg-nav-summary">{item.summary}</span>
                </div>
              </button>
            );
          })}
        </nav>

        {/* Content area */}
        <div className="stg-content">
          <div className="stg-content-header">
            <activeMeta.icon size={20} />
            <h2>{activeMeta.label}</h2>
          </div>
          {renderSection()}
        </div>
      </div>
    </div>
  );
}
