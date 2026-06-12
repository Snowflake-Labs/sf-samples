import { useCallback, useEffect, useRef, useState } from 'react';

interface MLFeatures {
  email_eq: boolean | null;
  phone_eq: boolean | null;
  email_handle_eq: boolean | null;
  phone_last7_eq: boolean | null;
  loyalty_id_eq: boolean | null;
  device_id_eq: boolean | null;
  jw_first_name: number | null;
  jw_last_name: number | null;
  jw_street: number | null;
  postal_eq: boolean | null;
  state_eq: boolean | null;
  nickname_first_eq: boolean | null;
}

interface RecordAttributes {
  email: string | null;
  phone: string | null;
  first_name: string | null;
  last_name: string | null;
  street: string | null;
  city: string | null;
  state: string | null;
  postal_code: string | null;
  loyalty_id: string | null;
  device_id: string | null;
}

interface RecordAttributesResponse {
  record_a: RecordAttributes;
  record_b: RecordAttributes;
  source_id_a: string;
  source_id_b: string;
  source_type_a: string;
  source_type_b: string;
}

interface LLMReview {
  review_id: string;
  pair_id: string;
  cluster_id_a: string;
  cluster_id_b: string;
  source_type_a: string | null;
  source_type_b: string | null;
  partner_cluster_id: string | null;
  ml_score: number | null;
  ml_tier: string | null;
  llm_decision: string | null;
  llm_confidence: number | null;
  llm_reasoning: string | null;
  llm_raw_output: string | null;
  llm_parse_error: string | null;
  status: string;
  created_at: string | null;
  model_used: string | null;
  features: MLFeatures | null;
}


const DECISION_LABELS: Record<string, string> = {
  MERGE_PERSON: 'Merge Person',
  HOUSEHOLD_LINK: 'Household Link',
  NO_MATCH: 'No Match',
};

const STATUS_OPTIONS = ['ALL', 'PENDING', 'REVIEWED', 'ACCEPTED', 'REJECTED'] as const;
const DECISION_OPTIONS = ['ALL', 'MERGE_PERSON', 'HOUSEHOLD_LINK', 'NO_MATCH'] as const;

export default function AIReview({ onNavigateToIdentity }: { onNavigateToIdentity?: (id: string) => void }) {
  const [reviews, setReviews] = useState<LLMReview[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<string>('ALL');
  const [decisionFilter, setDecisionFilter] = useState<string>('ALL');
  const [mlScoreMin, setMlScoreMin] = useState<string>('');
  const [mlScoreMax, setMlScoreMax] = useState<string>('');
  const [aiScoreMin, setAiScoreMin] = useState<string>('');
  const [aiScoreMax, setAiScoreMax] = useState<string>('');
  const [searchInput, setSearchInput] = useState<string>('');
  const [search, setSearch] = useState<string>('');
  const [stats, setStats] = useState<{ total?: number; unfiltered_total?: number; filtered_total?: number; PENDING?: number; REVIEWED?: number; ACCEPTED?: number; REJECTED?: number; score_ranges?: { ml_min: number; ml_max: number; ai_min: number; ai_max: number }; pending_ai_review?: number }>({});
  const searchTimer = useRef<ReturnType<typeof setTimeout>>(null);
  const scoreTimer = useRef<ReturnType<typeof setTimeout>>(null);

  // Debounced score values — sliders update instantly for visual feedback,
  // but API calls only fire after user stops dragging (300ms)
  const [debouncedMlMin, setDebouncedMlMin] = useState<string>(mlScoreMin);
  const [debouncedMlMax, setDebouncedMlMax] = useState<string>(mlScoreMax);
  const [debouncedAiMin, setDebouncedAiMin] = useState<string>(aiScoreMin);
  const [debouncedAiMax, setDebouncedAiMax] = useState<string>(aiScoreMax);

  useEffect(() => {
    if (scoreTimer.current) clearTimeout(scoreTimer.current);
    scoreTimer.current = setTimeout(() => {
      setDebouncedMlMin(mlScoreMin);
      setDebouncedMlMax(mlScoreMax);
      setDebouncedAiMin(aiScoreMin);
      setDebouncedAiMax(aiScoreMax);
    }, 300);
    return () => { if (scoreTimer.current) clearTimeout(scoreTimer.current); };
  }, [mlScoreMin, mlScoreMax, aiScoreMin, aiScoreMax]);

  // Debounce search input (400ms)
  useEffect(() => {
    if (searchTimer.current) clearTimeout(searchTimer.current);
    searchTimer.current = setTimeout(() => setSearch(searchInput), 400);
    return () => { if (searchTimer.current) clearTimeout(searchTimer.current); };
  }, [searchInput]);

  const fetchStats = useCallback(async () => {
    try {
      const params = new URLSearchParams();
      if (statusFilter !== 'ALL') params.set('status', statusFilter);
      if (decisionFilter !== 'ALL') params.set('decision', decisionFilter);
      if (search.trim()) params.set('search', search.trim());
      if (debouncedMlMin) params.set('ml_score_min', String(Number(debouncedMlMin) / 100));
      if (debouncedMlMax) params.set('ml_score_max', String(Number(debouncedMlMax) / 100));
      if (debouncedAiMin) params.set('ai_score_min', String(Number(debouncedAiMin) / 100));
      if (debouncedAiMax) params.set('ai_score_max', String(Number(debouncedAiMax) / 100));
      const qs = params.toString();
      const resp = await fetch(`/api/llm-reviews/stats${qs ? `?${qs}` : ''}`);
      if (resp.ok) setStats(await resp.json());
    } catch { /* ignore */ }
  }, [statusFilter, decisionFilter, search, debouncedMlMin, debouncedMlMax, debouncedAiMin, debouncedAiMax]);

  const fetchReviews = useCallback(async () => {
    setLoading(true);
    try {
      const params = new URLSearchParams();
      if (statusFilter !== 'ALL') params.set('status', statusFilter);
      if (decisionFilter !== 'ALL') params.set('decision', decisionFilter);
      if (debouncedMlMin) params.set('ml_score_min', String(Number(debouncedMlMin) / 100));
      if (debouncedMlMax) params.set('ml_score_max', String(Number(debouncedMlMax) / 100));
      if (debouncedAiMin) params.set('ai_score_min', String(Number(debouncedAiMin) / 100));
      if (debouncedAiMax) params.set('ai_score_max', String(Number(debouncedAiMax) / 100));
      if (search.trim()) params.set('search', search.trim());
      const qs = params.toString();
      const resp = await fetch(`/api/llm-reviews${qs ? `?${qs}` : ''}`);
      if (resp.ok) {
        const data: LLMReview[] = await resp.json();
        setReviews(data);
        if (data.length > 0 && !data.find(r => r.review_id === selectedId)) {
          setSelectedId(data[0].review_id);
        }
      }
    } catch { /* ignore */ }
    setLoading(false);
  }, [statusFilter, decisionFilter, debouncedMlMin, debouncedMlMax, debouncedAiMin, debouncedAiMax, search, selectedId]);

  useEffect(() => { fetchStats(); }, [statusFilter, decisionFilter, search, debouncedMlMin, debouncedMlMax, debouncedAiMin, debouncedAiMax]); // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { fetchReviews(); }, [statusFilter, decisionFilter, debouncedMlMin, debouncedMlMax, debouncedAiMin, debouncedAiMax, search]); // eslint-disable-line react-hooks/exhaustive-deps

  const selected = reviews.find(r => r.review_id === selectedId) ?? null;

  const [recordAttrs, setRecordAttrs] = useState<RecordAttributesResponse | null>(null);
  const [recordAttrsLoading, setRecordAttrsLoading] = useState(false);

  useEffect(() => {
    if (!selectedId) { setRecordAttrs(null); return; }
    let cancelled = false;
    setRecordAttrsLoading(true);
    fetch(`/api/llm-reviews/${selectedId}/record-attributes`)
      .then(r => r.ok ? r.json() : null)
      .then(data => { if (!cancelled) setRecordAttrs(data); })
      .catch(() => { if (!cancelled) setRecordAttrs(null); })
      .finally(() => { if (!cancelled) setRecordAttrsLoading(false); });
    return () => { cancelled = true; };
  }, [selectedId]);

  const handleAction = async (reviewId: string, action: 'accept' | 'reject') => {
    try {
      await fetch(`/api/llm-reviews/${reviewId}/${action}`, { method: 'POST' });
      fetchStats();
      fetchReviews();
    } catch { /* ignore */ }
  };

  // KPI counts from server stats endpoint (always unfiltered)
  const total = stats.total ?? 0;
  const unfilteredTotal = stats.unfiltered_total ?? total;
  const filteredTotal = stats.filtered_total ?? total;
  const pending = stats.PENDING ?? 0;
  const reviewed = stats.REVIEWED ?? 0;
  const accepted = stats.ACCEPTED ?? 0;
  const rejected = stats.REJECTED ?? 0;
  const hasActiveFilter = statusFilter !== 'ALL' || decisionFilter !== 'ALL' || !!debouncedMlMin || !!debouncedMlMax || !!debouncedAiMin || !!debouncedAiMax || !!search.trim();

  // Dynamic slider bounds from score_ranges (based on current non-score filters)
  const mlBoundMin = stats.score_ranges?.ml_min ?? 0;
  const mlBoundMax = stats.score_ranges?.ml_max ?? 100;
  const aiBoundMin = stats.score_ranges?.ai_min ?? 0;
  const aiBoundMax = stats.score_ranges?.ai_max ?? 100;

  const decisionClass = (decision: string | null) => {
    switch (decision) {
      case 'MERGE_PERSON': return 'cc-overview-pill--merge';
      case 'HOUSEHOLD_LINK': return 'cc-overview-pill--household';
      case 'NO_MATCH': return 'cc-overview-pill--down';
      default: return 'cc-overview-pill--candidate';
    }
  };

  const statusClass = (status: string) => {
    switch (status) {
      case 'ACCEPTED': return 'cc-overview-pill--merge';
      case 'REJECTED': return 'cc-overview-pill--down';
      case 'REVIEWED': return 'cc-overview-pill--score';
      default: return 'cc-overview-pill--household';
    }
  };

  const statusLabel = (status: string) => status === 'REVIEWED' ? 'AI REVIEWED' : status;

  const shortId = (id: string) => id.length > 16 ? `${id.slice(0, 14)}…` : id;

  return (
    <div className="ai-review-page">
      <div className="ai-review-header">
        <h1>AI Review Queue</h1>
        <p className="page-subtitle">All cluster pairs evaluated by Cortex AI — review decisions and take action.</p>
        <div className="ai-review-info-bar">
          These pairs scored between <strong>55%–85%</strong> on the ML model — too uncertain for auto-merge but too strong to discard. They were routed to LLM adjudication for a second opinion.
        </div>
        {(stats.pending_ai_review ?? 0) > 0 && (
          <p className="ai-review-pending-note">
            {stats.pending_ai_review!.toLocaleString()} pairs are pending AI review and will be processed in upcoming task runs.
          </p>
        )}
      </div>

      {/* KPI chips */}
      <div className="ai-review-kpis">
        <button
          type="button"
          className={`ai-review-kpi ${statusFilter === 'ALL' ? 'ai-review-kpi--active' : ''}`}
          onClick={() => setStatusFilter('ALL')}
        >
          <span className="ai-review-kpi-value">
            {hasActiveFilter ? <>{filteredTotal.toLocaleString()} <span className="ai-review-kpi-of">of {unfilteredTotal.toLocaleString()}</span></> : total.toLocaleString()}
          </span>
          <span className="ai-review-kpi-label">Total</span>
        </button>
        <button
          type="button"
          className={`ai-review-kpi ${statusFilter === 'PENDING' ? 'ai-review-kpi--active' : ''}`}
          onClick={() => setStatusFilter('PENDING')}
        >
          <span className="ai-review-kpi-value">{pending}</span>
          <span className="ai-review-kpi-label">Pending</span>
        </button>
        <button
          type="button"
          className={`ai-review-kpi ${statusFilter === 'REVIEWED' ? 'ai-review-kpi--active' : ''}`}
          onClick={() => setStatusFilter('REVIEWED')}
        >
          <span className="ai-review-kpi-value">{reviewed}</span>
          <span className="ai-review-kpi-label">AI Reviewed</span>
        </button>
        <button
          type="button"
          className={`ai-review-kpi ${statusFilter === 'ACCEPTED' ? 'ai-review-kpi--active' : ''}`}
          onClick={() => setStatusFilter('ACCEPTED')}
        >
          <span className="ai-review-kpi-value">{accepted}</span>
          <span className="ai-review-kpi-label">Accepted</span>
        </button>
        <button
          type="button"
          className={`ai-review-kpi ${statusFilter === 'REJECTED' ? 'ai-review-kpi--active' : ''}`}
          onClick={() => setStatusFilter('REJECTED')}
        >
          <span className="ai-review-kpi-value">{rejected}</span>
          <span className="ai-review-kpi-label">Rejected</span>
        </button>
      </div>

      {/* Filters */}
      <div className="ai-review-filters">
        <label>
          AI Status:
          <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)}>
            {STATUS_OPTIONS.map(s => <option key={s} value={s}>{s === 'ALL' ? 'All Statuses' : s === 'REVIEWED' ? 'AI REVIEWED' : s}</option>)}
          </select>
        </label>
        <label>
          ML Status:
          <select value={decisionFilter} onChange={e => setDecisionFilter(e.target.value)}>
            {DECISION_OPTIONS.map(d => <option key={d} value={d}>{d === 'ALL' ? 'All Decisions' : DECISION_LABELS[d] || d}</option>)}
          </select>
        </label>
        <div className="ai-review-range-group">
          <span className="ai-review-slider-label">ML: {mlScoreMin || mlBoundMin}–{mlScoreMax || mlBoundMax}%</span>
          <div className="ai-review-dual-range">
            <div className="ai-review-dual-range-track">
              <div className="ai-review-dual-range-fill" style={{ left: `${((Number(mlScoreMin || mlBoundMin) - mlBoundMin) / (mlBoundMax - mlBoundMin)) * 100}%`, right: `${((mlBoundMax - Number(mlScoreMax || mlBoundMax)) / (mlBoundMax - mlBoundMin)) * 100}%` }} />
            </div>
            <input type="range" min={mlBoundMin} max={mlBoundMax} value={mlScoreMin || mlBoundMin} onChange={e => { const v = Math.min(Number(e.target.value), Number(mlScoreMax || mlBoundMax) - 1); setMlScoreMin(v <= mlBoundMin ? '' : String(v)); }} />
            <input type="range" min={mlBoundMin} max={mlBoundMax} value={mlScoreMax || mlBoundMax} onChange={e => { const v = Math.max(Number(e.target.value), Number(mlScoreMin || mlBoundMin) + 1); setMlScoreMax(v >= mlBoundMax ? '' : String(v)); }} />
          </div>
        </div>
        <div className="ai-review-range-group">
          <span className="ai-review-slider-label">AI: {aiScoreMin || aiBoundMin}–{aiScoreMax || aiBoundMax}%</span>
          <div className="ai-review-dual-range">
            <div className="ai-review-dual-range-track">
              <div className="ai-review-dual-range-fill" style={{ left: `${((Number(aiScoreMin || aiBoundMin) - aiBoundMin) / (aiBoundMax - aiBoundMin)) * 100}%`, right: `${((aiBoundMax - Number(aiScoreMax || aiBoundMax)) / (aiBoundMax - aiBoundMin)) * 100}%` }} />
            </div>
            <input type="range" min={aiBoundMin} max={aiBoundMax} value={aiScoreMin || aiBoundMin} onChange={e => { const v = Math.min(Number(e.target.value), Number(aiScoreMax || aiBoundMax) - 1); setAiScoreMin(v <= aiBoundMin ? '' : String(v)); }} />
            <input type="range" min={aiBoundMin} max={aiBoundMax} value={aiScoreMax || aiBoundMax} onChange={e => { const v = Math.max(Number(e.target.value), Number(aiScoreMin || aiBoundMin) + 1); setAiScoreMax(v >= aiBoundMax ? '' : String(v)); }} />
          </div>
        </div>
        <label className="ai-review-search-label">
          <input type="text" placeholder="Search cluster ID…" value={searchInput} onChange={e => setSearchInput(e.target.value)} className="ai-review-search-input" />
        </label>
      </div>

      {/* Split panel */}
      <div className="ai-review-split">
        {/* Left panel - list */}
        <div className="ai-review-list">
          <div className="ai-review-showing">{loading ? 'Loading…' : `Showing ${reviews.length} of ${(stats.filtered_total ?? total).toLocaleString()}`}</div>
          {!loading && reviews.length === 0 && <div className="empty-hint">No reviews found.</div>}
          {reviews.map(r => (
            <button
              key={r.review_id}
              type="button"
              className={`ai-review-list-item ${r.review_id === selectedId ? 'ai-review-list-item--selected' : ''}`}
              onClick={() => setSelectedId(r.review_id)}
            >
              <div className="ai-review-list-item-top">
                <span className={`cc-overview-pill cc-overview-pill--tier ${decisionClass(r.llm_decision)}`}>
                  {DECISION_LABELS[r.llm_decision || ''] || r.llm_decision || '—'}
                </span>
                {r.ml_score != null && (
                  <span className="cc-overview-pill cc-overview-pill--score">
                    ML {(r.ml_score * 100).toFixed(1)}%
                  </span>
                )}
                <span className={`cc-overview-pill ${statusClass(r.status)}`}>
                  {statusLabel(r.status)}
                </span>
              </div>
              <div className="ai-review-list-item-ids mono">
                {shortId(r.cluster_id_a)} ↔ {shortId(r.cluster_id_b)}
              </div>
              {r.llm_confidence != null && (
                <div className="ai-review-list-item-conf">
                  AI Confidence: {(r.llm_confidence * 100).toFixed(0)}% {r.llm_decision ? (DECISION_LABELS[r.llm_decision] || r.llm_decision) : ''}
                </div>
              )}
            </button>
          ))}
        </div>

        {/* Right panel - detail */}
        <div className="ai-review-detail">
          {!selected ? (
            <div className="ai-review-detail-empty">
              <p>Select a review from the list to see details.</p>
            </div>
          ) : (
            <>
              <div className="ai-review-detail-header">
                <h2>Pair Detail</h2>
                <span className={`cc-overview-pill ${statusClass(selected.status)}`}>
                  {statusLabel(selected.status)}
                </span>
              </div>

              <div className="ai-review-detail-clusters">
                <button
                  type="button"
                  className="ai-review-cluster-link mono"
                  onClick={() => onNavigateToIdentity?.(selected.cluster_id_a)}
                  title="View identity detail"
                >
                  {selected.cluster_id_a}
                </button>
                <span className="ai-review-detail-arrow">↔</span>
                <button
                  type="button"
                  className="ai-review-cluster-link mono"
                  onClick={() => onNavigateToIdentity?.(selected.cluster_id_b)}
                  title="View identity detail"
                >
                  {selected.cluster_id_b}
                </button>
              </div>

              <div className="ai-review-detail-pills">
                {selected.llm_decision && (
                  <span className={`cc-overview-pill cc-overview-pill--tier ${decisionClass(selected.llm_decision)}`}>
                    {DECISION_LABELS[selected.llm_decision] || selected.llm_decision}
                  </span>
                )}
                {selected.ml_score != null && (
                  <span className="cc-overview-pill cc-overview-pill--score">
                    ML {(selected.ml_score * 100).toFixed(1)}%
                  </span>
                )}
                {selected.llm_confidence != null && (
                  <span className="cc-overview-pill cc-overview-pill--ai">
                    AI {(selected.llm_confidence * 100).toFixed(0)}% {selected.llm_decision ? (DECISION_LABELS[selected.llm_decision] || selected.llm_decision) : ''}
                  </span>
                )}

              </div>

              {selected.llm_reasoning && (
                <div className="ai-review-detail-reasoning">
                  <h3>AI Reasoning</h3>
                  <p>{selected.llm_reasoning}</p>
                </div>
              )}

              {selected.features && (
                <div className="ai-review-detail-features">
                  <h3>Record Comparison</h3>
                  {recordAttrsLoading && <p className="ai-review-attrs-loading">Loading record values…</p>}
                  {!recordAttrsLoading && recordAttrs && (
                    <table className="ai-review-features-table">
                      <thead>
                        <tr>
                          <th>Feature</th>
                          <th>{selected.source_type_a ? selected.source_type_a.replace('_RAW', '').replace(/_/g, ' ') : 'Record A'}</th>
                          <th>{selected.source_type_b ? selected.source_type_b.replace('_RAW', '').replace(/_/g, ' ') : 'Record B'}</th>
                          <th>Match?</th>
                        </tr>
                      </thead>
                      <tbody>
                        {(recordAttrs.record_a.email || recordAttrs.record_b.email) && (
                          <tr>
                            <td>Email</td>
                            <td className="mono">{recordAttrs.record_a.email || '—'}</td>
                            <td className="mono">{recordAttrs.record_b.email || '—'}</td>
                            <td className={selected.features.email_eq ? 'feat-match' : selected.features.email_handle_eq ? 'feat-partial' : 'feat-miss'}>
                              {selected.features.email_eq ? 'Exact' : selected.features.email_handle_eq ? 'Handle match' : 'No'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.phone || recordAttrs.record_b.phone) && (
                          <tr>
                            <td>Phone</td>
                            <td className="mono">{recordAttrs.record_a.phone || '—'}</td>
                            <td className="mono">{recordAttrs.record_b.phone || '—'}</td>
                            <td className={selected.features.phone_eq ? 'feat-match' : selected.features.phone_last7_eq ? 'feat-partial' : 'feat-miss'}>
                              {selected.features.phone_eq ? 'Exact' : selected.features.phone_last7_eq ? 'Last-7 match' : 'No'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.first_name || recordAttrs.record_b.first_name) && (
                          <tr>
                            <td>First Name</td>
                            <td>{recordAttrs.record_a.first_name || '—'}</td>
                            <td>{recordAttrs.record_b.first_name || '—'}</td>
                            <td className={selected.features.jw_first_name != null ? (selected.features.jw_first_name >= 0.9 ? 'feat-match' : selected.features.jw_first_name >= 0.8 ? 'feat-partial' : 'feat-miss') : ''}>
                              {selected.features.jw_first_name != null ? `JW ${selected.features.jw_first_name.toFixed(2)}` : '—'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.last_name || recordAttrs.record_b.last_name) && (
                          <tr>
                            <td>Last Name</td>
                            <td>{recordAttrs.record_a.last_name || '—'}</td>
                            <td>{recordAttrs.record_b.last_name || '—'}</td>
                            <td className={selected.features.jw_last_name != null ? (selected.features.jw_last_name >= 0.9 ? 'feat-match' : selected.features.jw_last_name >= 0.8 ? 'feat-partial' : 'feat-miss') : ''}>
                              {selected.features.jw_last_name != null ? `JW ${selected.features.jw_last_name.toFixed(2)}` : '—'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.street || recordAttrs.record_b.street) && (
                          <tr>
                            <td>Street</td>
                            <td>{recordAttrs.record_a.street || '—'}</td>
                            <td>{recordAttrs.record_b.street || '—'}</td>
                            <td className={selected.features.jw_street != null ? (selected.features.jw_street >= 0.9 ? 'feat-match' : selected.features.jw_street >= 0.7 ? 'feat-partial' : 'feat-miss') : ''}>
                              {selected.features.jw_street != null ? `JW ${selected.features.jw_street.toFixed(2)}` : '—'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.state || recordAttrs.record_b.state) && (
                          <tr>
                            <td>State</td>
                            <td>{recordAttrs.record_a.state || '—'}</td>
                            <td>{recordAttrs.record_b.state || '—'}</td>
                            <td className={selected.features.state_eq ? 'feat-match' : 'feat-miss'}>
                              {selected.features.state_eq ? 'Yes' : 'No'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.postal_code || recordAttrs.record_b.postal_code) && (
                          <tr>
                            <td>Postal Code</td>
                            <td className="mono">{recordAttrs.record_a.postal_code || '—'}</td>
                            <td className="mono">{recordAttrs.record_b.postal_code || '—'}</td>
                            <td className={selected.features.postal_eq ? 'feat-match' : 'feat-miss'}>
                              {selected.features.postal_eq ? 'Yes' : 'No'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.loyalty_id || recordAttrs.record_b.loyalty_id) && (
                          <tr>
                            <td>Loyalty ID</td>
                            <td className="mono">{recordAttrs.record_a.loyalty_id || '—'}</td>
                            <td className="mono">{recordAttrs.record_b.loyalty_id || '—'}</td>
                            <td className={selected.features.loyalty_id_eq ? 'feat-match' : 'feat-miss'}>
                              {selected.features.loyalty_id_eq ? 'Yes' : 'No'}
                            </td>
                          </tr>
                        )}
                        {(recordAttrs.record_a.device_id || recordAttrs.record_b.device_id) && (
                          <tr>
                            <td>Device ID</td>
                            <td className="mono">{recordAttrs.record_a.device_id || '—'}</td>
                            <td className="mono">{recordAttrs.record_b.device_id || '—'}</td>
                            <td className={selected.features.device_id_eq ? 'feat-match' : 'feat-miss'}>
                              {selected.features.device_id_eq ? 'Yes' : 'No'}
                            </td>
                          </tr>
                        )}
                      </tbody>
                    </table>
                  )}
                </div>
              )}

              {selected.model_used && (
                <div className="ai-review-detail-meta">
                  <span className="ai-review-meta-item">Model: <strong>{selected.model_used}</strong></span>
                </div>
              )}

              <div className="ai-review-detail-meta">
                {selected.created_at && (
                  <span className="ai-review-meta-item">Created: <strong>{new Date(selected.created_at).toLocaleDateString()}</strong></span>
                )}
              </div>

              {(selected.status === 'PENDING' || selected.status === 'REVIEWED') && (
                <div className="ai-review-detail-actions">
                  <button
                    type="button"
                    className="ai-review-action-btn ai-review-action-btn--accept"
                    onClick={() => handleAction(selected.review_id, 'accept')}
                  >
                    ✓ Accept
                  </button>
                  <button
                    type="button"
                    className="ai-review-action-btn ai-review-action-btn--reject"
                    onClick={() => handleAction(selected.review_id, 'reject')}
                  >
                    ✗ Reject
                  </button>
                </div>
              )}

              {selected.status !== 'PENDING' && (
                <div className="ai-review-detail-resolved">
                  This review has been <strong>{selected.status === 'REVIEWED' ? 'AI reviewed' : selected.status.toLowerCase()}</strong>.
                  {selected.status === 'ACCEPTED' && (
                    <a
                      href={`/#/identity/${selected.cluster_id_a}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="ai-review-cluster-open-link"
                    >
                      View cluster →
                    </a>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
