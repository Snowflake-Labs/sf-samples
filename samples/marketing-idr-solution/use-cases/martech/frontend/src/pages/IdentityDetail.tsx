import { useEffect, useMemo, useState } from 'react';
import { ListOrdered, BookOpen, Info, Network, Link2, ShieldCheck, Target } from 'lucide-react';
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import {
  fetchIdentity,
  fetchIDRExplanation,
  fetchWeakLinks,
  fetchLLMReviews,
  acceptLLMReview,
  rejectLLMReview,
  demoLLMReview,
  fetchLineage,
  fetchAISummary,
  fetchSourceRecords,
  fetchConnectedClustersGraph,
  graphBundlePairToWeakLink,
  fetchMatchingRules,
  fetchIdentityHousehold,
  fetchHouseholdAIRisk,
} from '../api';
import type { SourceRecordDetail, WeakLink, LLMReview, LineageResponse, LineageEvent, MatchingRule, MatchExplanation, HouseholdInfo, AIRiskSignals } from '../api';
import { IDRForceGraph, MatchMatrix, IDRGraphLegend } from '../components/IDRForceGraph';
import IDRMatchQualityDonut from '../components/IDRMatchQualityDonut';
import IDRTopRulesTable from '../components/IDRTopRulesTable';
import IDRMatchDetailsTable from '../components/IDRMatchDetailsTable';
import { ConnectedClustersGraph } from '../components/ConnectedClustersGraph';
import { HouseholdGraph } from '../components/HouseholdGraph';
import CopyButton from '../components/CopyButton';
import SourceRecordModal from '../components/SourceRecordModal';
import CompareView from '../components/CompareView';
import { aggregateLinkedIdentifiersByValue } from '../utils/linkedIdentifiers';

type Tab = 'overview' | 'sources' | 'idr' | 'connected' | 'household' | 'lineage' | 'ai';

const TAB_LABELS: Record<Tab, string> = {
  overview: 'Overview',
  sources: 'Sources',
  idr: 'IDR Explanation',
  connected: 'Connected Clusters',
  household: 'Household',
  lineage: 'Resolution History',
  ai: 'AI Summary',
};

const TAB_ORDER: Tab[] = ['overview', 'sources', 'household', 'idr', 'lineage', 'ai'];

/** Placeholder token for UI areas not backed by API data yet. */
const LOREM = '{lorem}';

function CcLorem({ title }: { title?: string }) {
  return (
    <span className="cc-lorem" title={title}>
      {LOREM}
    </span>
  );
}

function weakLinkPartnerId(identityId: string, wl: WeakLink): string {
  return wl.cluster_id_a === identityId ? wl.cluster_id_b : wl.cluster_id_a;
}

/** DOM id for scrolling from the sidebar jump list to a candidate card. */
function connectedClusterCardDomId(pairId: string): string {
  return `cc-candidate-${pairId.replace(/[^a-zA-Z0-9_-]/g, '-')}`;
}

function scrollToConnectedClusterCard(pairId: string) {
  document.getElementById(connectedClusterCardDomId(pairId))?.scrollIntoView({
    behavior: 'smooth',
    block: 'start',
  });
}

function scorePriorityLabel(ml: number | null): string {
  if (ml == null) return 'Unscored';
  if (ml >= 0.85) return 'High priority';
  if (ml >= 0.5) return 'Medium priority';
  return 'Low priority';
}

function tierOf(wl: WeakLink): string {
  return wl.ml_tier || (wl.score_details?.tier as string) || 'CANDIDATE_ONLY';
}

function isStrongCandidate(wl: WeakLink): boolean {
  if (tierOf(wl) === 'MERGE_PERSON') return true;
  return wl.ml_score != null && wl.ml_score >= 0.8;
}

function needsReviewBucket(wl: WeakLink): boolean {
  if (isStrongCandidate(wl)) return false;
  return wl.status === 'PENDING' || wl.ml_score == null || (wl.ml_score != null && wl.ml_score < 0.8);
}

function computeConnectedKpis(links: WeakLink[]): {
  strong: number;
  needsReview: number;
  avgScoreDisplay: string;
  commonSignalDisplay: string;
} {
  const strong = links.filter(isStrongCandidate).length;
  const needsReview = links.filter(needsReviewBucket).length;
  const scored = links.map(w => w.ml_score).filter((s): s is number => s != null);
  const avgScoreDisplay =
    scored.length > 0 ? `${Math.round((scored.reduce((a, b) => a + b, 0) / scored.length) * 100)}%` : LOREM;
  const labelCounts = new Map<string, number>();
  for (const wl of links) {
    for (const row of wl.signal_rows ?? []) {
      const k = row.label.trim() || LOREM;
      labelCounts.set(k, (labelCounts.get(k) ?? 0) + 1);
    }
  }
  let commonSignalDisplay = LOREM;
  let best = 0;
  for (const [label, c] of labelCounts) {
    if (c > best) {
      best = c;
      commonSignalDisplay = label;
    }
  }
  if (best === 0 && links.length > 0) commonSignalDisplay = LOREM;
  return { strong, needsReview, avgScoreDisplay, commonSignalDisplay };
}

function formatWeakLinkLastSeen(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

function extractSignalBadges(d: Record<string, unknown> | null): { key: string; label: string; value: string; status: 'pos' | 'neg' | 'neutral' }[] {
  if (!d) return [];
  const badges: { key: string; label: string; value: string; status: 'pos' | 'neg' | 'neutral' }[] = [];
  const fpOverlap = Number(d.fingerprint_overlap ?? d.fingerprint_overlap_count ?? 0);
  if (fpOverlap > 0 || d.fingerprint_overlap != null) {
    badges.push({ key: 'fp', label: 'Shared FP', value: String(fpOverlap), status: fpOverlap > 0 ? 'pos' : 'neg' });
  }
  const geo = d.geo_overlap;
  if (geo != null) badges.push({ key: 'geo', label: 'Geo', value: Number(geo) >= 0.5 ? '✓' : '✗', status: Number(geo) >= 0.5 ? 'pos' : 'neg' });
  const ip = d.ip_overlap ?? d.ip_overlap_count;
  if (ip != null) badges.push({ key: 'ip', label: 'IP', value: Number(ip) >= 1 ? '✓' : '✗', status: Number(ip) >= 1 ? 'pos' : 'neg' });
  const session = d.session_overlap ?? d.session_overlap_count;
  if (session != null) badges.push({ key: 'session', label: 'Session', value: Number(session) >= 1 ? '✓' : '✗', status: Number(session) >= 1 ? 'pos' : 'neg' });
  const embed = Number(d.avg_embedding_similarity ?? d.avg_embedding_sim ?? d.max_embedding_similarity ?? d.max_embedding_sim ?? -1);
  if (embed >= 0) badges.push({ key: 'embed', label: 'Embed', value: embed.toFixed(2), status: embed >= 0.7 ? 'pos' : embed >= 0.4 ? 'neutral' : 'neg' });

  const rarity = Number(d.avg_fingerprint_rarity ?? d.min_fingerprint_rarity ?? -1);
  if (rarity >= 0) badges.push({ key: 'rarity', label: 'FP rarity', value: rarity.toFixed(2), status: rarity >= 0.7 ? 'pos' : rarity >= 0.4 ? 'neutral' : 'neg' });
  return badges;
}

function tierColorClass(tier: string): string {
  const t = tier.toUpperCase();
  if (t === 'MERGE_PERSON') return 'cc-tier-accent--merge';
  if (t.includes('REVIEW') || t === 'LLM_REVIEW' || t === 'HOUSEHOLD_LINK' || t === 'HOUSEHOLD LINK') return 'cc-tier-accent--review';
  if (t === 'SEPARATE_PERSON') return 'cc-tier-accent--separate';
  return 'cc-tier-accent--low';
}

function scoreArcColor(score: number | null): string {
  if (score == null) return 'var(--text-muted)';
  if (score >= 0.7) return '#16a34a';
  if (score >= 0.4) return '#f59e0b';
  return '#dc2626';
}

function ConnectedClusterCandidateCard({ wl, identityId, llmReview }: { wl: WeakLink; identityId: string; llmReview?: LLMReview }) {
  const [activeTab, setActiveTab] = useState<'evidence' | 'events' | 'history'>('evidence');
  const partner = weakLinkPartnerId(identityId, wl);
  const score = wl.ml_score;
  const scorePct = score == null ? null : `${(score * 100).toFixed(0)}%`;
  const tier = tierOf(wl);
  const tierPretty = tier.replace(/_/g, ' ');

  const prevScore = wl.score_details?.prev_score != null ? Number(wl.score_details.prev_score) : null;
  const scoreDelta = wl.score_details?.score_delta != null ? Number(wl.score_details.score_delta) : null;
  const hasTrend = prevScore != null && scoreDelta != null && !isNaN(prevScore) && !isNaN(scoreDelta);
  const trendUp = hasTrend && scoreDelta! > 0;
  const trendDown = hasTrend && scoreDelta! < 0;

  const signals = extractSignalBadges(wl.score_details);
  const arcDeg = score != null ? Math.min(score * 180, 180) : 0;
  const arcColor = scoreArcColor(score);

  const aiScore = llmReview?.llm_confidence ?? null;
  const aiPct = aiScore != null ? `${(aiScore * 100).toFixed(0)}%` : null;
  const aiArcDeg = aiScore != null ? Math.min(aiScore * 180, 180) : 0;
  const aiArcColor = scoreArcColor(aiScore);

  const whyFlagged =
    wl.english_summary?.trim() ||
    (wl.reasoning_lines && wl.reasoning_lines[0]?.trim()) ||
    null;

  return (
    <article className={`cc-candidate-card ${tierColorClass(tier)}`} id={connectedClusterCardDomId(wl.pair_id)}>
      <header className="cc-card-head">
        <div className="cc-card-title-row">
          <div className="cc-card-id-row">
            <h3 className="cc-card-id mono" title={partner}>{partner}</h3>
            <CopyButton value={partner} />
            <a href={`/identity/${partner}`} className="cc-partner-link" title="Open partner cluster">&#x2197;</a>
          </div>
          <div className="cc-card-head-trailing">
            <div className="cc-last-seen-inline" title={wl.last_seen ?? undefined}>
              <span className="cc-last-seen-value mono">{formatWeakLinkLastSeen(wl.last_seen)}</span>
            </div>
            <span className={`cc-chip cc-chip--tier cc-chip--tier-${tier.toLowerCase()}`}>{tierPretty}</span>
          </div>
        </div>
      </header>

      <div className="cc-card-body-v2">
        <div className="cc-gauges-row">
          <div className="cc-gauge-col">
            <div className="cc-score-gauge" style={{ '--arc-deg': `${arcDeg}deg`, '--arc-color': arcColor } as React.CSSProperties}>
              <svg viewBox="0 0 80 48" className="cc-gauge-svg">
                <path d="M 8 44 A 32 32 0 0 1 72 44" fill="none" stroke="var(--border)" strokeWidth="6" strokeLinecap="round" />
                <path d="M 8 44 A 32 32 0 0 1 72 44" fill="none" stroke={arcColor} strokeWidth="6" strokeLinecap="round"
                  strokeDasharray={`${(arcDeg / 180) * 100.5} 100.5`} />
              </svg>
              <span className="cc-gauge-value">{scorePct ?? '—'}</span>
            </div>
            {hasTrend && (
              <span className={`cc-gauge-trend ${trendUp ? 'cc-trend--up' : trendDown ? 'cc-trend--down' : ''}`}>
                {trendUp ? '\u25B2' : trendDown ? '\u25BC' : '\u25C6'}{' '}
                {scoreDelta! > 0 ? '+' : ''}{(scoreDelta! * 100).toFixed(1)}%
              </span>
            )}
            <span className="cc-gauge-label">ML Score</span>
          </div>

          {aiScore != null && (
            <div className="cc-gauge-col">
              <div className="cc-score-gauge" style={{ '--arc-deg': `${aiArcDeg}deg`, '--arc-color': aiArcColor } as React.CSSProperties}>
                <svg viewBox="0 0 80 48" className="cc-gauge-svg">
                  <path d="M 8 44 A 32 32 0 0 1 72 44" fill="none" stroke="var(--border)" strokeWidth="6" strokeLinecap="round" />
                  <path d="M 8 44 A 32 32 0 0 1 72 44" fill="none" stroke={aiArcColor} strokeWidth="6" strokeLinecap="round"
                    strokeDasharray={`${(aiArcDeg / 180) * 100.5} 100.5`} />
                </svg>
                <span className="cc-gauge-value">{aiPct}</span>
              </div>
              <span className="cc-gauge-label">AI Conf.</span>
            </div>
          )}
        </div>

        <div className="cc-signals-col">
          {signals.length > 0 && (
            <div className="cc-signal-badges">
              {signals.map(s => (
                <span key={s.key} className={`cc-badge cc-badge--${s.status}`} title={`${s.label}: ${s.value}`}>
                  <span className="cc-badge-dot" />
                  {s.label} <strong>{s.value}</strong>
                </span>
              ))}
            </div>
          )}

          {llmReview && llmReview.llm_decision && (
            <div className={`cc-ai-strip cc-ai-strip--${(llmReview.llm_decision || '').toLowerCase()}`}>
              <span className="cc-ai-strip-icon">&#x2728;</span>
              <span className={`cc-chip cc-chip--llm cc-chip--llm-${(llmReview.llm_decision || '').toLowerCase()}`}>
                {llmReview.llm_decision?.replace(/_/g, ' ')}
              </span>
              <span className="cc-ai-strip-conf">{((llmReview.llm_confidence ?? 0) * 100).toFixed(0)}%</span>
              {llmReview.llm_reasoning && (
                <span className="cc-ai-strip-reason">
                  {llmReview.llm_reasoning}
                </span>
              )}
              {llmReview.status === 'REVIEWED' && (
                <span className="cc-ai-strip-actions">
                  <button className="cc-btn cc-btn--accept" onClick={() => acceptLLMReview(llmReview.review_id)} title="Accept">&#x2713;</button>
                  <button className="cc-btn cc-btn--reject" onClick={() => rejectLLMReview(llmReview.review_id)} title="Reject">&#x2717;</button>
                </span>
              )}
            </div>
          )}

          {wl.structured_explanation ? (
            <details className="cc-ml-explain" open>
              <summary className="cc-why-summary-toggle">ML Explanation</summary>
              <div className="cc-ml-explain-body">
                <div className={`cc-ml-verdict cc-ml-verdict--${wl.structured_explanation.rec_level}`}>
                  {wl.structured_explanation.verdict}
                </div>

                {wl.structured_explanation.evidence.length > 0 && (
                  <div className="cc-ml-section">
                    <span className="cc-ml-section-label">Evidence</span>
                    <ul className="cc-ml-evidence">
                      {wl.structured_explanation.evidence.map((e, i) => <li key={i}>{e}</li>)}
                    </ul>
                  </div>
                )}

                {(wl.structured_explanation.supporting.length > 0 || wl.structured_explanation.opposing.length > 0) && (
                  <div className="cc-ml-section">
                    <span className="cc-ml-section-label">
                      Model Drivers
                      <span className="panel-hint-wrap" style={{ marginLeft: 4, verticalAlign: 'middle' }}>
                        <button type="button" className="panel-hint-btn" aria-label="What are model drivers?" aria-describedby="ml-drivers-tooltip">
                          <Info size={13} strokeWidth={2} aria-hidden />
                        </button>
                        <div id="ml-drivers-tooltip" className="panel-hint-tooltip panel-hint-tooltip--below" role="tooltip">
                          <p><strong>SHAP values</strong> show how much each feature pushed the score up (+) or down (-) from the baseline.</p>
                          <p>Larger bars mean stronger influence on this specific prediction.</p>
                        </div>
                      </span>
                    </span>
                    <div className="cc-ml-drivers">
                      {wl.structured_explanation.supporting.length > 0 && (
                        <div className="cc-ml-driver-col">
                          <span className="cc-ml-driver-heading cc-ml-driver-heading--pos">
                            Supporting
                            <span className="panel-hint-wrap" style={{ marginLeft: 4, verticalAlign: 'middle' }}>
                              <button type="button" className="panel-hint-btn" aria-label="What does Supporting mean?" aria-describedby="ml-supporting-tooltip">
                                <Info size={13} strokeWidth={2} aria-hidden />
                              </button>
                              <div id="ml-supporting-tooltip" className="panel-hint-tooltip panel-hint-tooltip--below" role="tooltip">
                                <p>Features that pushed the score <strong>UP</strong> toward a match.</p>
                                <p>Each bar shows the SHAP contribution — larger bars mean stronger evidence that these clusters belong to the same person.</p>
                              </div>
                            </span>
                          </span>
                          {wl.structured_explanation.supporting.map((d, i) => (
                            <div key={i} className="cc-ml-driver-row">
                              <span className="cc-ml-driver-name">{d.feature}</span>
                              <div className="cc-ml-driver-bar-wrap">
                                <div className="cc-ml-driver-bar cc-ml-driver-bar--pos" style={{ width: `${Math.min(Math.abs(d.shap) / 5 * 100, 100)}%` }} />
                              </div>
                              <span className="cc-ml-driver-val">+{Math.abs(d.shap).toFixed(1)}</span>
                              {d.explanation && <span className="cc-ml-driver-explanation">{d.explanation}</span>}
                            </div>
                          ))}
                        </div>
                      )}
                      {wl.structured_explanation.opposing.length > 0 && (
                        <div className="cc-ml-driver-col">
                          <span className="cc-ml-driver-heading cc-ml-driver-heading--neg">
                            Opposing
                            <span className="panel-hint-wrap" style={{ marginLeft: 4, verticalAlign: 'middle' }}>
                              <button type="button" className="panel-hint-btn" aria-label="What does Opposing mean?" aria-describedby="ml-opposing-tooltip">
                                <Info size={13} strokeWidth={2} aria-hidden />
                              </button>
                              <div id="ml-opposing-tooltip" className="panel-hint-tooltip panel-hint-tooltip--below" role="tooltip">
                                <p>Features that pulled the score <strong>DOWN</strong>.</p>
                                <p>Each bar shows the SHAP contribution — larger bars mean stronger evidence against the match (e.g. fingerprints too common, mismatched cluster sizes, too few overlapping sessions).</p>
                              </div>
                            </span>
                          </span>
                          {wl.structured_explanation.opposing.map((d, i) => (
                            <div key={i} className="cc-ml-driver-row">
                              <span className="cc-ml-driver-name">{d.feature}</span>
                              <div className="cc-ml-driver-bar-wrap">
                                <div className="cc-ml-driver-bar cc-ml-driver-bar--neg" style={{ width: `${Math.min(Math.abs(d.shap) / 5 * 100, 100)}%` }} />
                              </div>
                              <span className="cc-ml-driver-val">{d.shap.toFixed(1)}</span>
                              {d.explanation && <span className="cc-ml-driver-explanation">{d.explanation}</span>}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>
                )}

                {wl.structured_explanation.context && (
                  <p className="cc-ml-context">{wl.structured_explanation.context}</p>
                )}

                <div className={`cc-ml-recommendation cc-ml-recommendation--${wl.structured_explanation.rec_level}`}>
                  {wl.structured_explanation.recommendation}
                </div>
              </div>
            </details>
          ) : whyFlagged ? (
            <details className="cc-why-summary">
              <summary className="cc-why-summary-toggle">Detail</summary>
              <p className="cc-why-summary-text">{whyFlagged}</p>
            </details>
          ) : null}
        </div>
      </div>

      <div className="cc-card-detail">
        <nav className="cc-tab-bar">
          <button className={`cc-tab-btn ${activeTab === 'evidence' ? 'cc-tab-btn--active' : ''}`} onClick={() => setActiveTab('evidence')}>Evidence</button>
          <button className={`cc-tab-btn ${activeTab === 'events' ? 'cc-tab-btn--active' : ''}`} onClick={() => setActiveTab('events')}>Cluster Events</button>
          <button className={`cc-tab-btn ${activeTab === 'history' ? 'cc-tab-btn--active' : ''}`} onClick={() => setActiveTab('history')}>Score History</button>
        </nav>

        <div className="cc-tab-panel">
          {activeTab === 'evidence' && (
            <div className="cc-tab-content">
              <div className="cc-evidence-metrics">
                <div className="cc-ev-metric">
                  <span className="cc-ev-metric-val">{wl.fingerprint_overlap_count ?? 0}</span>
                  <span className="cc-ev-metric-label">Shared FPs</span>
                </div>
                {wl.score_snapshot_evidence?.ml_score != null && (
                  <div className="cc-ev-metric">
                    <span className="cc-ev-metric-val">{(wl.score_snapshot_evidence.ml_score * 100).toFixed(1)}%</span>
                    <span className="cc-ev-metric-label">Edge Score</span>
                  </div>
                )}
                {wl.fingerprint_match_evidence?.fingerprint_rarity != null && (
                  <div className="cc-ev-metric">
                    <span className="cc-ev-metric-val">{Number(wl.fingerprint_match_evidence.fingerprint_rarity).toFixed(2)}</span>
                    <span className="cc-ev-metric-label">FP Rarity</span>
                  </div>
                )}
                {wl.score_snapshot_evidence?.observed_at && (
                  <div className="cc-ev-metric">
                    <span className="cc-ev-metric-val mono">{wl.score_snapshot_evidence.observed_at.split('T')[0]}</span>
                    <span className="cc-ev-metric-label">Scored</span>
                  </div>
                )}
              </div>
              {wl.fingerprint_match_evidence && (
                <details className="cc-snapshot-details">
                  <summary>Source record IDs</summary>
                  <ul className="cc-pair-evidence-list">
                    <li><span className="cc-pe-label">Record A</span><span className="mono cc-pe-val">{wl.fingerprint_match_evidence.source_record_id_a ?? '—'}</span></li>
                    <li><span className="cc-pe-label">Record B</span><span className="mono cc-pe-val">{wl.fingerprint_match_evidence.source_record_id_b ?? '—'}</span></li>
                    {wl.fingerprint_match_evidence.device_fingerprint && (
                      <li><span className="cc-pe-label">Device FP</span><span className="mono cc-pe-val">{wl.fingerprint_match_evidence.device_fingerprint}</span></li>
                    )}
                  </ul>
                </details>
              )}
              {wl.score_snapshot_evidence?.score_snapshot_details &&
                Object.keys(wl.score_snapshot_evidence.score_snapshot_details).length > 0 && (
                  <details className="cc-snapshot-details">
                    <summary>Feature payload</summary>
                    <pre className="cc-snapshot-pre mono">{JSON.stringify(wl.score_snapshot_evidence.score_snapshot_details, null, 2)}</pre>
                  </details>
                )}
              {!wl.fingerprint_match_evidence && !wl.score_snapshot_evidence && (
                <p className="cc-tab-empty">No evidence data available for this pair.</p>
              )}
            </div>
          )}

          {activeTab === 'events' && (
            <div className="cc-tab-content">
              {((wl.cluster_a_events?.length ?? 0) > 0 || (wl.cluster_b_events?.length ?? 0) > 0) ? (
                <div className="cc-idr-events-grid">
                  <div>
                    <div className="cc-pe-label">Cluster A</div>
                    <div className="cc-cluster-events-id-row">
                      <span className="mono cc-cluster-events-id">{wl.cluster_id_a}</span>
                      <CopyButton value={wl.cluster_id_a} />
                    </div>
                    <p className="cc-idr-events-count">{(wl.cluster_a_events ?? []).length} source record(s)</p>
                    {(wl.cluster_a_events ?? []).length > 0 && (
                      <details className="cc-snapshot-details">
                        <summary>Show IDs</summary>
                        <ul className="cc-idr-events-ul mono">
                          {(wl.cluster_a_events ?? []).map(id => (<li key={id}>{id}</li>))}
                        </ul>
                      </details>
                    )}
                  </div>
                  <div>
                    <div className="cc-pe-label">Cluster B</div>
                    <div className="cc-cluster-events-id-row">
                      <span className="mono cc-cluster-events-id">{wl.cluster_id_b}</span>
                      <CopyButton value={wl.cluster_id_b} />
                    </div>
                    <p className="cc-idr-events-count">{(wl.cluster_b_events ?? []).length} source record(s)</p>
                    {(wl.cluster_b_events ?? []).length > 0 && (
                      <details className="cc-snapshot-details">
                        <summary>Show IDs</summary>
                        <ul className="cc-idr-events-ul mono">
                          {(wl.cluster_b_events ?? []).map(id => (<li key={id}>{id}</li>))}
                        </ul>
                      </details>
                    )}
                  </div>
                </div>
              ) : (
                <p className="cc-tab-empty">No cluster event data available.</p>
              )}
            </div>
          )}

          {activeTab === 'history' && (
            <div className="cc-tab-content">
              <WeakLinkExplainability
                signalRows={wl.signal_rows}
                lines={wl.reasoning_lines}
                details={wl.score_details}
              />
            </div>
          )}
        </div>
      </div>
    </article>
  );
}

function WeakLinkExplainability({
  englishSummary,
  signalRows,
  lines,
  details,
}: {
  /** When set (e.g. IDR table), shown at top; omit on cards that already surface summary elsewhere. */
  englishSummary?: string | null;
  signalRows?: { label: string; value: string }[] | null;
  lines?: string[] | null;
  details?: Record<string, unknown> | null;
}) {
  const safeLines = (lines || []).filter(Boolean);
  const rows = signalRows || [];
  const hasSignals = rows.length > 0;
  const hasExtras = safeLines.length > 0;
  const hasEnglish = !!(englishSummary && englishSummary.trim());
  const hasRawFallback =
    !hasSignals && safeLines.length === 0 && details && typeof details === 'object' && Object.keys(details).length > 0;
  const hasAnyContent = hasSignals || hasExtras || hasRawFallback || hasEnglish;

  if (!hasAnyContent) {
    return <span className="weak-link-no-explain">No model explanation stored yet.</span>;
  }

  return (
    <div className="weak-link-explainability">
      <details className="weak-link-explain-outer" open>
        <summary className="weak-link-explain-outer-summary">
          <span className="wl-summary-label">Explanation</span>
        </summary>
        <div className="weak-link-explain-outer-body">
          {hasEnglish && <p className="weak-link-english">{englishSummary!.trim()}</p>}

          {hasSignals && (
            <details className="weak-link-nested-details weak-link-model-inputs-details">
              <summary className="weak-link-nested-summary">
                Model inputs <span className="wl-nested-count">({rows.length})</span>
              </summary>
              <ul className="weak-link-signals-list">
                {rows.map((r, i) => (
                  <li key={`${r.label}-${i}`}>
                    <span className="wl-sig-label">{r.label}</span>
                    <span className="wl-sig-value">{r.value}</span>
                  </li>
                ))}
              </ul>
            </details>
          )}

          {hasExtras && (
            <details className="weak-link-nested-details">
              <summary className="weak-link-nested-summary">
                Additional detail <span className="wl-nested-count">({safeLines.length})</span>
              </summary>
              <ul className="weak-link-reasoning-list">
                {safeLines.map((ln, i) => {
                  const sentiment = /supporting/i.test(ln) ? 'positive' : /reducing/i.test(ln) ? 'negative' : '';
                  return <li key={i} className={sentiment}>{ln}</li>;
                })}
              </ul>
            </details>
          )}

          {hasRawFallback && (
            <details className="weak-link-raw-details weak-link-nested-details">
              <summary className="weak-link-nested-summary">Technical metadata</summary>
              <pre className="weak-link-json">
                {(() => {
                  const raw = JSON.stringify(details, null, 2);
                  return raw.length > 1400 ? `${raw.slice(0, 1400)}…` : raw;
                })()}
              </pre>
            </details>
          )}
        </div>
      </details>
    </div>
  );
}

function TopRulesModal({ matches, matchingRules, onClose }: {
  matches: MatchExplanation[];
  matchingRules?: MatchingRule[];
  onClose: () => void;
}) {
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [onClose]);

  const ruleNameById = new Map<string, string>();
  for (const r of matchingRules ?? []) ruleNameById.set(r.rule_id, r.rule_name);

  const groups = new Map<string, { links: number; sum: number }>();
  for (const m of matches) {
    const id = m.match_rule || 'UNKNOWN';
    let g = groups.get(id);
    if (!g) { g = { links: 0, sum: 0 }; groups.set(id, g); }
    g.links += 1;
    g.sum += m.match_score ?? 0;
  }

  const rows = Array.from(groups.entries()).map(([rule_id, g]) => ({
    rule_id,
    rule_name: ruleNameById.get(rule_id) || rule_id,
    links: g.links,
    avg_score: g.links ? g.sum / g.links : 0,
  })).sort((a, b) => b.links - a.links || b.avg_score - a.avg_score);

  const totalLinks = rows.reduce((s, r) => s + r.links, 0) || 1;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-matching-rules" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h2 className="modal-title matching-rules-modal-title">
            <ListOrdered size={16} strokeWidth={2} aria-hidden />
            All Matching Rules ({rows.length})
          </h2>
          <button type="button" className="modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="modal-body">
          {rows.length === 0 ? (
            <div className="empty-hint">No matching rules applied.</div>
          ) : (
            <table className="idr-table">
              <thead>
                <tr>
                  <th>Rule</th>
                  <th className="num">Links</th>
                  <th className="num">Impact</th>
                  <th className="num">Score</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.rule_id}>
                    <td>
                      <span className="idr-rule-id">{r.rule_id}</span>
                      <span className="idr-rule-name">{r.rule_name}</span>
                    </td>
                    <td className="num">{r.links}</td>
                    <td className="num">{Math.round((r.links / totalLinks) * 100)}%</td>
                    <td className="num">{r.avg_score.toFixed(2)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

/* ── Lineage / Resolution History redesign ────────────────────────── */

function parseRecordIds(raw: string | null): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [];
  } catch { return []; }
}

function parseMergedFrom(raw: string | null): string[] {
  if (!raw) return [];
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed : [raw];
  } catch { return raw.split(',').map(s => s.trim()).filter(Boolean); }
}

function ruleProportions(match_details: Record<string, unknown> | null): { rule: string; pct: number }[] {
  if (!match_details) return [];
  const rules = match_details.rules;
  if (!Array.isArray(rules) || rules.length === 0) return [];
  const counts = new Map<string, number>();
  for (const r of rules) counts.set(String(r.rule ?? ''), (counts.get(String(r.rule ?? '')) ?? 0) + 1);
  const total = rules.length;
  return [...counts.entries()]
    .map(([rule, count]) => ({ rule, pct: Math.round((count / total) * 100) }))
    .sort((a, b) => b.pct - a.pct);
}

function computeDelta(evt: LineageEvent): { added: number; total: number } {
  const curr = parseRecordIds(evt.source_record_ids);
  const prev = new Set(parseRecordIds(evt.previous_source_record_ids));
  const added = prev.size > 0 ? curr.filter(id => !prev.has(id)).length : curr.length;
  return { added, total: curr.length };
}

function eventNarrative(evt: LineageEvent): string {
  const { added, total } = computeDelta(evt);
  const merged = parseMergedFrom(evt.merged_from_clusters);

  switch (evt.event_type) {
    case 'CREATED': {
      const props = ruleProportions(evt.match_details);
      const topRule = props.length > 0 ? props[0].rule : null;
      return `Cluster created with ${total} record${total !== 1 ? 's' : ''}${topRule ? ` via ${topRule}` : ''}`;
    }
    case 'MERGED': {
      const props = ruleProportions(evt.match_details);
      const topRule = props.length > 0 ? props[0].rule : null;
      return `Absorbed ${merged.length} cluster${merged.length !== 1 ? 's' : ''} (+${added} record${added !== 1 ? 's' : ''})${topRule ? ` via ${topRule}` : ''} — now ${total} total`;
    }
    case 'MEMBERS_ADDED':
    case 'EXPANDED': {
      const props = ruleProportions(evt.match_details);
      const topRule = props.length > 0 ? props[0].rule : null;
      return `Added ${added} record${added !== 1 ? 's' : ''}${topRule ? ` via ${topRule}` : ''} — now ${total} total`;
    }
    default:
      return `${evt.event_type} — ${added} added, ${total} total`;
  }
}

function recordsBySource(ids: string[], srcMap: Record<string, { source: string }>): { source: string; count: number }[] {
  const map = new Map<string, number>();
  for (const id of ids) {
    const src = srcMap[id]?.source ?? 'unknown';
    map.set(src, (map.get(src) ?? 0) + 1);
  }
  return [...map.entries()].map(([source, count]) => ({ source, count })).sort((a, b) => b.count - a.count);
}

function ClusterSizeChart({ events }: { events: LineageEvent[] }) {
  const points = events.map((evt, i) => ({
    step: i,
    size: parseRecordIds(evt.source_record_ids).length,
    label: evt.event_type,
    date: evt.created_at,
  })).filter(p => p.size > 0);

  if (points.length < 2) return null;

  const maxSize = Math.max(...points.map(p => p.size));
  const W = 320;
  const H = 100;
  const padX = 36;
  const padY = 24;
  const plotW = W - padX - 8;
  const plotH = H - padY * 2;

  const x = (i: number) => padX + (i / (points.length - 1)) * plotW;
  const y = (s: number) => padY + plotH - (s / maxSize) * plotH;

  let pathD = '';
  for (let i = 0; i < points.length; i++) {
    const px = x(i);
    const py = y(points[i].size);
    if (i === 0) { pathD += `M ${px} ${py}`; }
    else {
      const prevX = x(i - 1);
      const midX = (prevX + px) / 2;
      pathD += ` C ${midX} ${y(points[i - 1].size)}, ${midX} ${py}, ${px} ${py}`;
    }
  }
  const areaD = pathD + ` L ${x(points.length - 1)} ${H - padY} L ${x(0)} ${H - padY} Z`;

  return (
    <div className="lnr-size-chart">
      <span className="lnr-size-chart-label">Cluster growth</span>
      <svg viewBox={`0 0 ${W} ${H}`} className="lnr-size-chart-svg">
        <path d={areaD} className="lnr-size-area" />
        <path d={pathD} className="lnr-size-line" fill="none" />
        {points.map((p, i) => (
          <g key={i}>
            <circle cx={x(i)} cy={y(p.size)} r={3.5} className="lnr-size-dot" />
            <text x={x(i)} y={y(p.size) - 8} className="lnr-size-val" textAnchor="middle">{p.size}</text>
          </g>
        ))}
      </svg>
    </div>
  );
}

function LineageTimeline({ lineage }: { lineage: LineageResponse }) {
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const toggle = (id: string) => setExpandedIds(prev => {
    const next = new Set(prev);
    next.has(id) ? next.delete(id) : next.add(id);
    return next;
  });

  const srcMap = lineage.source_records ?? {};

  if (lineage.lineage.length === 0) {
    return <div className="empty-hint">No resolution history found for this cluster.</div>;
  }

  return (
    <>
      <ClusterSizeChart events={lineage.lineage} />
      <div className="timeline">
        {lineage.lineage.map((evt, i) => {
          const ids = parseRecordIds(evt.source_record_ids);
          const prevIds = new Set(parseRecordIds(evt.previous_source_record_ids));
          const addedIds = prevIds.size > 0 ? ids.filter(id => !prevIds.has(id)) : ids;
          const merged = parseMergedFrom(evt.merged_from_clusters);
          const ruleProps = ruleProportions(evt.match_details);
          const narrative = eventNarrative(evt);
          const bySource = recordsBySource(addedIds, srcMap);
          const isOpen = expandedIds.has(evt.log_id);

          return (
            <div key={evt.log_id || i} className={`timeline-event ${evt.event_type.toLowerCase()}`}>
              <div className="timeline-marker" />
              <div className="timeline-content">
                <div className="timeline-header">
                  <span className={`event-type-badge ${evt.event_type.toLowerCase()}`}>{evt.event_type}</span>
                  <span className="timeline-date">{evt.created_at ? new Date(evt.created_at).toLocaleString() : ''}</span>
                </div>

                <p className="lnr-narrative">{narrative}</p>

                {merged.length > 0 && (
                  <div className="lnr-merged-from">
                    <span className="lnr-detail-label">Absorbed from</span>
                    {merged.map(cid => (
                      <span key={cid} className="lnr-cluster-chip">{cid}<CopyButton value={cid} /></span>
                    ))}
                  </div>
                )}

                {ruleProps.length > 0 && (
                  <div className="lnr-rule-pills-section">
                    <span className="lnr-detail-label">Matching rules</span>
                    <div className="lnr-rule-pill-row">
                      {ruleProps.map((p, pi) => (
                        <span key={pi} className="lnr-rule-pill">{p.rule} <span className="lnr-rule-pct">{p.pct}%</span></span>
                      ))}
                    </div>
                  </div>
                )}

                {addedIds.length > 0 && (
                  <div className="lnr-records-section">
                    <button type="button" className="lnr-toggle" onClick={() => toggle(evt.log_id)}>
                      {isOpen ? '▾' : '▸'} {addedIds.length} record{addedIds.length !== 1 ? 's' : ''} added
                      {bySource.length > 0 && (
                        <span className="lnr-source-summary">
                          ({bySource.map(s => `${s.count} ${s.source.replace(/_RAW$/i, '').toLowerCase()}`).join(', ')})
                        </span>
                      )}
                    </button>
                    {isOpen && (
                      <ul className="lnr-record-list">
                        {addedIds.map(id => (
                          <li key={id} className="lnr-record-item">
                            <span className="mono">{id.slice(0, 12)}…</span>
                            {srcMap[id] && <span className="lnr-record-source">{srcMap[id].source}</span>}
                            <CopyButton value={id} />
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                )}
              </div>
            </div>
          );
        })}
      </div>

      {lineage.merged_clusters.length > 0 && (
        <div className="panel" style={{ marginTop: 24 }}>
          <h3 className="lnr-section-title">Absorbed Clusters</h3>
          <div className="lnr-absorbed-list">
            {lineage.merged_clusters.map((evt, i) => (
              <div key={i} className="lnr-absorbed-chip">
                <span className={`event-type-badge ${evt.event_type.toLowerCase()}`}>{evt.event_type}</span>
                <span className="mono">{evt.cluster_id.slice(0, 12)}…<CopyButton value={evt.cluster_id} /></span>
                <span className="timeline-date">{evt.created_at ? new Date(evt.created_at).toLocaleString() : ''}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </>
  );
}

export default function IdentityDetailPage({ identityId, onBack, activeTab }: { identityId: string; onBack: () => void; activeTab?: string }) {
  const [tab, setTab] = useState<Tab>((activeTab as Tab) || 'overview');
  useEffect(() => { if (activeTab && TAB_ORDER.includes(activeTab as Tab)) setTab(activeTab as Tab); }, [activeTab]);
  const [selectedRecord, setSelectedRecord] = useState<SourceRecordDetail | null>(null);
  const [compareIds, setCompareIds] = useState<Set<string>>(new Set());
  const [showCompare, setShowCompare] = useState(false);
  const [showMatchingRulesModal, setShowMatchingRulesModal] = useState(false);
  const [showTopRulesModal, setShowTopRulesModal] = useState(false);
  const [showAIPromptModal, setShowAIPromptModal] = useState(false);
  /** BFS depth for GET connected-clusters-graph (1 = direct partners only; 2–3 expand frontier). */
  const [connectedGraphDepth, setConnectedGraphDepth] = useState(1);
  const [idrSelectedRecordId, setIdrSelectedRecordId] = useState<string | null>(null);
  const { data: detail, isPending, isError, error, refetch } = useQuery({
    queryKey: ['identity', identityId],
    queryFn: () => fetchIdentity(identityId),
  });

  useEffect(() => {
    setConnectedGraphDepth(1);
  }, [identityId]);

  const { data: sourceRecords, isLoading: sourcesLoading } = useQuery({
    queryKey: ['source-records', identityId],
    queryFn: () => fetchSourceRecords(identityId),
    enabled: tab === 'sources',
  });

  const {
    data: explanation,
    isLoading: idrLoading,
    isError: idrExplainError,
    error: idrExplainErr,
    refetch: refetchIdrExplain,
  } = useQuery({
    queryKey: ['idr-explanation', identityId],
    queryFn: () => fetchIDRExplanation(identityId),
    /* Also used on Overview (header: hide confidence when cluster has a single source record). */
    enabled: !!identityId && (tab === 'idr' || tab === 'connected' || tab === 'overview'),
  });

  const { data: lineage, isLoading: lineageLoading } = useQuery({
    queryKey: ['lineage', identityId],
    queryFn: () => fetchLineage(identityId),
    enabled: tab === 'lineage',
  });

  const { data: matchingRules, isLoading: matchingRulesLoading } = useQuery({
    queryKey: ['matching-rules'],
    queryFn: fetchMatchingRules,
    enabled: showMatchingRulesModal || tab === 'household' || tab === 'idr',
    staleTime: 5 * 60 * 1000,
  });

  const { data: aiSummary, isLoading: aiLoading } = useQuery({
    queryKey: ['ai-summary', identityId],
    queryFn: () => fetchAISummary(identityId),
    enabled: tab === 'ai',
  });

  const { data: weakLinks, isLoading: weakLinksLoading } = useQuery({
    queryKey: ['weak-links', identityId],
    queryFn: () => fetchWeakLinks(identityId),
    enabled: !!identityId && (tab === 'overview' || tab === 'connected'),
  });

  const { data: llmReviews } = useQuery({
    queryKey: ['llm-reviews', identityId],
    queryFn: () => fetchLLMReviews(identityId),
    enabled: !!identityId && (tab === 'overview' || tab === 'connected'),
  });

  const { data: graphBundle, isLoading: graphLoading, isFetching: graphFetching } = useQuery({
    queryKey: ['connected-clusters-graph', identityId, connectedGraphDepth],
    queryFn: () => fetchConnectedClustersGraph(identityId, connectedGraphDepth),
    enabled: !!identityId && tab === 'connected',
    placeholderData: keepPreviousData,
  });

  const { data: household, isLoading: householdLoading } = useQuery<HouseholdInfo | null>({
    queryKey: ['identity-household', identityId],
    queryFn: () => fetchIdentityHousehold(identityId),
    enabled: !!identityId && (tab === 'overview' || tab === 'household'),
  });

  const { data: aiRiskSignals, isLoading: aiRiskLoading } = useQuery<AIRiskSignals>({
    queryKey: ['household-ai-risk', household?.household_id, identityId],
    queryFn: () => fetchHouseholdAIRisk(household!.household_id, identityId),
    enabled: tab === 'household' && !!household?.household_id,
    staleTime: 5 * 60 * 1000,
  });

  const linksForGraph = useMemo(() => {
    if (graphBundle?.pairs && graphBundle.pairs.length > 0) {
      const wlByPair = new Map((Array.isArray(weakLinks) ? weakLinks : []).map(wl => [wl.pair_id, wl]));
      return graphBundle.pairs.map(p => {
        const converted = graphBundlePairToWeakLink(identityId, p);
        const orig = wlByPair.get(p.pair_id);
        if (orig) {
          converted.english_summary = orig.english_summary;
          converted.reasoning_lines = orig.reasoning_lines;
          converted.signal_rows = orig.signal_rows;
        }
        return converted;
      });
    }
    return Array.isArray(weakLinks) ? weakLinks : [];
  }, [graphBundle, weakLinks, identityId]);

  /** Candidate cards / KPIs / jump list: only pairs incident on this identity (expanded graph adds non-ego edges). */
  const linksForConnectedCards = useMemo(
    () => linksForGraph.filter(wl => wl.cluster_id_a === identityId || wl.cluster_id_b === identityId),
    [linksForGraph, identityId],
  );

  const needsWeakFallback =
    graphBundle != null && (!graphBundle.pairs || graphBundle.pairs.length === 0);
  const connectedTabLoading =
    graphLoading || idrLoading || (tab === 'connected' && needsWeakFallback && weakLinksLoading);

  const connectedKpis = useMemo(() => computeConnectedKpis(linksForConnectedCards), [linksForConnectedCards]);

  const toggleCompare = (recordId: string) => {
    setCompareIds(prev => {
      const next = new Set(prev);
      if (next.has(recordId)) {
        next.delete(recordId);
      } else if (next.size < 3) {
        next.add(recordId);
      }
      return next;
    });
  };

  if (isPending) return <div className="loading loading-page">Loading...</div>;

  if (isError) {
    const msg = error instanceof Error ? error.message : 'Request failed.';
    return (
      <div className="identity-detail-page">
        <div className="detail-header">
          <button type="button" className="back-btn" onClick={onBack}>&larr; Back</button>
        </div>
        <div className="error-message">
          <p><strong>Could not load this identity.</strong></p>
          <p className="empty-detail">{msg}</p>
          <button type="button" className="btn-secondary" style={{ marginTop: 12 }} onClick={() => void refetch()}>
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!detail) return <div className="loading loading-page">Loading...</div>;

  const { profile, identifiers } = detail;
  const aggregatedIdentifiers = aggregateLinkedIdentifiersByValue(identifiers);
  const piiByType = (() => {
    const counts = new Map<string, Map<string, number>>();
    for (const ident of identifiers) {
      if (!ident.identifier_value) continue;
      const t = ident.identifier_type;
      if (!counts.has(t)) counts.set(t, new Map());
      const m = counts.get(t)!;
      m.set(ident.identifier_value, (m.get(ident.identifier_value) || 0) + 1);
    }
    const top = (t: string): string | null => {
      const m = counts.get(t);
      if (!m || m.size === 0) return null;
      let best: string | null = null;
      let bestN = -1;
      for (const [v, n] of m) {
        if (n > bestN) { best = v; bestN = n; }
      }
      return best;
    };
    const titleCase = (s: string | null) =>
      s == null
        ? null
        : s
            .toLowerCase()
            .replace(/\b\w/g, (c) => c.toUpperCase());
    const first = titleCase(top('NAME_FIRST'));
    const last = titleCase(top('NAME_LAST'));
    const fullName = [first, last].filter(Boolean).join(' ') || null;
    const email = top('EMAIL');
    const phone = top('PHONE');
    const street = titleCase(top('STREET_ADDRESS'));
    const city = titleCase(top('CITY'));
    const state = top('STATE');
    const postal = top('POSTAL_CODE');
    const address = [street, [city, state].filter(Boolean).join(', '), postal].filter(Boolean).join(' · ') || null;
    return { fullName, email, phone, address };
  })();
  const hideHeaderConfidence =
    explanation != null &&
    Array.isArray(explanation.source_records) &&
    explanation.source_records.length === 1;

  return (
    <div className="identity-detail-page">
      <div className="detail-header">
        <button className="back-btn" onClick={onBack}>&larr; Back</button>
        <div className="detail-title">
          <h1>{profile.display_name || profile.identity_id.slice(0, 16)}</h1>
          <span className="identity-id-full mono">{profile.identity_id}<CopyButton value={profile.identity_id} /></span>
        </div>
        <div className="header-badges">
          {profile.confidence_score != null && !hideHeaderConfidence && (
            <span className={`confidence-badge ${getConfidenceClass(profile.confidence_score)}`}>
              {(profile.confidence_score * 100).toFixed(0)}% Confidence
            </span>
          )}
          <span className="source-badge-sm" title="Distinct source systems (e.g. POS_TRANSACTION_RAW). Row count is on the Sources tab.">
            {profile.source_count != null
              ? `${profile.source_count} source type${profile.source_count === 1 ? '' : 's'}`
              : '—'}
          </span>
        </div>
      </div>

      <div className="tab-bar">
        {TAB_ORDER.map(t => (
          <button
            key={t}
            className={`tab-btn ${tab === t ? 'active' : ''}`}
            onClick={() => { setTab(t); setShowCompare(false); }}
          >
            {TAB_LABELS[t]}
          </button>
        ))}
      </div>

      <div className="tab-content">
        {tab === 'overview' && (
          <div className="overview-tab">
            <div className="overview-grid">
              <div className="panel">
                <h2>Primary Identifiers</h2>
                <div className="kv-list">
                  {piiByType.fullName && <div className="kv-row"><span className="kv-key">Name</span><span className="kv-val">{piiByType.fullName}</span></div>}
                  {piiByType.email && <div className="kv-row"><span className="kv-key">Email</span><span className="kv-val mono">{piiByType.email}</span></div>}
                  {piiByType.phone && <div className="kv-row"><span className="kv-key">Phone</span><span className="kv-val mono">{piiByType.phone}</span></div>}
                  {piiByType.address && <div className="kv-row"><span className="kv-key">Address</span><span className="kv-val">{piiByType.address}</span></div>}
                  {profile.primary_hem && <div className="kv-row"><span className="kv-key">Email HEM</span><span className="kv-val mono">{profile.primary_hem}</span></div>}
                  {profile.primary_uid2 && <div className="kv-row"><span className="kv-key">UID2</span><span className="kv-val mono">{profile.primary_uid2}</span></div>}
                  {profile.primary_rampid && <div className="kv-row"><span className="kv-key">RampID</span><span className="kv-val mono">{profile.primary_rampid}</span></div>}
                  {profile.primary_ppid && <div className="kv-row"><span className="kv-key">Loyalty Member #</span><span className="kv-val mono">{profile.primary_ppid}</span></div>}
                  {profile.primary_device_id && <div className="kv-row"><span className="kv-key">Device ID</span><span className="kv-val mono">{profile.primary_device_id}</span></div>}
                  {profile.primary_cookie && <div className="kv-row"><span className="kv-key">Cookie</span><span className="kv-val mono">{profile.primary_cookie}</span></div>}
                </div>
              </div>

              <div className="panel">
                <div className="panel-title-row">
                  <h2>Household</h2>
                  {household && (
                    <span className={`source-pill ${household.household_type === 'SAME_SURNAME' ? 'pos-transaction-raw' : 'shopify-order-raw'}`}>
                      {household.household_type === 'SAME_SURNAME' ? 'Same surname' : 'Fuzzy surname'} &middot; {household.member_count} members
                    </span>
                  )}
                </div>
                {householdLoading && !household ? (
                  <div className="loading">Loading household...</div>
                ) : household ? (
                  <>
                    <div className="kv-list">
                      <div className="kv-row">
                        <span className="kv-key">Shared Address</span>
                        <span className="kv-val">
                          {[household.shared_street, household.shared_city, household.shared_state, household.shared_postal].filter(Boolean).join(', ') || '-'}
                        </span>
                      </div>
                      {household.avg_member_confidence != null && (
                        <div className="kv-row">
                          <span className="kv-key">Avg Member Confidence</span>
                          <span className="kv-val mono">{(household.avg_member_confidence * 100).toFixed(1)}%</span>
                        </div>
                      )}
                    </div>
                    {household.members.filter(m => !m.is_current).length > 0 && (
                      <div className="household-chip-row">
                        <span className="household-chip-label">Co-members:</span>
                        {household.members.filter(m => !m.is_current).map(m => (
                          <button
                            key={m.cluster_id}
                            type="button"
                            className="household-chip"
                            onClick={() => { window.location.hash = `#/identity/${m.cluster_id}`; }}
                            title={m.primary_email || m.cluster_id}
                          >
                            {[m.primary_first_name, m.primary_last_name].filter(Boolean).join(' ') || m.cluster_id.slice(0, 8)}
                          </button>
                        ))}
                      </div>
                    )}
                    <div className="panel-footer-link">
                      <button type="button" className="cc-more-link" onClick={() => setTab('household')}>
                        View household details &rarr;
                      </button>
                    </div>
                  </>
                ) : (
                  <div className="empty-hint">Not part of a household</div>
                )}
              </div>

              <div className="panel">
                <div className="panel-title-row">
                  <h2>Connected Clusters</h2>
                  <div className="panel-hint-wrap">
                    <button
                      type="button"
                      className="panel-hint-btn"
                      aria-label="About connected clusters"
                      aria-describedby="connected-clusters-overview-tooltip"
                    >
                      <svg
                        className="panel-hint-icon-svg"
                        width="16"
                        height="16"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        aria-hidden
                      >
                        <circle cx="12" cy="12" r="10" />
                        <path d="M12 16v-4M12 8h.01" />
                      </svg>
                    </button>
                    <div
                      id="connected-clusters-overview-tooltip"
                      className="panel-hint-tooltip panel-hint-tooltip--below"
                      role="tooltip"
                    >
                      <p>
                        Clusters that are not deterministically merged, but show a high likelihood of belonging to
                        the same user or household based on probabilistic signals (e.g., shared fingerprints,
                        behavioral patterns).
                      </p>
                      <p>
                        These links are confidence-scored and do not change the core identity unless promoted.
                      </p>
                    </div>
                  </div>
                </div>
                <div className="kv-list">
                  {weakLinksLoading && <div className="empty-hint">Loading…</div>}
                  {!weakLinksLoading && weakLinks && weakLinks.length > 0 ? (
                    <>
                      {weakLinks.slice(0, 3).map(wl => {
                        const partnerId = wl.cluster_id_a === identityId ? wl.cluster_id_b : wl.cluster_id_a;
                        const scoreLabel = wl.ml_score != null ? `${(wl.ml_score * 100).toFixed(1)}%` : '—';
                        const tileAiConf = llmReviews?.find(r => r.pair_id === wl.pair_id)?.llm_confidence ?? null;
                        const tileDelta = wl.score_details?.score_delta != null ? Number(wl.score_details.score_delta) : null;
                        const tileHasTrend = tileDelta != null && !isNaN(tileDelta) && tileDelta !== 0;
                        const tileUp = tileHasTrend && tileDelta! > 0;
                        return (
                          <div key={wl.pair_id} className="cc-overview-cluster-row">
                            <span className="cc-overview-cluster-id mono">{partnerId}</span>
                            <div className="cc-overview-cluster-pills">
                              <span className={`cc-overview-pill cc-overview-pill--tier ${tierOf(wl) === 'MERGE_PERSON' ? 'cc-overview-pill--merge' : tierOf(wl) === 'HOUSEHOLD_LINK' ? 'cc-overview-pill--household' : 'cc-overview-pill--candidate'}`}>
                                {tierOf(wl).replace(/_/g, ' ')}
                              </span>
                              <span className="cc-overview-pill cc-overview-pill--score">
                                ML {scoreLabel}
                              </span>
                              {tileAiConf != null && (
                                <span className="cc-overview-pill cc-overview-pill--ai">
                                  AI {(tileAiConf * 100).toFixed(0)}%
                                </span>
                              )}
                              {tileHasTrend && (
                                <span className={`cc-overview-pill ${tileUp ? 'cc-overview-pill--up' : 'cc-overview-pill--down'}`}>
                                  {tileUp ? '\u25B2' : '\u25BC'} {tileDelta! > 0 ? '+' : ''}{(tileDelta! * 100).toFixed(1)}%
                                </span>
                              )}

                            </div>
                          </div>
                        );
                      })}
                      {weakLinks.length > 3 && (
                        <button
                          type="button"
                          className="cc-more-link"
                          onClick={() => setTab('connected')}
                        >
                          +{weakLinks.length - 3} more
                        </button>
                      )}
                    </>
                  ) : !weakLinksLoading ? (
                    <div className="empty-hint">No connected clusters detected</div>
                  ) : null}
                </div>
              </div>
            </div>

            {aggregatedIdentifiers.length > 0 && (
              <div className="panel full-width">
                <h2>All Linked Identifiers</h2>
                <p className="linked-id-hint">
                  One row per value. Types and sources combine when the same value appears from multiple systems.
                </p>
                <table className="identity-table full">
                  <thead>
                    <tr>
                      <th>Type</th>
                      <th>Value</th>
                      <th>Sources</th>
                    </tr>
                  </thead>
                  <tbody>
                    {aggregatedIdentifiers.map((row) => (
                      <tr key={row.identifier_value}>
                        <td>
                          <div className="tag-cell">
                            {row.identifier_types.map((t) => (
                              <span key={t} className="id-type-tag">{t}</span>
                            ))}
                          </div>
                        </td>
                        <td className="mono">{row.identifier_value}</td>
                        <td>
                          <div className="tag-cell">
                            {row.source_systems.map((s) => (
                              <span key={s} className={`source-pill ${s.toLowerCase().replace(/_/g, '-')}`}>{s}</span>
                            ))}
                          </div>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {tab === 'sources' && (
          <div className="sources-tab">
            {sourcesLoading && <div className="loading">Loading source records...</div>}
            {sourceRecords && sourceRecords.length === 0 && (
              <div className="empty-state">No source records found for this cluster.</div>
            )}
            {sourceRecords && sourceRecords.length > 0 && (
              showCompare ? (
                <CompareView
                  records={sourceRecords.filter(r => compareIds.has(r.record_id))}
                  onBack={() => setShowCompare(false)}
                />
              ) : (
                <>
                  <p className="sources-intro">
                    Each row is one vendor contribution to this identity. Use <strong>Details</strong> for a plain-English summary, extracted IDs, and the original payload when stored.
                    Select up to three rows to <strong>compare</strong> how sources agree.
                  </p>
                  <div className="sources-toolbar">
                    <span className="sources-count">{sourceRecords.length} source record{sourceRecords.length !== 1 ? 's' : ''}</span>
                    <button
                      type="button"
                      className="compare-btn"
                      disabled={compareIds.size < 2}
                      onClick={() => setShowCompare(true)}
                    >
                      Compare selected ({compareIds.size})
                    </button>
                  </div>
                  <table className="sources-table">
                    <thead>
                      <tr>
                        <th className="th-checkbox" title="Include in side-by-side compare"></th>
                        <th>Source</th>
                        <th>Record</th>
                        <th>IDs</th>
                        <th>Created</th>
                        <th className="th-actions">Details</th>
                      </tr>
                    </thead>
                    <tbody>
                      {sourceRecords.map(r => (
                        <tr
                          key={r.record_id}
                          className={`sources-data-row ${compareIds.has(r.record_id) ? 'row-selected' : ''}`}
                        >
                          <td className="td-checkbox">
                            <input
                              type="checkbox"
                              checked={compareIds.has(r.record_id)}
                              disabled={!compareIds.has(r.record_id) && compareIds.size >= 3}
                              onChange={() => toggleCompare(r.record_id)}
                              aria-label={`Include ${r.record_id} in compare`}
                            />
                          </td>
                          <td><span className={`source-pill ${r.source_type.toLowerCase().replace(/_/g, '-')}`}>{r.source_type.replace(/_/g, ' ')}</span></td>
                          <td className="mono sources-record-cell" title={r.record_id}>
                            {r.record_id.length > 24 ? r.record_id.slice(0, 12) + '…' + r.record_id.slice(-8) : r.record_id}
                            <CopyButton value={r.record_id} />
                          </td>
                          <td>{r.identifiers.length} extracted</td>
                          <td>{r.created_at ? new Date(r.created_at).toLocaleString() : '—'}</td>
                          <td className="td-actions">
                            <button
                              type="button"
                              className="btn-details"
                              onClick={() => setSelectedRecord(r)}
                            >
                              View
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )
            )}
          </div>
        )}

        {tab === 'idr' && (
          <div className="idr-tab idr-tab-v2">
            {idrLoading && <div className="loading">Loading IDR explanation...</div>}
            {explanation && (
              explanation.matches.length === 0 ? (
                <div className="no-match-info">
                  <div className="no-match-icon"><Info size={20} /></div>
                  <div className="no-match-content">
                    <h3>No Match Explanation Available</h3>
                    <p>This record has not been matched with any other records in the system. It exists as a standalone identity.</p>
                  </div>
                </div>
              ) : (
                <>
                  <div className="idr-systems idr-chip-row">
                    {explanation.source_systems.map(s => (
                      <span key={s} className={`source-pill ${s.toLowerCase().replace(/_/g, '-')}`}>{s}</span>
                    ))}
                    {explanation.rules_applied.map(r => (
                      <span key={r} className="rule-pill">{r}</span>
                    ))}
                    <button
                      type="button"
                      className="tab-inline-link matching-rules-link"
                      onClick={() => setShowMatchingRulesModal(true)}
                    >
                      <BookOpen size={13} strokeWidth={2} aria-hidden />
                      View Matching Rules
                    </button>
                  </div>

                  <div className="idr-overview-grid">
                    <section className="idr-card idr-match-overview">
                      <header className="idr-card-header"><h3>Match Overview</h3></header>
                      <div className="idr-card-body idr-mo-body">
                        <div className="idr-mo-legend">
                          <IDRGraphLegend sourceRecords={explanation.source_records} />
                        </div>
                        <div className="idr-mo-stats">
                          <div className="idr-mo-stat">
                            <Network size={18} strokeWidth={1.75} aria-hidden className="idr-mo-stat-icon" />
                            <div className="idr-mo-stat-text">
                              <div className="idr-mo-stat-val">{explanation.total_identifiers}</div>
                              <div className="idr-mo-stat-label">Records</div>
                              <div className="idr-mo-stat-sub">Representing this identity</div>
                            </div>
                          </div>
                          <div className="idr-mo-stat">
                            <Link2 size={18} strokeWidth={1.75} aria-hidden className="idr-mo-stat-icon" />
                            <div className="idr-mo-stat-text">
                              <div className="idr-mo-stat-val">{explanation.matches.length}</div>
                              <div className="idr-mo-stat-label">Match Pairs</div>
                              <div className="idr-mo-stat-sub">Between records</div>
                            </div>
                          </div>
                          <div className="idr-mo-stat">
                            <ShieldCheck size={18} strokeWidth={1.75} aria-hidden className="idr-mo-stat-icon" />
                            <div className="idr-mo-stat-text">
                              <div className={`idr-mo-stat-val ${getConfidenceClass(explanation.cluster_confidence)}`}>
                                {explanation.cluster_confidence != null
                                  ? `${(explanation.cluster_confidence * 100).toFixed(0)}%`
                                  : '—'}
                              </div>
                              <div className="idr-mo-stat-label">Cluster Confidence</div>
                              <div className="idr-mo-stat-sub">
                                {(() => {
                                  const c = explanation.cluster_confidence ?? 0;
                                  if (c >= 0.95) return 'High confidence match';
                                  if (c >= 0.85) return 'Medium confidence match';
                                  return 'Low confidence match';
                                })()}
                              </div>
                            </div>
                          </div>
                          {(() => {
                            const ruleTypeById = new Map<string, string | null>();
                            for (const r of matchingRules ?? []) ruleTypeById.set(r.rule_id, r.match_type);
                            const isDetLike = (t: string | null | undefined) => {
                              const v = (t || '').toUpperCase();
                              return v === 'DETERMINISTIC' || v === 'EXACT' || v === 'HASH';
                            };
                            const allDet = explanation.matches.length > 0
                              && explanation.matches.every(m => isDetLike(ruleTypeById.get(m.match_rule || '')));
                            const label = allDet ? 'Deterministic' : 'Probabilistic';
                            const sub = allDet
                              ? 'All rules are deterministic'
                              : 'Mix of deterministic & fuzzy rules';
                            return (
                              <div className="idr-mo-stat">
                                <Target size={18} strokeWidth={1.75} aria-hidden className="idr-mo-stat-icon" />
                                <div className="idr-mo-stat-text">
                                  <div className={`idr-mo-stat-val idr-mo-stat-val--sm ${allDet ? 'high' : ''}`}>{label}</div>
                                  <div className="idr-mo-stat-label">Resolution Type</div>
                                  <div className="idr-mo-stat-sub">{sub}</div>
                                </div>
                              </div>
                            );
                          })()}
                        </div>
                        <div className="idr-mo-graph">
                          <IDRForceGraph
                            identityId={identityId}
                            matches={explanation.matches}
                            sourceRecords={explanation.source_records}
                            selectedRecordId={idrSelectedRecordId}
                            onSelectRecord={setIdrSelectedRecordId}
                            hideMatrix
                            hideLegend
                          />
                        </div>
                      </div>
                    </section>
                    <div className="idr-overview-right">
                      <section className="idr-card idr-match-quality">
                        <header className="idr-card-header"><h3>Match Quality</h3></header>
                        <div className="idr-card-body">
                          <IDRMatchQualityDonut matches={explanation.matches} />
                        </div>
                      </section>
                      <section className="idr-card">
                        <header className="idr-card-header"><h3>Top Matching Rules</h3></header>
                        <div className="idr-card-body">
                          <IDRTopRulesTable
                            matches={explanation.matches}
                            matchingRules={matchingRules}
                            topN={2}
                            onShowAll={() => setShowTopRulesModal(true)}
                          />
                        </div>
                      </section>
                    </div>
                  </div>

                  {idrSelectedRecordId && (
                    <section className="idr-match-detail-pane">
                        <MatchMatrix
                          recordId={idrSelectedRecordId}
                          matches={explanation.matches}
                          recSourceMap={new Map(explanation.source_records.map(r => [r.record_id, r.source]))}
                          clusterId={identityId}
                          onClose={() => setIdrSelectedRecordId(null)}
                        />
                    </section>
                  )}
                </>
              )
            )}
          </div>
        )}

        {tab === 'connected' && (
          <div className="connected-clusters-tab">
            {idrExplainError && (
              <div className="error-message">
                <p>
                  <strong>Could not load connected clusters.</strong>{' '}
                  {idrExplainErr instanceof Error ? idrExplainErr.message : 'Request failed.'}
                </p>
                <button type="button" className="btn-secondary" onClick={() => void refetchIdrExplain()}>
                  Retry
                </button>
              </div>
            )}

            {!idrExplainError && connectedTabLoading && <div className="loading">Loading connected clusters…</div>}

            {!idrExplainError && !connectedTabLoading && linksForGraph.length === 0 && (
              <div className="empty-hint">No connected cluster candidates for this identity.</div>
            )}

            {!idrExplainError && !connectedTabLoading && linksForGraph.length > 0 && (
              <>
                <ConnectedClustersGraph
                  identityId={identityId}
                  links={linksForGraph}
                  graphDepth={connectedGraphDepth}
                  onGraphDepthChange={setConnectedGraphDepth}
                  clusterDepth={graphBundle?.cluster_depth}
                  clusterParent={graphBundle?.cluster_parent}
                  isRefreshing={graphFetching && !graphLoading}
                  lead={
                    <p className="connected-clusters-lead">
                      ML <strong>weak-link candidates</strong> (shared fingerprints) — not merged into this identity.
                      The graph shows one <strong>bridge path</strong> per candidate pair (via the diamond); each bridge
                      lists the pair&apos;s ML score and distinct shared fingerprint count. Per-record event dots are
                      off for now. Model context:{' '}
                      <button type="button" className="tab-inline-link" onClick={() => setTab('idr')}>
                        IDR Explanation
                      </button>
                      .
                    </p>
                  }
                  asideExtra={
                    <div className="cc-ego-card">
                      <div className="cc-ego-card-title">
                        <ListOrdered size={16} strokeWidth={2} aria-hidden />
                        Connected clusters
                      </div>
                      <p className="cc-ego-cluster-jump-hint">
                        Open the matching detail panel below the graph.
                      </p>
                      <ul className="cc-ego-cluster-jump-list" role="list">
                        {linksForConnectedCards.map(wl => {
                          const partner = weakLinkPartnerId(identityId, wl);
                          const score = wl.ml_score;
                          const aiConf = llmReviews?.find(r => r.pair_id === wl.pair_id)?.llm_confidence ?? null;
                          const short =
                            partner.length > 22 ? `${partner.slice(0, 20)}…` : partner;
                          const tier = tierOf(wl);
                          return (
                            <li key={wl.pair_id}>
                              <button
                                type="button"
                                className="cc-ego-cluster-jump-link"
                                title={partner}
                                onClick={() => scrollToConnectedClusterCard(wl.pair_id)}
                              >
                                <span className="mono cc-ego-cluster-jump-id">{short}</span>
                                <div className="cc-ego-cluster-jump-pills">
                                  <span className={`cc-overview-pill cc-overview-pill--tier ${tier === 'MERGE_PERSON' ? 'cc-overview-pill--merge' : tier === 'HOUSEHOLD_LINK' ? 'cc-overview-pill--household' : 'cc-overview-pill--candidate'}`}>
                                    {tier.replace(/_/g, ' ')}
                                  </span>
                                  {score != null && (
                                    <span className="cc-overview-pill cc-overview-pill--score">ML {(score * 100).toFixed(0)}%</span>
                                  )}
                                  {aiConf != null && (
                                    <span className="cc-overview-pill cc-overview-pill--ai">AI {(aiConf * 100).toFixed(0)}%</span>
                                  )}
                                </div>
                              </button>
                            </li>
                          );
                        })}
                      </ul>
                    </div>
                  }
                >
                  <div className="cc-kpi-row" aria-label="Candidate summary">
                    <div className="cc-kpi-card">
                      <div className="cc-kpi-label">Strong candidates</div>
                      <div className="cc-kpi-value">{connectedKpis.strong}</div>
                      <div className="cc-kpi-desc">Score ≥ 80% or merge-approved</div>
                    </div>
                    <div className="cc-kpi-card">
                      <div className="cc-kpi-label">Needs review</div>
                      <div className="cc-kpi-value">{connectedKpis.needsReview}</div>
                      <div className="cc-kpi-desc">Pending or below threshold</div>
                    </div>
                    <div className="cc-kpi-card">
                      <div className="cc-kpi-label">Avg candidate score</div>
                      <div className="cc-kpi-value">{connectedKpis.avgScoreDisplay}</div>
                      <div className="cc-kpi-desc">Across scored pairs only</div>
                    </div>
                    <div className="cc-kpi-card">
                      <div className="cc-kpi-label">Common signal</div>
                      <div className="cc-kpi-value cc-kpi-value--sm">{connectedKpis.commonSignalDisplay}</div>
                      <div className="cc-kpi-desc">Most frequent model input label</div>
                    </div>
                  </div>
                </ConnectedClustersGraph>

                <div className="cc-candidate-stack">
                  {linksForConnectedCards.map(wl => (
                    <ConnectedClusterCandidateCard key={wl.pair_id} wl={wl} identityId={identityId} llmReview={llmReviews?.find(r => r.pair_id === wl.pair_id)} />
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        {tab === 'household' && (
          <div className="household-tab">
            {householdLoading && !household ? (
              <div className="loading">Loading household...</div>
            ) : !household ? (
              <div className="panel">
                <div className="panel-title-row">
                  <h2>Household</h2>
                </div>
                <div className="empty-hint">
                  Not part of a household. Households are formed when R14 (last name + full address)
                  or R15 (fuzzy last name + address) match between two individual clusters.
                </div>
              </div>
            ) : (
              <>
                {(() => {
                  const orderedMembers = [...household.members].sort((a, b) => {
                    if (a.is_current && !b.is_current) return -1;
                    if (!a.is_current && b.is_current) return 1;
                    return 0;
                  });
                  const memberNames = orderedMembers
                    .map(m => [m.primary_first_name, m.primary_last_name].filter(Boolean).join(' ').trim())
                    .filter(Boolean);
                  const namesSentence = memberNames.length === 0
                    ? 'These identities'
                    : memberNames.length === 1
                    ? memberNames[0]
                    : memberNames.length === 2
                    ? `${memberNames[0]} and ${memberNames[1]}`
                    : `${memberNames.slice(0, -1).join(', ')}, and ${memberNames[memberNames.length - 1]}`;
                  const matchKindWord = household.household_type === 'SAME_SURNAME' ? 'last name' : 'fuzzy last name';
                  const avgConfPct = household.avg_member_confidence != null
                    ? `${(household.avg_member_confidence * 100).toFixed(1)}%`
                    : null;
                  const addressLine = [household.shared_street, household.shared_city, household.shared_state, household.shared_postal]
                    .filter(Boolean)
                    .join(', ');
                  const postalOnly = household.shared_postal || addressLine || 'shared address';

                  const ruleLookup = new Map((matchingRules ?? []).map(r => [r.rule_id, r]));
                  const primaryEdge = household.edges.length > 0
                    ? [...household.edges].sort((a, b) => (b.match_score ?? 0) - (a.match_score ?? 0))[0]
                    : null;
                  const primaryRule = primaryEdge ? ruleLookup.get(primaryEdge.rule_id) : null;
                  const primaryRuleShort = primaryEdge
                    ? primaryEdge.rule_id.replace(/^MARTECH_/, '')
                    : null;
                  const primaryRuleType = primaryRule?.match_type ?? (household.household_type === 'SAME_SURNAME' ? 'DETERMINISTIC' : 'PROBABILISTIC');
                  // Treat household-grain rules and DETERMINISTIC rules the same for risk classification.
                  // R14 / SAME_SURNAME is rule-anchored on shared address + exact last name; the score field
                  // is a base score (0.9), not a similarity measure, so it should not penalize risk.
                  const isDeterministicLike = (matchType: string | null | undefined, ruleId: string) => {
                    const t = (matchType ?? '').toUpperCase();
                    if (t === 'DETERMINISTIC' || t === 'HOUSEHOLD') return true;
                    if (ruleId === 'MARTECH_R14') return true;
                    return false;
                  };

                  const stripRunBy = (desc: string | null | undefined): string | null => {
                    if (!desc) return null;
                    return desc.replace(/\s*Run by [^.]*\.?\s*$/i, '').trim() || null;
                  };

                  const minEdgeScore = household.edges.length > 0
                    ? Math.min(...household.edges.map(e => e.match_score ?? 1))
                    : 1;
                  const allDeterministic = household.edges.every(e => {
                    const r = ruleLookup.get(e.rule_id);
                    return isDeterministicLike(r?.match_type, e.rule_id);
                  });

                  const legendRules = (() => {
                    const seen = new Set<string>();
                    const out: { ruleId: string; label: string; dashed: boolean }[] = [];
                    for (const e of household.edges) {
                      if (seen.has(e.rule_id)) continue;
                      seen.add(e.rule_id);
                      const r = ruleLookup.get(e.rule_id);
                      const shortId = e.rule_id.replace(/^MARTECH_/, '');
                      const label = r?.rule_name ?? shortId;
                      const dashed = shortId === 'R15';
                      out.push({ ruleId: shortId, label, dashed });
                    }
                    return out.sort((a, b) => a.ruleId.localeCompare(b.ruleId));
                  })();
                  const memberNameLookup = new Map<string, string>();
                  for (const m of household.members) {
                    const fn = (m.primary_first_name ?? '').trim();
                    const ln = (m.primary_last_name ?? '').trim();
                    const name = `${fn} ${ln}`.trim();
                    if (name) memberNameLookup.set(m.cluster_id, name);
                  }
                  const memberNameFor = (cid: string): string =>
                    memberNameLookup.get(cid) ?? cid.slice(0, 8);
                  const risk: { level: 'Low' | 'Medium' | 'High'; reason: string } = (() => {
                    if (household.edges.length === 0) return { level: 'Low', reason: 'No fuzzy edges in household' };
                    if (allDeterministic) return { level: 'Low', reason: 'All linking edges are deterministic rule matches' };
                    if (minEdgeScore >= 0.85) return { level: 'Medium', reason: 'At least one edge in the probabilistic band' };
                    return { level: 'High', reason: 'Edge score below 0.85 — review recommended' };
                  })();

                  const riskSignals: string[] = [];
                  if (allDeterministic) riskSignals.push('All linking edges are deterministic rule matches.');
                  else riskSignals.push('Household contains at least one fuzzy / probabilistic edge.');
                  if (allDeterministic) {
                    riskSignals.push('Anchored on rule-based match — score is a fixed base, not a similarity measure.');
                  }
                  if (household.edges.length > 0) {
                    riskSignals.push(`Lowest edge score: ${minEdgeScore.toFixed(3)}.`);
                  }
                  if (household.shared_postal) {
                    riskSignals.push(`Address shared across ${household.member_count} individuals (postal ${household.shared_postal}).`);
                  }
                  if (household.household_type === 'FUZZY_SURNAME') {
                    riskSignals.push('Last names matched on fuzzy similarity (R15) rather than exact match (R14).');
                  }

                  const fmtDate = (s: string | null) => s ? new Date(s).toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: 'numeric' }) : '—';
                  const fmtDateTime = (s: string | null) => s ? new Date(s).toLocaleString() : '';

                  type TimelineItem = { ts: string | null; label: string; detail: string; tone: 'event' | 'rule' | 'system' };
                  const timeline: TimelineItem[] = [];
                  if (household.first_seen) {
                    timeline.push({
                      ts: household.first_seen,
                      label: 'First household member observed',
                      detail: 'Earliest activity recorded for any member of this household.',
                      tone: 'event',
                    });
                  }
                  household.edges.forEach((e) => {
                    const desc = stripRunBy(ruleLookup.get(e.rule_id)?.rule_description);
                    const detIsh = isDeterministicLike(ruleLookup.get(e.rule_id)?.match_type, e.rule_id);
                    const scoreLabel = detIsh ? `base ${e.match_score?.toFixed(3) ?? '—'}` : `score ${e.match_score?.toFixed(3) ?? '—'}`;
                    timeline.push({
                      ts: household.first_seen,
                      label: `${e.rule_id.replace(/^MARTECH_/, '')} matched`,
                      detail: `${desc ?? 'Rule matched between two cluster members.'} (${scoreLabel})`,
                      tone: 'rule',
                    });
                  });
                  if (household.edges.length > 0) {
                    timeline.push({
                      ts: household.first_seen,
                      label: 'Household formed',
                      detail: `${household.member_count} clusters joined under household ${household.household_id.slice(0, 8)}…`,
                      tone: 'system',
                    });
                  }
                  if (household.last_seen) {
                    timeline.push({
                      ts: household.last_seen,
                      label: 'Most recent activity',
                      detail: 'Last observed activity from any household member.',
                      tone: 'event',
                    });
                  }

                  return (
                    <>
                      <div className="hh-summary-row">
                        <div className="hh-summary-banner">
                          <span>
                            {namesSentence} {memberNames.length === 1 ? 'is' : 'are'} linked into the same household because they share postal address <strong>{postalOnly}</strong> and match on {matchKindWord}{avgConfPct ? <> with <strong>{avgConfPct}</strong> average confidence</> : null}.
                          </span>
                        </div>
                      </div>

                      <div className="cc-kpi-row cc-kpi-row--five" aria-label="Household summary">
                        <div className="cc-kpi-card">
                          <div className="cc-kpi-label">Household members</div>
                          <div className="cc-kpi-value">{household.member_count}</div>
                          <div className="cc-kpi-desc">Individuals sharing this address</div>
                        </div>
                        <div className="cc-kpi-card">
                          <div className="cc-kpi-label">Why linked</div>
                          <div className="cc-kpi-value cc-kpi-value--sm">
                            {household.household_type === 'SAME_SURNAME' ? 'Shared address + same last name' : 'Shared address + fuzzy last name'}
                          </div>
                          <div className="cc-kpi-desc">
                            {(() => {
                              const detIsh = primaryEdge ? isDeterministicLike(primaryRule?.match_type, primaryEdge.rule_id) : false;
                              const typeWord = detIsh ? 'deterministic' : (primaryRuleType.toLowerCase() || 'probabilistic');
                              return primaryRuleShort ? `${primaryRuleShort} ${typeWord}` : `${typeWord} match`;
                            })()}
                          </div>
                        </div>
                        <div className="cc-kpi-card">
                          <div className="cc-kpi-label">Household confidence</div>
                          <div className="cc-kpi-value">{avgConfPct ?? '—'}</div>
                          <div className="cc-kpi-desc">Avg over household edges</div>
                        </div>
                        <div className="cc-kpi-card">
                          <div className="cc-kpi-label">Identity confidence</div>
                          <div className="cc-kpi-value">
                            {profile.confidence_score != null ? `${(profile.confidence_score * 100).toFixed(1)}%` : '—'}
                          </div>
                          <div className="cc-kpi-desc">This identity within the household</div>
                        </div>
                        <div className="cc-kpi-card">
                          <div className="cc-kpi-label">Risk level</div>
                          <div className={`cc-kpi-value hh-risk hh-risk--${(aiRiskSignals?.risk_level ?? risk.level).toLowerCase()}`}>
                            {aiRiskLoading ? '...' : (aiRiskSignals?.risk_level ?? risk.level)}
                          </div>
                          <div className="cc-kpi-desc">{aiRiskSignals?.recommendation ?? risk.reason}</div>
                        </div>
                      </div>

                      <div className="hh-grid-2col">
                      <div className="panel">
                        <div className="panel-title-row">
                          <h2>Household Graph</h2>
                          <div className="panel-hint-wrap">
                            <button
                              type="button"
                              className="panel-hint-btn"
                              aria-label="About the household graph"
                              aria-describedby="household-graph-tooltip"
                            >
                              <svg
                                className="panel-hint-icon-svg"
                                width="16"
                                height="16"
                                viewBox="0 0 24 24"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="2"
                                strokeLinecap="round"
                                strokeLinejoin="round"
                                aria-hidden
                              >
                                <circle cx="12" cy="12" r="10" />
                                <path d="M12 16v-4M12 8h.01" />
                              </svg>
                            </button>
                            <div
                              id="household-graph-tooltip"
                              className="panel-hint-tooltip panel-hint-tooltip--below"
                              role="tooltip"
                            >
                              <p>
                                The center node is the current identity; co-members appear around it.
                                Edge labels show the rule that linked the pair: solid edge = R14
                                (exact last name + address); dashed edge = R15 (fuzzy last name + address).
                                Click any node to navigate to that identity.
                              </p>
                            </div>
                          </div>
                        </div>
                        <div className="household-graph-wrap">
                          <HouseholdGraph
                            household={household}
                            addressLabel={
                              household.shared_postal ||
                              [household.shared_city, household.shared_state].filter(Boolean).join(', ') ||
                              undefined
                            }
                            onMemberClick={(id) => { window.location.hash = `#/identity/${id}`; }}
                          />
                          <div className="household-graph-legend" aria-label="Edge legend">
                            {legendRules.length === 0 ? (
                              <span className="household-graph-legend-item">No edges</span>
                            ) : legendRules.map(lr => (
                              <span key={lr.ruleId} className="household-graph-legend-item">
                                <svg width="28" height="6" aria-hidden>
                                  <line x1="0" y1="3" x2="28" y2="3" stroke="var(--border-strong)" strokeWidth="2" {...(lr.dashed ? { strokeDasharray: '6 4' } : {})} />
                                </svg>
                                {lr.ruleId} — {lr.label}
                              </span>
                            ))}
                          </div>
                        </div>
                      </div>

                      <div className="panel">
                        <div className="panel-title-row">
                          <h2>Why this household exists</h2>
                          {primaryRule && (
                            <span className={`cc-overview-pill ${primaryEdge && isDeterministicLike(primaryRule?.match_type, primaryEdge.rule_id) ? 'cc-overview-pill--merge' : 'cc-overview-pill--household'}`}>
                              {primaryEdge && isDeterministicLike(primaryRule?.match_type, primaryEdge.rule_id) ? 'Deterministic' : (primaryRuleType.charAt(0).toUpperCase() + primaryRuleType.slice(1).toLowerCase())}
                            </span>
                          )}
                        </div>
                        <div className="hh-why-grid">
                          <div className="hh-why-rule">
                            <div className="hh-why-rule-id mono">{primaryEdge?.rule_id ?? '—'}</div>
                            <div className="hh-why-rule-desc">
                              {stripRunBy(primaryRule?.rule_description)
                                ?? (household.household_type === 'SAME_SURNAME'
                                  ? 'Two clusters share the same postal address and an exact last name.'
                                  : 'Two clusters share the same postal address and a fuzzy last name match.')}
                            </div>
                            <div className="hh-why-rule-meta">
                              <span>Type: <strong>{primaryRuleType}</strong></span>
                              {primaryEdge?.match_score != null && <span>Score: <strong>{primaryEdge.match_score.toFixed(3)}</strong></span>}
                              {primaryEdge?.jw_last != null && <span>JW(last): <strong>{primaryEdge.jw_last.toFixed(3)}</strong></span>}
                              {primaryRule?.base_score != null && <span>Base: <strong>{(primaryRule.base_score * 100).toFixed(0)}%</strong></span>}
                              {addressLine && <span>Address: <strong className="mono">{addressLine}</strong></span>}
                            </div>
                          </div>
                          <hr className="hh-why-divider" />
                          <div className="hh-why-risks">
                            <h3 className="hh-why-risks-title">Risk signals</h3>
                            {aiRiskLoading ? (
                              <div className="ai-risk-loading">
                                <div className="ai-risk-spinner" />
                                <span>Analyzing household risk...</span>
                              </div>
                            ) : aiRiskSignals?.signals ? (
                              <>
                                <ul className="hh-why-risks-list">
                                  {aiRiskSignals.signals.map((s, i) => (
                                    <li key={i} className={`ai-risk-signal ai-risk-signal--${s.severity}`}>
                                      <strong>{s.signal}</strong>
                                      {s.detail && <span className="ai-risk-detail"> — {s.detail}</span>}
                                    </li>
                                  ))}
                                </ul>
                                {aiRiskSignals.recommendation && (
                                  <p className="ai-risk-recommendation">{aiRiskSignals.recommendation}</p>
                                )}
                                {aiRiskSignals.prompt && (
                                  <button
                                    type="button"
                                    className="tab-inline-link idr-table-toggle"
                                    onClick={() => setShowAIPromptModal(true)}
                                  >
                                    View prompt &rarr;
                                  </button>
                                )}
                              </>
                            ) : (
                              <ul className="hh-why-risks-list">
                                {riskSignals.map((s, i) => <li key={i}>{s}</li>)}
                              </ul>
                            )}
                          </div>
                        </div>
                      </div>
                      </div>

                      <div className="hh-grid-2col">
                      {household.edges.length > 0 && (                        <div className="panel">
                          <div className="panel-title-row">
                            <h2>Linking Rules</h2>
                            <span className="cc-overview-pill">{household.edges.length} edge{household.edges.length === 1 ? '' : 's'}</span>
                          </div>
                          <table className="identity-table full">
                            <thead>
                              <tr>
                                <th>Members</th>
                                <th>Rule</th>
                                <th>Description</th>
                                <th className="num">Score</th>
                              </tr>
                            </thead>
                            <tbody>
                              {household.edges.map((e, i) => {
                                const r = ruleLookup.get(e.rule_id);
                                const ruleClass = e.rule_id === 'MARTECH_R14'
                                  ? 'cc-overview-pill cc-overview-pill--merge'
                                  : e.rule_id === 'MARTECH_R15'
                                  ? 'cc-overview-pill cc-overview-pill--household'
                                  : 'cc-overview-pill';
                                const nameA = memberNameFor(e.a_cluster_id);
                                const nameB = memberNameFor(e.b_cluster_id);
                                return (
                                  <tr key={i}>
                                    <td className="hh-rule-members" title={`${e.a_cluster_id} ↔ ${e.b_cluster_id}`}>{nameA} ↔ {nameB}</td>
                                    <td><span className={ruleClass}>{e.rule_id}</span></td>
                                    <td className="hh-rule-desc">{stripRunBy(r?.rule_description) ?? (matchingRulesLoading ? 'Loading…' : '—')}</td>
                                    <td className="num mono">{e.match_score != null ? e.match_score.toFixed(3) : '—'}</td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      )}

                      <div className="panel">
                        <div className="panel-title-row">
                          <h2>Household Timeline</h2>
                        </div>
                        {timeline.length === 0 ? (
                          <div className="empty-hint">No timeline events available for this household.</div>
                        ) : (
                          <ol className="hh-timeline">
                            {timeline.map((item, i) => (
                              <li key={i} className={`hh-timeline-item hh-timeline-item--${item.tone}`}>
                                <span className="hh-timeline-marker" />
                                <div className="hh-timeline-content">
                                  <div className="hh-timeline-header">
                                    <span className="hh-timeline-label">{item.label}</span>
                                    <span className="hh-timeline-date">{fmtDateTime(item.ts)}</span>
                                  </div>
                                  <p className="hh-timeline-detail">{item.detail}</p>
                                </div>
                              </li>
                            ))}
                          </ol>
                        )}
                        <div className="hh-table-footnote">
                          Timeline events are derived from household first/last seen and edge rule fires.
                          A dedicated <code>/households/&#123;id&#125;/timeline</code> endpoint will provide
                          ingestion- and approval-level events.
                        </div>
                      </div>
                      </div>
                    </>
                  );
                })()}
              </>
            )}
          </div>
        )}

        {tab === 'lineage' && (
          <div className="lineage-tab">
            {lineageLoading && <div className="loading">Loading resolution history...</div>}
            {lineage && (
              <LineageTimeline lineage={lineage} />
            )}
          </div>
        )}

        {tab === 'ai' && (
          <div className="ai-tab">
            {aiLoading && <div className="loading">Generating AI summary...</div>}
            {aiSummary && (
              <div className="ai-summary-card">
                <div className="ai-icon">AI</div>
                <div className="ai-content">
                  <h2>Identity Profile Summary</h2>
                  <p className="ai-text">{aiSummary.summary}</p>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {selectedRecord && (
        <SourceRecordModal record={selectedRecord} onClose={() => setSelectedRecord(null)} />
      )}
      {showMatchingRulesModal && (
        <MatchingRulesModal
          rules={matchingRules ?? []}
          isLoading={matchingRulesLoading}
          onClose={() => setShowMatchingRulesModal(false)}
        />
      )}
      {showTopRulesModal && explanation && (
        <TopRulesModal
          matches={explanation.matches}
          matchingRules={matchingRules}
          onClose={() => setShowTopRulesModal(false)}
        />
      )}
      {showAIPromptModal && aiRiskSignals?.prompt && (
        <div className="modal-overlay" onClick={() => setShowAIPromptModal(false)}>
          <div className="modal modal-ai-prompt" onClick={e => e.stopPropagation()}>
            <div className="modal-header">
              <h2 className="modal-title">AI Risk Signal Prompt</h2>
              <button type="button" className="modal-close" onClick={() => setShowAIPromptModal(false)} aria-label="Close">✕</button>
            </div>
            <div className="modal-body">
              <pre className="ai-prompt-pre">{aiRiskSignals.prompt}</pre>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function getConfidenceClass(score: number | null): string {
  if (score == null) return '';
  if (score >= 0.95) return 'high';
  if (score >= 0.85) return 'medium';
  return 'low';
}

function MatchingRulesModal({ rules, isLoading, onClose }: {
  rules: MatchingRule[];
  isLoading: boolean;
  onClose: () => void;
}) {
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [onClose]);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-matching-rules" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <h2 className="modal-title matching-rules-modal-title">
            <BookOpen size={16} strokeWidth={2} aria-hidden />
            IDR Matching Rules
          </h2>
          <button type="button" className="modal-close" onClick={onClose} aria-label="Close">✕</button>
        </div>
        <div className="modal-body">
          {isLoading ? (
            <div className="loading">Loading matching rules…</div>
          ) : rules.length === 0 ? (
            <div className="empty-hint">No matching rules found.</div>
          ) : (
            <table className="matching-rules-table">
              <thead>
                <tr>
                  <th>Rule ID</th>
                  <th>Rule Name</th>
                  <th>Description</th>
                  <th>Type</th>
                  <th>Base Score</th>
                </tr>
              </thead>
              <tbody>
                {rules.map(r => (
                  <tr key={r.rule_id} className={r.is_active === false ? 'rule-inactive' : ''}>
                    <td><span className="mono">{r.rule_id}</span></td>
                    <td>{r.rule_name}</td>
                    <td className="rule-desc">{r.rule_description ?? '—'}</td>
                    <td>
                      <span className={`rule-type-badge rule-type-${(r.match_type ?? 'unknown').toLowerCase()}`}>
                        {r.match_type ?? '—'}
                      </span>
                    </td>
                    <td>{r.base_score != null ? `${(r.base_score * 100).toFixed(0)}%` : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}
