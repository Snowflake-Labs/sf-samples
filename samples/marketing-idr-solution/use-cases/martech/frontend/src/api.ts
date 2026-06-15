import axios from 'axios';

const api = axios.create({ baseURL: '/api' });

export interface IdentityProfile {
  identity_id: string;
  display_name: string | null;
  primary_hem: string | null;
  primary_uid2: string | null;
  primary_rampid: string | null;
  primary_ppid: string | null;
  primary_device_id: string | null;
  primary_cookie: string | null;
  device_count: number | null;
  id_type_count: number | null;
  source_count: number | null;
  /** Distinct SILVER.IDR_CORE_IDENTIFIER_LINK.source_type values for this cluster (list endpoint only). */
  source_types?: string[] | null;
  /** UUID of the household this individual belongs to (list endpoint only). */
  household_id?: string | null;
  geo_country: string | null;
  geo_metro: string | null;
  /** ML weak-link rows involving this cluster (list endpoint). */
  connected_cluster_count?: number | null;
  confidence_score: number | null;
  first_seen: string | null;
  last_seen: string | null;
}

export interface IdentityListResponse {
  identities: IdentityProfile[];
  total: number;
  showing: number;
  page: number;
  page_size: number;
}

export type IdentitySortField =
  | 'confidence'
  | 'last_seen'
  | 'first_seen'
  | 'display_name'
  | 'identity_id'
  | 'source_count'
  | 'device_count'
  | 'id_type_count'
  | 'connected_clusters'
  | 'household_id';

export type IdentityConfidenceBand = 'high' | 'medium' | 'low';
export type IdentityConnectedFilter = 'yes' | 'no';

export interface IdentityFilterSourceOption {
  value: string;
  label: string;
  priority?: number | null;
}

export interface IdentityFilterSourcesResponse {
  sources: IdentityFilterSourceOption[];
  ui_mode: 'checkbox' | 'multiselect';
  checkbox_threshold: number;
}

export const fetchIdentityFilterSources = () =>
  api.get<IdentityFilterSourcesResponse>('/identity-filter-sources').then(r => r.data);

export interface FetchIdentitiesParams {
  q?: string;
  page?: number;
  pageSize?: number;
  sortBy?: IdentitySortField;
  sortDir?: 'asc' | 'desc';
  /** Confidence bucket: high ≥90%, medium 60–90%, low &lt;60% or null */
  confidenceBand?: IdentityConfidenceBand | '';
  /** Weak-link neighbor count: yes = &gt;0, no = 0 */
  connectedClusters?: IdentityConnectedFilter | '';
  /** Source table names from identity-filter-sources (OR; server maps to link source types) */
  filterSourceTypes?: string[];
  minDevices?: number | '';
}

export interface LinkedIdentifier {
  identifier_type: string;
  identifier_value: string;
  source_system: string;
  is_primary: boolean;
}

export interface IdentityDetail {
  profile: IdentityProfile;
  identifiers: LinkedIdentifier[];
  tapad_person_ids: string | null;
  experian_person_ids: string | null;
  household_ids: string | null;
  audience_segments: string | null;
}

export interface SourceRecord {
  record_id: string;
  source: string;
  name: string | null;
  identifiers: string[] | null;
}

export interface MatchExplanation {
  identifier_1: string;
  identifier_2: string;
  match_score: number;
  match_rule?: string;
  rule_description?: string;
  last_seen?: string | null;
  match_details?: { identifier_1: string | null; identifier_2: string | null };
}

export interface WeakLinkSignal {
  label: string;
  value: string;
}

/** One co-observation row from IDR_ML_CANDIDATE_PAIR_EVIDENCE (backend). */
export interface WeakLinkPairEvidence {
  evidence_type: string;
  device_fingerprint?: string | null;
  source_record_id_a?: string | null;
  source_record_id_b?: string | null;
  /** IDR_CORE_CLUSTER id that owns source_record_id_a (when resolved). */
  record_cluster_id_a?: string | null;
  /** IDR_CORE_CLUSTER id that owns source_record_id_b (when resolved). */
  record_cluster_id_b?: string | null;
  observed_at?: string | null;
  publisher_id_a?: string | null;
  publisher_id_b?: string | null;
  fingerprint_rarity?: number | null;
  /** SCORE_SNAPSHOT: ML_SCORE at observation time. */
  ml_score?: number | null;
  /** SCORE_SNAPSHOT: FEATURE_VALUES (score / feature payload). */
  score_snapshot_details?: Record<string, unknown> | null;
}

export interface WeakLink {
  pair_id: string;
  cluster_id_a: string;
  cluster_id_b: string;
  ml_score: number | null;
  status: string;
  ml_tier?: string | null;
  confidence_level?: string | null;
  fingerprint_overlap_count: number;
  last_seen?: string | null;
  score_details: Record<string, unknown> | null;
  /** Server-derived bullets from SCORE_DETAILS (summaries, importances, top features). */
  reasoning_lines?: string[];
  /** Plain-language interpretation of the ML score and signals. */
  english_summary?: string | null;
  /** Structured breakdown of ML explanation for scannable UI. */
  structured_explanation?: {
    verdict: string;
    evidence: string[];
    supporting: { feature: string; shap: number; strength: string; explanation?: string | null }[];
    opposing: { feature: string; shap: number; strength: string; explanation?: string | null }[];
    context?: string | null;
    recommendation: string;
    rec_level: string;
  } | null;
  /** Formatted metric rows (friendly labels + display values). */
  signal_rows?: WeakLinkSignal[];
  /** Latest FINGERPRINT_MATCH row (participating source records / device). */
  fingerprint_match_evidence?: WeakLinkPairEvidence | null;
  /** Latest SCORE_SNAPSHOT (edge score + FEATURE_VALUES for that scoring moment). */
  score_snapshot_evidence?: WeakLinkPairEvidence | null;
  /** Graph: at most one fingerprint-match row (same as fingerprint_match_evidence). */
  pair_evidence?: WeakLinkPairEvidence[];
  /** All SOURCE_RECORD_IDS on cluster_id_a from IDR_CORE_CLUSTER (active). */
  cluster_a_events?: string[];
  /** All SOURCE_RECORD_IDS on cluster_id_b from IDR_CORE_CLUSTER (active). */
  cluster_b_events?: string[];
}

/** One row from GET /identities/{id}/connected-clusters-graph (ego graph bundle). */
export interface ConnectedClusterGraphPair {
  pair_id: string;
  cluster_id_a: string;
  cluster_id_b: string;
  partner_cluster_id: string;
  ml_score: number | null;
  status: string;
  ml_tier?: string | null;
  confidence_level?: string | null;
  fingerprint_overlap_count: number;
  last_seen?: string | null;
  score_details: Record<string, unknown> | null;
  english_summary?: string | null;
  structured_explanation?: {
    verdict: string;
    evidence: string[];
    supporting: { feature: string; shap: number; strength: string; explanation?: string | null }[];
    opposing: { feature: string; shap: number; strength: string; explanation?: string | null }[];
    context?: string | null;
    recommendation: string;
    rec_level: string;
  } | null;
  cluster_a_source_record_ids: string[];
  cluster_b_source_record_ids: string[];
  fingerprint_matches: WeakLinkPairEvidence[];
  score_snapshot?: WeakLinkPairEvidence | null;
}

export interface ConnectedClustersGraphBundle {
  ego_cluster_id: string;
  /** BFS hop from ego: 0 = ego, 1 = direct partners, 2+ = expanded. */
  cluster_depth?: Record<string, number>;
  /** BFS tree parent per cluster (`null` for ego). */
  cluster_parent?: Record<string, string | null>;
  pairs: ConnectedClusterGraphPair[];
}

/** Map graph bundle pair → WeakLink for shared graph / card helpers. */
export function graphBundlePairToWeakLink(
  _egoClusterId: string,
  p: ConnectedClusterGraphPair,
): WeakLink {
  const fps = p.fingerprint_matches ?? [];
  const fp = fps[0] ?? null;
  const snap = p.score_snapshot ?? null;
  return {
    pair_id: p.pair_id,
    cluster_id_a: p.cluster_id_a,
    cluster_id_b: p.cluster_id_b,
    ml_score: snap?.ml_score ?? p.ml_score ?? null,
    status: p.status,
    ml_tier: p.ml_tier ?? null,
    confidence_level: p.confidence_level ?? null,
    fingerprint_overlap_count: p.fingerprint_overlap_count,
    last_seen: p.last_seen ?? null,
    score_details: (snap?.score_snapshot_details as Record<string, unknown> | null) ?? p.score_details ?? null,
    reasoning_lines: [],
    english_summary: p.english_summary ?? null,
    structured_explanation: p.structured_explanation ?? null,
    signal_rows: [],
    fingerprint_match_evidence: fp,
    score_snapshot_evidence: snap,
    pair_evidence: fps.length ? fps : fp ? [fp] : [],
    cluster_a_events: p.cluster_a_source_record_ids,
    cluster_b_events: p.cluster_b_source_record_ids,
  };
}

export interface IDRExplanation {
  identity_id: string;
  cluster_confidence: number | null;
  rules_applied: string[];
  total_identifiers: number;
  source_systems: string[];
  matches: MatchExplanation[];
  source_records: SourceRecord[];
}

export interface MatchingRule {
  rule_id: string;
  rule_name: string;
  rule_description: string | null;
  rule_priority: number | null;
  match_type: string | null;
  anchor_field: string | null;
  exact_match_fields: string | null;
  fuzzy_match_field: string | null;
  fuzzy_algorithm: string | null;
  fuzzy_threshold: number | null;
  use_fuzzy_score: boolean | null;
  base_score: number | null;
  auto_match_threshold: number | null;
  require_cross_source: boolean | null;
  require_different_record: boolean | null;
  is_active: boolean | null;
}

export interface AISummary {
  summary: string;
}

export interface AIRiskSignal {
  signal: string;
  severity: 'info' | 'warning' | 'critical';
  detail: string;
}

export interface AIRiskSignals {
  risk_level: 'Low' | 'Medium' | 'High' | 'Unknown';
  risk_score: number | null;
  signals: AIRiskSignal[];
  recommendation: string;
  prompt?: string;
}

export interface ConfidenceBuckets {
  high: number;
  medium: number;
  low: number;
}

export interface RunDelta {
  run_id: string | null;
  new_clusters: number | null;
  updated_clusters: number | null;
  merged_clusters: number | null;
  /** ML cluster-to-cluster candidates from the latest IDR_COMPLETE (when present). */
  candidate_matches?: number | null;
}

export interface MergeSourceShare {
  label: string;
  pct: number;
  weight: number;
}

export interface RuleDriver {
  rule_label: string;
  count: number;
}

export interface OperationalAlert {
  key: string;
  title: string;
  detail: string;
  severity: 'warn' | 'info';
}

export interface IdentityHealth {
  score: number;
  summary: string;
}

export interface DashboardStats {
  total_identities: number;
  total_clusters: number;
  total_source_records: number;
  /** 0 = dashboard pipeline tiles are all-time sums; Data to Identity still uses /pipeline-sankey minutes. */
  pipeline_flow_window_minutes: number;
  /** All-time sum of ingestion-style counts from every IDR_COMPLETE payload (capped server-side). */
  pipeline_events_ingested: number;
  /** All-time sum of profiles.created from those completions (same cap). */
  pipeline_clusters_created: number;
  global_dedupe_ratio: number | null;
  avg_confidence: number | null;
  source_breakdown: Record<string, number>;
  id_type_breakdown: Record<string, number>;
  confidence_buckets: ConfidenceBuckets;
  run_delta: RunDelta;
  merge_source_shares: MergeSourceShare[];
  rule_drivers: RuleDriver[];
  alerts: OperationalAlert[];
  health: IdentityHealth;
  /** GET /api/dashboard?profile=true — per parallel backend query (ms). */
  timings_ms?: Record<string, number> | null;
  dashboard_wall_ms?: number | null;
}

export interface LineageEvent {
  log_id: string;
  cluster_id: string;
  event_type: string;
  source_record_ids: string | null;
  previous_source_record_ids: string | null;
  merged_from_clusters: string | null;
  event_details: Record<string, unknown> | null;
  created_at: string | null;
  match_id: string | null;
  match_details: Record<string, unknown> | null;
}

export interface LineageResponse {
  cluster_id: string | null;
  lineage: LineageEvent[];
  merged_clusters: LineageEvent[];
  source_records: Record<string, { record_id: string; source: string; name: string }>;
}

export interface SourceRecordIdentifier {
  type: string;
  value: string;
}

export interface SourceRecordDetail {
  record_id: string;
  source_type: string;
  identifiers: SourceRecordIdentifier[];
  created_at: string | null;
  raw_payload_json?: string | null;
  raw_payload_origin?: string | null;
  source_attributes?: Record<string, string | null> | null;
}

export const fetchDashboard = (
  runId?: string | null,
  opts?: { profile?: boolean },
) => {
  const params: Record<string, string | number> = {};
  if (runId) params.run_id = runId;
  // FastAPI bool query accepts 1 / true; use a number so the query string is always ?profile=1
  if (opts?.profile) params.profile = 1;
  return api
    .get<DashboardStats>('/dashboard', { params: Object.keys(params).length ? params : undefined })
    .then(r => r.data);
};

export interface PipelineSankeyTotals {
  inserts: number;
  deletes: number;
  created: number;
  updated: number;
  merged: number;
  matches: number;
  ml_pairs: number;
}

export interface PipelineSankeyResponse {
  totals: PipelineSankeyTotals;
  by_source: Record<string, number>;
  by_match_rule: Record<string, number>;
  run_count: number;
}

export const fetchPipelineSankey = (minutes: number) =>
  api.get<PipelineSankeyResponse>('/pipeline-sankey', { params: { minutes } }).then(r => r.data);

export const fetchIdentities = (
  params: FetchIdentitiesParams = {},
  options?: { signal?: AbortSignal },
) => {
  const band = params.confidenceBand || undefined;
  const connected = params.connectedClusters || undefined;
  const minDev = params.minDevices === '' || params.minDevices == null ? undefined : params.minDevices;
  const srcCsv =
    params.filterSourceTypes && params.filterSourceTypes.length > 0
      ? params.filterSourceTypes.join(',')
      : undefined;
  return api
    .get<IdentityListResponse>('/identities', {
      signal: options?.signal,
      params: {
        q: params.q?.trim() || undefined,
        page: params.page ?? 1,
        page_size: params.pageSize ?? 25,
        sort_by: params.sortBy ?? 'confidence',
        sort_dir: params.sortDir ?? 'desc',
        // Snake + camel: some proxies only forward one naming style.
        confidence_band: band,
        confidenceBand: band,
        connected_clusters: connected,
        connectedClusters: connected,
        filter_source_types: srcCsv,
        filterSourceTypes: srcCsv,
        min_devices: minDev,
        minDevices: minDev,
      },
    })
    .then(r => r.data);
};

export const fetchIdentity = (id: string) =>
  api.get<IdentityDetail>(`/identities/${id}`).then(r => r.data);

export interface HouseholdMember {
  cluster_id: string;
  primary_first_name: string | null;
  primary_last_name: string | null;
  primary_email: string | null;
  primary_phone: string | null;
  lifetime_total_spend: number | null;
  confidence_score: number | null;
  is_current: boolean;
}

export interface HouseholdEdge {
  a_cluster_id: string;
  b_cluster_id: string;
  rule_id: string;
  match_score: number | null;
  jw_last: number | null;
}

export interface HouseholdInfo {
  household_id: string;
  household_type: 'SAME_SURNAME' | 'FUZZY_SURNAME';
  shared_street: string | null;
  shared_city: string | null;
  shared_state: string | null;
  shared_postal: string | null;
  member_count: number;
  primary_last_name: string | null;
  household_lifetime_spend: number;
  avg_member_confidence: number | null;
  first_seen: string | null;
  last_seen: string | null;
  members: HouseholdMember[];
  edges: HouseholdEdge[];
}

export const fetchIdentityHousehold = (id: string) =>
  api.get<HouseholdInfo | null>(`/identities/${id}/household`).then(r => r.data);

export const fetchHousehold = (householdId: string) =>
  api.get<HouseholdInfo>(`/households/${householdId}`).then(r => r.data);

export const fetchIDRExplanation = (id: string) =>
  api.get<IDRExplanation>(`/identities/${id}/idr-explanation`).then(r => r.data);

export const fetchMatchingRules = () =>
  api.get<MatchingRule[]>('/config/matching-rules').then(r => r.data);

export const fetchWeakLinks = (id: string) =>
  api.get<WeakLink[]>(`/identities/${id}/weak-links`).then(r => r.data);

export interface LLMReview {
  review_id: string;
  pair_id: string;
  cluster_id_a: string;
  cluster_id_b: string;
  partner_cluster_id?: string;
  ml_score?: number;
  ml_tier?: string;
  llm_decision?: string;
  llm_confidence?: number;
  llm_reasoning?: string;
  status: string;
  created_at?: string;
  model_used?: string;
}

export interface LLMThresholds {
  score_low: number;
  score_high: number;
  batch_size: number;
  model: string;
  available_models: string[];
}

export const fetchLLMReviews = (id: string) =>
  api.get<LLMReview[]>(`/identities/${id}/llm-reviews`).then(r => r.data);

export const acceptLLMReview = (reviewId: string) =>
  api.post(`/llm-reviews/${reviewId}/accept`).then(r => r.data);

export const rejectLLMReview = (reviewId: string) =>
  api.post(`/llm-reviews/${reviewId}/reject`).then(r => r.data);

export const fetchLLMThresholds = () =>
  api.get<LLMThresholds>('/config/llm-thresholds').then(r => r.data);

export const updateLLMThresholds = (body: LLMThresholds) =>
  api.post<LLMThresholds>('/config/llm-thresholds', body).then(r => r.data);

// ─── Settings: Sources, Pipeline, Infrastructure, Display ─────────────────────

export interface SourceConfig {
  source: string;
  priority: number;
  description: string;
  pk_column: string;
  prefix: string;
  has_location: boolean;
}

export interface PipelineTask {
  task_name: string;
  display_name: string;
  schedule: string;
  warehouse: string;
  state: string;
}

export interface WarehouseInfo {
  name: string;
  size: string;
  auto_suspend: number;
  status: string;
}

export interface ComputePoolInfo {
  name: string;
  instance_family: string;
  min_nodes: number;
  max_nodes: number;
  auto_suspend: number;
  status: string;
}

export interface ServiceInfo {
  name: string;
  min_instances: number;
  max_instances: number;
  status: string;
}

export interface InfraConfig {
  warehouse: WarehouseInfo | null;
  compute_pool: ComputePoolInfo | null;
  service: ServiceInfo | null;
}

export interface DisplayConfig {
  confidence_high: number;
  confidence_medium: number;
  health_low_confidence_weight: number;
  health_vendor_tension_weight: number;
  health_probabilistic_weight: number;
  page_size_default: number;
}

export const fetchSources = () =>
  api.get<SourceConfig[]>('/config/sources').then(r => r.data);

export const fetchPipelineTasks = () =>
  api.get<PipelineTask[]>('/config/pipeline-tasks').then(r => r.data);

export const fetchInfrastructure = () =>
  api.get<InfraConfig>('/config/infrastructure').then(r => r.data);

export const fetchDisplayConfig = () =>
  api.get<DisplayConfig>('/config/display-thresholds').then(r => r.data);

export const demoLLMReview = (pairId: string) =>
  api.post<LLMReview>(`/demo/llm-review/${pairId}`).then(r => r.data);

export const fetchConnectedClustersGraph = (id: string, depth = 1) =>
  api
    .get<ConnectedClustersGraphBundle>(`/identities/${id}/connected-clusters-graph`, {
      params: { depth },
    })
    .then(r => r.data);

export const fetchLineage = (id: string) =>
  api.get<LineageResponse>(`/identities/${id}/lineage`).then(r => r.data);

export const fetchAISummary = (id: string) =>
  api.get<AISummary>(`/identities/${id}/ai-summary`).then(r => r.data);

export const fetchHouseholdAIRisk = (householdId: string, clusterId?: string) =>
  api.get<AIRiskSignals>(`/households/${householdId}/ai-risk-signals`, {
    params: clusterId ? { cluster_id: clusterId } : undefined,
  }).then(r => r.data);

export const fetchSourceRecords = (id: string) =>
  api.get<SourceRecordDetail[]>(`/identities/${id}/source-records`).then(r => r.data);

export interface SourceRecordPayload {
  raw_payload_json: string | null;
  raw_payload_origin: string | null;
  source_attributes: Record<string, string | null> | null;
}

export const fetchSourceRecordPayload = (recordId: string, sourceType: string) =>
  api
    .get<SourceRecordPayload>(`/source-record/${encodeURIComponent(recordId)}/payload`, {
      params: { source_type: sourceType },
    })
    .then(r => r.data);

// ─── Test Runner ─────────────────────────────────────────────────────────────

export interface TestResult {
  id: string;
  desc: string;
  status: 'PASS' | 'FAIL' | 'SKIP' | 'ERROR';
  actual?: unknown;
  expected?: unknown;
  op?: string;
  reason?: string;
  error?: string;
}

export interface TestRunResult {
  passed: number;
  failed: number;
  skipped: number;
  duration_ms: number;
  pipeline_result?: string;
  tests: TestResult[];
}

export interface TestRunStatus {
  status: 'RUNNING' | 'DONE' | 'FAILED';
  started_at?: string;
  finished_at?: string;
  result?: TestRunResult;
  error?: string;
}

export const triggerTests = (skipLlm = true, cleanup = true) =>
  api.post<{ run_id: string; status: string }>('/tests/run', null, { params: { skip_llm: skipLlm, cleanup } }).then(r => r.data);

export const fetchTestStatus = (runId: string) =>
  api.get<TestRunStatus>('/tests/status', { params: { run_id: runId } }).then(r => r.data);

