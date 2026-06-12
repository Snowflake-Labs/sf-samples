import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';
import { ResponsiveSankey } from '@nivo/sankey';
import { Copy, X, Info } from 'lucide-react';
import CopyButton from '../components/CopyButton';
import { readCssVar } from '../theme/readCssVar';
import { useResolvedTheme } from '../theme';

const api = axios.create({ baseURL: '/api' });

interface RunRecord {
  run_id: string;
  status: string;
  started_at: string | null;
  completed_at: string | null;
  records_processed: number | null;
  clusters_created: number | null;
  clusters_updated: number | null;
  clusters_merged: number | null;
  duration_seconds: number | null;
  ml_pairs: number | null;
  ml_upgraded: number | null;
}

interface TaskRun {
  task_name: string;
  state: string;
  scheduled_time: string | null;
  completed_time: string | null;
  duration_seconds: number | null;
  query_id: string | null;
  error_code: string | null;
  error_message: string | null;
  return_value: string | null;
}

interface SankeyResponse {
  totals: { inserts: number; deletes: number; created: number; updated: number; merged: number; matches: number; ml_pairs: number; ml_upgraded: number };
  by_source: Record<string, number>;
  by_match_rule: Record<string, number>;
  run_count: number;
}

const TIME_OPTIONS = [
  { label: 'Last 5 min', value: 5 },
  { label: 'Last 15 min', value: 15 },
  { label: 'Last 1 hour', value: 60 },
  { label: 'Last 1 day', value: 1440 },
  { label: 'Last 7 days', value: 10080 },
  { label: 'Last 30 days', value: 43200 },
] as const;

const fetchRunHistory = (minutes: number) =>
  api.get<RunRecord[]>('/run-history', { params: { minutes } }).then(r => r.data);

const fetchTaskRuns = (minutes: number) =>
  api.get<TaskRun[]>('/task-runs', { params: { minutes } }).then(r => r.data);

const fetchSankey = (minutes: number, runId?: string | null) =>
  api
    .get<SankeyResponse>('/pipeline-sankey', {
      params: runId ? { minutes, run_id: runId } : { minutes },
    })
    .then(r => r.data);

interface PipelineRunCluster {
  log_id: string | null;
  cluster_id: string | null;
  event_type: string;
  source_record_count: number;
  created_at: string | null;
  identity_id: string | null;
  name: string | null;
}

function clusterChipTargetId(c: PipelineRunCluster): string {
  const a = c.cluster_id != null ? String(c.cluster_id).trim() : '';
  const b = c.identity_id != null ? String(c.identity_id).trim() : '';
  return a || b;
}

type RunClusterFetchParams =
  | number
  | { timestamp: string; window_ms?: number }
  | { run_id: string };

function fetchPipelineRunClusters(eventType: string, params: RunClusterFetchParams) {
  if (typeof params === 'number') {
    return api
      .get<PipelineRunCluster[]>('/pipeline/run-clusters', {
        params: { event_type: eventType, minutes: params },
      })
      .then(r => r.data);
  }
  if ('run_id' in params) {
    return api
      .get<PipelineRunCluster[]>('/pipeline/run-clusters', {
        params: { event_type: eventType, run_id: params.run_id },
      })
      .then(r => r.data);
  }
  const { timestamp, window_ms = 180_000 } = params;
  return api
    .get<PipelineRunCluster[]>('/pipeline/run-clusters', {
      params: { event_type: eventType, timestamp, window_ms },
    })
    .then(r => r.data);
}

function coerceIsoTimestamp(value: string | null | undefined): string | null {
  if (value == null) return null;
  const s = typeof value === 'string' ? value : String(value);
  const t = s.trim();
  return t.length > 0 ? t : null;
}

function formatDuration(seconds: number | null) {
  if (seconds === null) return '—';
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
}

function formatWhen(iso: string | null) {
  if (!iso) return '—';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : d.toLocaleString();
}

function taskStateBadge(state: string) {
  const cls = state === 'SUCCEEDED' ? 'badge-success'
    : state === 'FAILED' ? 'badge-error'
    : state === 'EXECUTING' ? 'badge-executing'
    : 'badge-default';
  return <span className={`status-badge ${cls}`}>{state}</span>;
}

function statusBadge(status: string) {
  const cls = status === 'COMPLETED' ? 'badge-success'
    : status === 'RUNNING' ? 'badge-running'
    : status === 'FAILED' ? 'badge-error'
    : 'badge-default';
  return <span className={`status-badge ${cls}`}>{status}</span>;
}

type SankeyDrilldownKey = 'created' | 'updated' | 'merged' | 'ml_pairs';

/** Main Sankey nodes only (ML is a separate post-cluster callout, not a fourth flow split). */
const SANKEY_NODE_TO_DRILLDOWN: Record<string, SankeyDrilldownKey> = {
  'New Clusters': 'created',
  Updated: 'updated',
  Merged: 'merged',
};

function SankeyPipelineDrilldown({
  drilldownType,
  minutes,
  pipelineRunId,
  onClose,
  onSelectIdentity,
}: {
  drilldownType: SankeyDrilldownKey | null;
  minutes: number;
  pipelineRunId: string | null;
  onClose: () => void;
  onSelectIdentity?: (identityId: string) => void;
}) {
  const typeMap: Record<SankeyDrilldownKey, string> = {
    created: 'CREATED',
    updated: 'UPDATED',
    merged: 'MERGED',
    ml_pairs: 'ML_PAIRS',
  };
  const labelMap: Record<SankeyDrilldownKey, string> = {
    created: 'New Clusters',
    updated: 'Updated',
    merged: 'Merged',
    ml_pairs: 'ML Pairs',
  };
  const theme = useResolvedTheme();
  const colorMap = useMemo<Record<SankeyDrilldownKey, string>>(
    () => ({
      created: readCssVar('--sankey-new'),
      updated: readCssVar('--sankey-updated'),
      merged: readCssVar('--sankey-merged'),
      ml_pairs: readCssVar('--sankey-ml'),
    }),
    [theme],
  );

  const { data: clusters, isLoading, isError, error } = useQuery({
    queryKey: pipelineRunId
      ? ['pipeline-run-clusters', drilldownType, 'run_id', pipelineRunId]
      : ['pipeline-run-clusters', drilldownType, 'minutes', minutes],
    queryFn: () =>
      pipelineRunId
        ? fetchPipelineRunClusters(typeMap[drilldownType!], { run_id: pipelineRunId })
        : fetchPipelineRunClusters(typeMap[drilldownType!], minutes),
    enabled: !!drilldownType && (!!pipelineRunId || minutes >= 1),
  });

  if (!drilldownType) return null;

  return (
    <div className="cluster-drilldown">
      <div className="cluster-drilldown-header">
        <span className="cluster-drilldown-badge" style={{ backgroundColor: colorMap[drilldownType] }}>
          {labelMap[drilldownType]}
        </span>
        <span className="cluster-drilldown-count">
          {isLoading
            ? 'Loading…'
            : drilldownType === 'ml_pairs'
              ? `${clusters?.length ?? 0} cluster${clusters?.length !== 1 ? 's' : ''}`
              : `${clusters?.length ?? 0} log event${clusters?.length !== 1 ? 's' : ''}`}
        </span>
        <button type="button" className="cluster-drilldown-close" onClick={onClose} aria-label="Close">
          <X size={14} />
        </button>
      </div>
      {isLoading ? (
        <div className="cluster-drilldown-loading">Loading clusters…</div>
      ) : isError ? (
        <p className="cluster-drilldown-empty">
          Could not load cluster log rows
          {error instanceof Error && error.message ? `: ${error.message}` : '.'}
        </p>
      ) : clusters && clusters.length > 0 ? (
        <div className="cluster-chip-grid">
          {clusters.map((c, idx) => {
            const targetId = clusterChipTargetId(c);
            const displayId = targetId || '(unknown id)';
            const showCopy = targetId.length > 0;
            return (
              <div
                key={c.log_id ?? `${targetId || 'row'}-${c.created_at ?? ''}-${idx}`}
                className={`cluster-chip${onSelectIdentity && targetId ? ' cluster-chip-clickable' : ''}`}
                style={{ borderColor: colorMap[drilldownType] }}
                role={onSelectIdentity && targetId ? 'button' : undefined}
                tabIndex={onSelectIdentity && targetId ? 0 : undefined}
                onClick={() => targetId && onSelectIdentity?.(targetId)}
                onKeyDown={e => {
                  if (onSelectIdentity && targetId && (e.key === 'Enter' || e.key === ' ')) {
                    e.preventDefault();
                    onSelectIdentity(targetId);
                  }
                }}
              >
                <span className="cluster-chip-id">
                  {displayId.length > 20 ? `${displayId.slice(0, 20)}…` : displayId}
                  {showCopy && (
                    <Copy
                      size={12}
                      className="copy-icon"
                      onClick={e => {
                        e.stopPropagation();
                        void navigator.clipboard.writeText(targetId);
                      }}
                    />
                  )}
                </span>
                {c.name && <span className="cluster-chip-name">{c.name}</span>}
                <span className="cluster-chip-meta">
                  {c.event_type === 'ML_PAIR'
                    ? `${c.source_record_count} pair row${c.source_record_count !== 1 ? 's' : ''} (this cluster)`
                    : `${c.source_record_count} source record${c.source_record_count !== 1 ? 's' : ''}`}
                </span>
                {onSelectIdentity && targetId && <span className="cluster-chip-action">Open identity →</span>}
              </div>
            );
          })}
        </div>
      ) : (
        <p className="cluster-drilldown-empty">
          {drilldownType === 'ml_pairs'
            ? pipelineRunId
              ? 'No clusters with cluster-to-cluster candidate activity in this run’s time window.'
              : 'No clusters with cluster-to-cluster candidate activity in this time range.'
            : pipelineRunId
              ? "No matching cluster log events during this pipeline run's time window."
              : 'No matching cluster log events in this time range.'}
        </p>
      )}
    </div>
  );
}

function PipelineSankey({
  data,
  drilldownType,
  setDrilldownType,
  minutes,
  selectedRunId,
  onSelectIdentity,
}: {
  data: SankeyResponse;
  drilldownType: SankeyDrilldownKey | null;
  setDrilldownType: (v: SankeyDrilldownKey | null) => void;
  minutes: number;
  selectedRunId: string | null;
  onSelectIdentity?: (identityId: string) => void;
}) {
  const theme = useResolvedTheme();
  const sankeyNodeColors = useMemo(() => {
    const c = readCssVar;
    return {
      'Events Ingested': c('--sankey-ingest'),
      'Identity Resolution': c('--sankey-identity'),
      'New Clusters': c('--sankey-new'),
      Updated: c('--sankey-updated'),
      Merged: c('--sankey-merged'),
    } as Record<string, string>;
  }, [theme]);
  const sankeyMlColor = useMemo(() => readCssVar('--sankey-ml'), [theme]);
  const sankeyUpgradeColor = useMemo(() => readCssVar('--sankey-new'), [theme]);
  const sankeyColorFallback = useMemo(() => readCssVar('--sankey-identity'), [theme]);

  const { totals, by_source, run_count } = data;
  const mlPairs = totals.ml_pairs ?? 0;
  const mlUpgraded = totals.ml_upgraded ?? 0;
  const sankeyData = useMemo(() => {
    if (run_count === 0) return null;
    const totalIn = totals.inserts + totals.deletes;
    if (totalIn === 0) return null;

    const nodes: { id: string; label: string }[] = [];
    const links: { source: string; target: string; value: number }[] = [];

    const sourceEntries = Object.entries(by_source).filter(([, v]) => v > 0);
    if (sourceEntries.length > 0) {
      for (const [tbl, cnt] of sourceEntries) {
        const short = tbl.replace(/_RAW$/, '');
        nodes.push({ id: tbl, label: `${short} (${cnt})` });
        links.push({ source: tbl, target: 'Events Ingested', value: cnt });
      }
    } else if (totals.inserts > 0) {
      nodes.push({ id: 'inserts', label: `Inserts (${totals.inserts})` });
      links.push({ source: 'inserts', target: 'Events Ingested', value: totals.inserts });
    }

    nodes.push({ id: 'Events Ingested', label: `Events Ingested (${totalIn})` });
    nodes.push({ id: 'Identity Resolution', label: 'Identity Resolution' });
    links.push({ source: 'Events Ingested', target: 'Identity Resolution', value: totalIn });

    if (totals.created > 0) {
      nodes.push({ id: 'New Clusters', label: `New Clusters (${totals.created})` });
      links.push({ source: 'Identity Resolution', target: 'New Clusters', value: totals.created });
    }
    if (totals.updated > 0) {
      nodes.push({ id: 'Updated', label: `Updated (${totals.updated})` });
      links.push({ source: 'Identity Resolution', target: 'Updated', value: totals.updated });
    }
    if (totals.merged > 0) {
      nodes.push({ id: 'Merged', label: `Merged (${totals.merged})` });
      links.push({ source: 'Identity Resolution', target: 'Merged', value: totals.merged });
    }

    if (links.length < 2) return null;
    return { nodes, links };
  }, [totals, by_source, run_count]);

  if (!sankeyData) return null;

  const drilldownScopedToRun = !!selectedRunId;
  const scopePhrase = drilldownScopedToRun
    ? 'this run’s start through end (plus a small buffer)'
    : 'the same time range as this chart';
  const aggregatePhrase =
    !drilldownScopedToRun && run_count > 1
      ? ` Totals aggregate ${run_count} completed runs in the selected window.`
      : '';

  return (
    <div className="sankey-section">
      <h3 className="sankey-title">
        Pipeline Flow —{' '}
        {selectedRunId
          ? 'selected run'
          : `${run_count} run${run_count !== 1 ? 's' : ''}`}
      </h3>
      <div className="sankey-how-to-read">
        <p className="sankey-how-to-read-title">How to read this chart</p>
        <ul className="sankey-how-to-read-list">
          <li>
            <strong>Left</strong> — Events ingested (bronze / standardization row counts). This is the closest thing to
            “X records touched this pipeline.”
          </li>
          <li>
            <strong>Center → New clusters / Updated / Merged</strong> —{' '}
            <strong>Cluster-level</strong> outcomes from identity resolution. They are <strong>not</strong> three buckets that add up to X; many records can join one cluster, and one
            update can touch one cluster.
          </li>
          <li>
            <strong>Cluster-to-cluster candidates</strong> (below) — A <strong>separate post-cluster stage</strong>: how
            many <strong>candidate pair rows</strong> were generated for ML scoring (each row links two cluster
            endpoints). Not “N clusters.”{aggregatePhrase}
          </li>
        </ul>
      </div>
      <p className="sankey-hint">
        Click <strong>New Clusters</strong>, <strong>Updated</strong>, or <strong>Merged</strong> to list matching
        cluster log rows for {scopePhrase}. Use{' '}
        <strong>View clusters</strong> in the ML panel to see distinct clusters that appear on those
        pair rows in the window.
      </p>
      <div className="sankey-ml-layout">
        <div className="sankey-chart sankey-chart-interactive">
          <ResponsiveSankey
            data={sankeyData}
          theme={{
            tooltip: {
              container: { fontSize: '10px', lineHeight: 1.35 },
              basic: { fontSize: '10px', lineHeight: 1.35 },
              chip: { fontSize: '10px' },
            },
          }}
          margin={{ top: 12, right: 160, bottom: 12, left: 160 }}
          align="justify"
          colors={(node: { id: string }) => sankeyNodeColors[node.id] || sankeyColorFallback}
          nodeOpacity={1}
          nodeThickness={18}
          nodeSpacing={24}
          nodeBorderWidth={0}
          nodeBorderRadius={3}
          linkOpacity={0.35}
          linkHoverOpacity={0.6}
          linkContract={3}
          enableLinkGradient
          label={(node: { id: string; label?: string }) => node.label || node.id}
          labelPosition="outside"
          labelOrientation="horizontal"
          labelPadding={12}
          labelTextColor={{ from: 'color', modifiers: [['darker', 1.2]] }}
          onClick={clickData => {
            let nodeId: string | null = null;
            if (clickData && typeof clickData === 'object' && 'id' in clickData) {
              nodeId = String((clickData as { id: string }).id);
            } else if (
              clickData &&
              typeof clickData === 'object' &&
              'target' in clickData &&
              (clickData as { target?: { id?: string } }).target?.id
            ) {
              nodeId = String((clickData as { target: { id: string } }).target.id);
            }
            const key = nodeId ? SANKEY_NODE_TO_DRILLDOWN[nodeId] : undefined;
            if (key) setDrilldownType(drilldownType === key ? null : key);
          }}
        />
        </div>
        <aside
          className={`sankey-ml-callout${drilldownType === 'ml_pairs' ? ' sankey-ml-callout-active' : ''}`}
          aria-label={`${mlPairs.toLocaleString()} cluster-to-cluster candidates generated`}
        >
          <div className="sankey-ml-callout-accent" style={{ backgroundColor: sankeyMlColor }} />
          <div className="sankey-ml-callout-body">
            <p className="sankey-ml-callout-kicker">After clustering</p>
            <p className="sankey-ml-callout-headline">
              <span className="sankey-ml-callout-num" style={{ color: sankeyMlColor }}>
                {mlPairs.toLocaleString()}
              </span>{' '}
              cluster-to-cluster candidates generated
            </p>
            {mlUpgraded > 0 && (
              <p className="sankey-ml-callout-headline">
                <span className="sankey-ml-callout-num" style={{ color: sankeyUpgradeColor }}>
                  {mlUpgraded.toLocaleString()}
                </span>{' '}
                pairs updated in confidence
              </p>
            )}
            <p className="sankey-ml-callout-desc">
              Candidate pair rows for this view — possible links between
              two clusters for ML evaluation. Not merged identities and not a slice of source-record count.
            </p>
            <button
              type="button"
              className="btn-secondary sankey-ml-callout-btn"
              onClick={() => setDrilldownType(drilldownType === 'ml_pairs' ? null : 'ml_pairs')}
            >
              {drilldownType === 'ml_pairs' ? 'Hide cluster list' : 'View clusters'}
            </button>
          </div>
        </aside>
      </div>
      <SankeyPipelineDrilldown
        drilldownType={drilldownType}
        minutes={minutes}
        pipelineRunId={selectedRunId}
        onClose={() => setDrilldownType(null)}
        onSelectIdentity={onSelectIdentity}
      />
    </div>
  );
}

type Tab = 'tasks' | 'pipeline';

interface StageInfo {
  stage: string;
  duration_ms: number | null;
  in_progress?: boolean;
}

const STAGE_LABELS: Record<string, string> = {
  consume_streams: 'Consume Streams',
  standardize: 'Standardize',
  custom_standardize: 'Custom Standardize',
  extract: 'Extract Identifiers',
  match: 'Matching',
  ml_scoring_only: 'ML Scoring',
  ml_scoring: 'ML + LLM (total)',
  llm_adjudication: 'LLM Adjudication',
  cluster: 'Clustering',
  profile_index: 'Profile Index',
  golden_records: 'Golden Records',
  total: 'Total',
};

function formatStageTime(ms: number | null): string {
  if (ms == null) return '…';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function StageTooltip({ queryId, isExecuting = false }: { queryId: string; isExecuting?: boolean }) {
  const [show, setShow] = useState(false);
  const { data: stages, isLoading } = useQuery<StageInfo[]>({
    queryKey: ['task-stages', queryId],
    queryFn: () => api.get<StageInfo[]>(`/task-runs/${queryId}/stages`).then(r => r.data),
    enabled: show,
    // While the run is executing, stages appear progressively — don't cache an
    // empty/partial result, and poll so an open tooltip updates live.
    staleTime: isExecuting ? 0 : 30000,
    refetchInterval: isExecuting && show ? 2000 : false,
  });

  return (
    <span className="stage-tooltip-wrap" onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}>
      <Info size={14} className="stage-tooltip-icon" />
      {show && (
        <div className="stage-tooltip-popup">
          <div className="stage-tooltip-title">Pipeline Stages</div>
          {isLoading && <div className="stage-tooltip-row"><span className="stage-tooltip-name">Loading…</span></div>}
          {stages && stages.filter(s => s.stage !== 'ml_scoring').map((s, i) => (
            <div key={i} className={`stage-tooltip-row ${s.in_progress ? 'in-progress' : ''}`}>
              <span className="stage-tooltip-name">{STAGE_LABELS[s.stage] || s.stage}</span>
              <span className="stage-tooltip-time">
                {s.in_progress ? '⏳' : formatStageTime(s.duration_ms)}
              </span>
            </div>
          ))}
          {stages && stages.length === 0 && !isLoading && (
            <div className="stage-tooltip-row">
              <span className="stage-tooltip-name">{isExecuting ? 'Starting…' : 'No stage data'}</span>
            </div>
          )}
        </div>
      )}
    </span>
  );
}

export default function RunHistory({
  onOpenGenerate,
  onOpenIdentities,
  onSelectIdentity,
  activeTab,
}: {
  onOpenGenerate?: () => void;
  onOpenIdentities?: () => void;
  onSelectIdentity?: (identityId: string) => void;
  activeTab?: string;
}) {
  const [tab, setTab] = useState<Tab>((activeTab as Tab) || 'tasks');
  useEffect(() => { if (activeTab && ['tasks', 'pipeline'].includes(activeTab)) setTab(activeTab as Tab); }, [activeTab]);
  const [minutes, setMinutes] = useState(43200);
  const [taskStateFilter, setTaskStateFilter] = useState<string>('all');
  const [pipelineStatusFilter, setPipelineStatusFilter] = useState<string>('all');
  const [sankeyDrilldown, setSankeyDrilldown] = useState<SankeyDrilldownKey | null>(null);
  const [selectedPipelineRun, setSelectedPipelineRun] = useState<RunRecord | null>(null);

  const { data: taskRuns, isLoading: taskLoading } = useQuery({
    queryKey: ['task-runs', minutes],
    queryFn: () => fetchTaskRuns(minutes),
    refetchInterval: 10000,
  });

  const { data: pipelineRuns, isLoading: pipelineLoading } = useQuery({
    queryKey: ['run-history', minutes],
    queryFn: () => fetchRunHistory(minutes),
    refetchInterval: 15000,
  });

  const { data: sankeyData } = useQuery({
    queryKey: ['pipeline-sankey', minutes, selectedPipelineRun?.run_id ?? null],
    queryFn: () => fetchSankey(minutes, selectedPipelineRun?.run_id ?? null),
    refetchInterval: 15000,
  });

  useEffect(() => {
    setSelectedPipelineRun(null);
    setSankeyDrilldown(null);
  }, [minutes]);

  const taskStateOptions = useMemo(() => {
    const set = new Set(taskRuns?.map(r => r.state) ?? []);
    return ['all', ...Array.from(set).sort()];
  }, [taskRuns]);

  const filteredTasks = useMemo(() => {
    if (!taskRuns?.length) return [];
    if (taskStateFilter === 'all') return taskRuns;
    return taskRuns.filter(r => r.state === taskStateFilter);
  }, [taskRuns, taskStateFilter]);

  const taskSummary = useMemo(() => {
    if (!taskRuns?.length) return { total: 0, succeeded: 0, failed: 0 };
    return {
      total: taskRuns.length,
      succeeded: taskRuns.filter(r => r.state === 'SUCCEEDED').length,
      failed: taskRuns.filter(r => r.state === 'FAILED').length,
    };
  }, [taskRuns]);

  const pipelineStatusOptions = useMemo(() => {
    const set = new Set(pipelineRuns?.map(r => r.status) ?? []);
    return ['all', ...Array.from(set).sort()];
  }, [pipelineRuns]);

  const filteredPipeline = useMemo(() => {
    if (!pipelineRuns?.length) return [];
    if (pipelineStatusFilter === 'all') return pipelineRuns;
    return pipelineRuns.filter(r => r.status === pipelineStatusFilter);
  }, [pipelineRuns, pipelineStatusFilter]);

  return (
    <div className="run-history">
      <header className="pfi-header">
        <div className="pfi-header-text">
          <div className="pfi-title-row">
            <h1>Run History</h1>
          </div>
        </div>
        <div className="pfi-header-actions">
          {onOpenGenerate && (
            <button type="button" className="btn-secondary" onClick={onOpenGenerate}>
              Generate Data
            </button>
          )}
          {onOpenIdentities && (
            <button type="button" className="btn-secondary" onClick={onOpenIdentities}>
              Identities
            </button>
          )}
        </div>
      </header>

      <div className="rh-controls">
        <div className="rh-tabs">
          <button
            type="button"
            className={`rh-tab${tab === 'tasks' ? ' active' : ''}`}
            onClick={() => setTab('tasks')}
          >
            Task Runs
            {taskRuns && <span className="rh-tab-count">{taskRuns.length}</span>}
          </button>
          <button
            type="button"
            className={`rh-tab${tab === 'pipeline' ? ' active' : ''}`}
            onClick={() => setTab('pipeline')}
          >
            Pipeline Events
            {pipelineRuns && <span className="rh-tab-count">{pipelineRuns.length}</span>}
          </button>
        </div>
        <div className="rh-time-filter">
          {TIME_OPTIONS.map(opt => (
            <button
              key={opt.value}
              type="button"
              className={`rh-time-btn${minutes === opt.value ? ' active' : ''}`}
              onClick={() => setMinutes(opt.value)}
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {tab === 'tasks' && (
        <>
          {taskRuns && taskRuns.length > 0 && (
            <div className="run-history-summary">
              <div className="rh-stat">
                <span className="rh-stat-val">{taskSummary.total}</span>
                <span className="rh-stat-label">Total runs</span>
              </div>
              <div className="rh-stat">
                <span className="rh-stat-val rh-val-success">{taskSummary.succeeded}</span>
                <span className="rh-stat-label">Succeeded</span>
              </div>
              {taskSummary.failed > 0 && (
                <div className="rh-stat">
                  <span className="rh-stat-val rh-val-error">{taskSummary.failed}</span>
                  <span className="rh-stat-label">Failed</span>
                </div>
              )}
            </div>
          )}

          <div className="run-history-toolbar">
            <label className="filter-label">
              State
              <select
                className="status-filter"
                value={taskStateFilter}
                onChange={e => setTaskStateFilter(e.target.value)}
              >
                {taskStateOptions.map(s => (
                  <option key={s} value={s}>{s === 'all' ? 'All states' : s}</option>
                ))}
              </select>
            </label>
          </div>

          {taskLoading && <div className="loading">Loading task runs…</div>}

          {taskRuns && taskRuns.length === 0 && !taskLoading && (
            <div className="empty-state run-history-empty">
              <p><strong>No task runs found in this time range.</strong></p>
              <p className="empty-detail">
                Task runs appear after <code className="inline-code">IDR_INCREMENTAL_TASK</code> is started.
                Try expanding the time range.
              </p>
            </div>
          )}

          {filteredTasks.length > 0 && (
            <div className="run-history-table-wrapper">
              <table className="run-history-table">
                <thead>
                  <tr>
                    <th>Scheduled Time</th>
                    <th>State</th>
                    <th>Completed</th>
                    <th>Duration</th>
                    <th>Query ID</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTasks.map((run, i) => (
                    <tr key={`${run.scheduled_time}-${i}`}>
                      <td>{formatWhen(run.scheduled_time)}</td>
                      <td>{taskStateBadge(run.state)}</td>
                      <td>{formatWhen(run.completed_time)}</td>
                      <td>{formatDuration(run.duration_seconds)}</td>
                      <td className="mono">
                        {run.query_id
                          ? <>{run.query_id.slice(0, 12)}…<CopyButton value={run.query_id} /></>
                          : '—'}
                      </td>
                      <td className="task-detail-cell">
                        {run.query_id && (run.state === 'SUCCEEDED' || run.state === 'EXECUTING') && (
                          <StageTooltip queryId={run.query_id} isExecuting={run.state === 'EXECUTING'} />
                        )}
                        {run.state === 'FAILED' && run.error_message
                          ? <span className="task-error-msg" title={run.error_message}>{run.error_message.slice(0, 60)}{run.error_message.length > 60 ? '…' : ''}</span>
                          : (!run.query_id || (run.state !== 'SUCCEEDED' && run.state !== 'EXECUTING')) ? (run.return_value || '—') : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {taskRuns && taskRuns.length > 0 && filteredTasks.length === 0 && (
            <div className="empty-state">No runs match this state filter.</div>
          )}
        </>
      )}

      {tab === 'pipeline' && (
        <>
          {pipelineRuns && pipelineRuns.length > 0 && (
            <div className="run-history-summary">
              <div className="rh-stat">
                <span className="rh-stat-val">{pipelineRuns.length}</span>
                <span className="rh-stat-label">Pipeline completions</span>
              </div>
            </div>
          )}

          <div className="run-history-toolbar">
            <label className="filter-label">
              Status
              <select
                className="status-filter"
                value={pipelineStatusFilter}
                onChange={e => setPipelineStatusFilter(e.target.value)}
              >
                {pipelineStatusOptions.map(s => (
                  <option key={s} value={s}>{s === 'all' ? 'All statuses' : s}</option>
                ))}
              </select>
            </label>
            {filteredPipeline.length > 0 && (
              <span className="rh-table-hint">
                Click a row in the table below to filter the flow chart above to that run.
              </span>
            )}
          </div>

          {pipelineLoading && <div className="loading">Loading pipeline history…</div>}

          {pipelineRuns && pipelineRuns.length === 0 && !pipelineLoading && (
            <div className="empty-state run-history-empty">
              <p><strong>No pipeline runs in this time range.</strong></p>
              <p className="empty-detail">
                Runs appear after pipeline completion events are logged.
                Try expanding the time range.
              </p>
              {onOpenGenerate && (
                <button type="button" className="btn-secondary empty-cta" onClick={onOpenGenerate}>Go to Generate Data</button>
              )}
            </div>
          )}

          {selectedPipelineRun && (
            <div className="rh-sankey-filter-banner">
              <span>
                Flow chart filtered to run{' '}
                <code className="inline-code">
                  {selectedPipelineRun.run_id.length > 24
                    ? `${selectedPipelineRun.run_id.slice(0, 20)}…`
                    : selectedPipelineRun.run_id}
                </code>
                . Click the row again or reset.
              </span>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => {
                  setSelectedPipelineRun(null);
                  setSankeyDrilldown(null);
                }}
              >
                Show all runs in range
              </button>
            </div>
          )}

          {sankeyData && sankeyData.run_count > 0 && (
            <PipelineSankey
              data={sankeyData}
              drilldownType={sankeyDrilldown}
              setDrilldownType={setSankeyDrilldown}
              minutes={minutes}
              selectedRunId={selectedPipelineRun?.run_id ?? null}
              onSelectIdentity={onSelectIdentity}
            />
          )}

          {selectedPipelineRun && sankeyData && sankeyData.run_count === 0 && (
            <div className="empty-state run-history-empty">
              <p><strong>No flow data for this run.</strong></p>
              <p className="empty-detail">
                The Sankey uses pipeline completion event details. If this run ID does not match an event in the log, try another row or reset.
              </p>
            </div>
          )}

          {filteredPipeline.length > 0 && (
            <div className="run-history-table-wrapper rh-pipeline-events-table">
              <table className="run-history-table">
                <thead>
                  <tr>
                    <th>Run ID</th>
                    <th>Status</th>
                    <th>Started</th>
                    <th>Completed</th>
                    <th>Duration</th>
                    <th>Records</th>
                    <th>Clusters (new)</th>
                    <th
                      title="Clusters consolidated in this run when the pipeline linked records that belonged to different existing clusters."
                    >
                      Merges
                    </th>
                    <th
                      title="Existing identity/cluster profiles written or refreshed in this run. Same metric as the “Updated” node in the pipeline flow chart."
                    >
                      Updated
                    </th>
                    <th
                      title="Cluster-to-cluster candidate pair rows generated for the ML scoring layer this run."
                    >
                      ML Pairs
                    </th>
                    <th
                      title="Pairs whose ML confidence score improved this run due to new evidence."
                    >
                      ML Upgraded
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredPipeline.map(run => (
                    <tr
                      key={run.run_id}
                      className={`rh-pipeline-row${selectedPipelineRun?.run_id === run.run_id ? ' rh-pipeline-row-selected' : ''}`}
                      onClick={() => {
                        setSankeyDrilldown(null);
                        setSelectedPipelineRun(prev =>
                          prev?.run_id === run.run_id ? null : run,
                        );
                      }}
                    >
                      <td className="mono">{run.run_id.length > 12 ? `${run.run_id.slice(0, 8)}…` : run.run_id}<CopyButton value={run.run_id} /></td>
                      <td>{statusBadge(run.status)}</td>
                      <td>{formatWhen(run.started_at)}</td>
                      <td>{formatWhen(run.completed_at)}</td>
                      <td>{formatDuration(run.duration_seconds)}</td>
                      <td>{run.records_processed?.toLocaleString() ?? '—'}</td>
                      <td>{run.clusters_created?.toLocaleString() ?? '—'}</td>
                      <td>{run.clusters_merged?.toLocaleString() ?? '—'}</td>
                      <td>{run.clusters_updated?.toLocaleString() ?? '—'}</td>
                      <td>{run.ml_pairs?.toLocaleString() ?? '—'}</td>
                      <td>{run.ml_upgraded?.toLocaleString() ?? '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {pipelineRuns && pipelineRuns.length > 0 && filteredPipeline.length === 0 && (
            <div className="empty-state">No runs match this status filter.</div>
          )}
        </>
      )}
    </div>
  );
}
