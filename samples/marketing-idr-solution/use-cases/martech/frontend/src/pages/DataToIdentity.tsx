import { useMemo, useState, type ReactNode } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  BrainCircuit,
  CalendarRange,
  ChevronRight,
  Database,
  Fingerprint,
  GitMerge,
  Info,
  Shield,
} from 'lucide-react';
import { fetchPipelineSankey } from '../api';
import { DashStatCard } from '../components/DashStatCard';

const TIME_OPTIONS = [
  { label: 'Last 5 min', value: 5 },
  { label: 'Last 15 min', value: 15 },
  { label: 'Last 1 hour', value: 60 },
  { label: 'Last 1 day', value: 1440 },
  { label: 'Last 7 days', value: 10080 },
  { label: 'Last 30 days', value: 43200 },
] as const;

const ICON_STROKE = 2;

function partitionSources(bySource: Record<string, number>) {
  let ssp = 0;
  let tapad = 0;
  let experian = 0;
  for (const [rawKey, v] of Object.entries(bySource)) {
    const k = rawKey.toUpperCase();
    if (k.includes('TAPAD')) tapad += v;
    else if (k.includes('EXPERIAN')) experian += v;
    else if (k.includes('BID') || k.includes('SSP')) ssp += v;
    else ssp += v;
  }
  const summed = ssp + tapad + experian;
  return { ssp, tapad, experian, summed };
}

function sumWeakSignalMatches(byRule: Record<string, number>): number {
  let n = 0;
  for (const [label, c] of Object.entries(byRule)) {
    const l = label.toLowerCase();
    if (
      l.includes('cookie') ||
      l.includes('fingerprint') ||
      l.includes('vector') ||
      l.includes('ip') && l.includes('user agent')
    ) {
      n += c;
    }
  }
  return n;
}

type MetricTone = 'default' | 'success' | 'warning' | 'accent' | 'muted';

function FlowMetric({
  label,
  value,
  tone = 'default',
}: {
  label: string;
  value: string | number;
  tone?: MetricTone;
}) {
  return (
    <div className={`pfi-metric-row${tone !== 'default' ? ` pfi-metric-row--${tone}` : ''}`}>
      <span className="pfi-metric-label">{label}</span>
      <span className="pfi-metric-value">{value}</span>
    </div>
  );
}

function FlowStepCard({
  step,
  badge,
  badgeVariant,
  Icon,
  title,
  description,
  children,
}: {
  step: number;
  badge: string;
  badgeVariant: 'bronze' | 'silver' | 'post' | 'gold';
  Icon: typeof Database;
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <div className="pfi-step-card">
      <div className="pfi-step-card-head">
        <span className="pfi-step-icon" aria-hidden>
          <Icon size={22} strokeWidth={ICON_STROKE} />
        </span>
        <span className={`pfi-step-badge pfi-step-badge--${badgeVariant}`}>{badge}</span>
      </div>
      <p className="pfi-step-kicker">Step {step}</p>
      <h3 className="pfi-step-title">{title}</h3>
      <p className="pfi-step-desc">{description}</p>
      <div className="pfi-step-metrics">{children}</div>
    </div>
  );
}

export default function DataToIdentity({ onViewDetails }: { onViewDetails?: () => void }) {
  const [minutes, setMinutes] = useState<number>(43200);

  const { data, isPending, isError, error, refetch } = useQuery({
    queryKey: ['pipeline-sankey', minutes],
    queryFn: () => fetchPipelineSankey(minutes),
    refetchInterval: 30_000,
  });

  const derived = useMemo(() => {
    if (!data) return null;
    const t = data.totals;
    const events = Math.max(0, t.inserts + t.deletes);
    const bySrc = partitionSources(data.by_source);
    const totalEvents = bySrc.summed > 0 ? bySrc.summed : events;
    const created = t.created;
    const updated = t.updated;
    const merged = t.merged;
    const matches = t.matches;
    const mlPairs = t.ml_pairs;
    const dedupX =
      created > 0 ? (totalEvents / created).toFixed(2) : totalEvents > 0 ? '—' : '—';
    const weakMatches = sumWeakSignalMatches(data.by_match_rule);

    return {
      totalEvents,
      bySrc,
      created,
      updated,
      merged,
      matches,
      mlPairs,
      dedupX,
      weakMatches,
      runCount: data.run_count,
    };
  }, [data]);

  return (
    <div className="data-to-identity">
      <header className="pfi-header">
        <div className="pfi-header-text">
          <div className="pfi-title-row">
            <h1>Data to Identity</h1>
            {derived && (
              <span className="pfi-run-badge">
                {derived.runCount} run{derived.runCount !== 1 ? 's' : ''}
              </span>
            )}
          </div>
          <p className="pfi-subtitle">
            Simple view of how incoming data turns into people/households and where additional matches are explored.
          </p>
        </div>
        <div className="pfi-header-actions">
          <div className="pfi-time-wrap">
            <CalendarRange size={16} strokeWidth={ICON_STROKE} aria-hidden className="pfi-time-icon" />
            <select
              className="pfi-time-select"
              value={minutes}
              onChange={e => setMinutes(Number(e.target.value))}
              aria-label="Time range"
            >
              {TIME_OPTIONS.map(o => (
                <option key={o.value} value={o.value}>
                  {o.label}
                </option>
              ))}
            </select>
          </div>
          {onViewDetails && (
            <button type="button" className="generate-btn pfi-details-btn" onClick={onViewDetails}>
              View details
            </button>
          )}
        </div>
      </header>

      {isError && (
        <div className="error-message pfi-error">
          <p>
            <strong>Could not load pipeline summary.</strong>{' '}
            {error instanceof Error ? error.message : 'Check the API and Snowflake connection.'}
          </p>
          <button type="button" className="btn-secondary" onClick={() => void refetch()}>
            Retry
          </button>
        </div>
      )}

      {isPending && <div className="loading">Loading pipeline flow…</div>}

      {derived && !isPending && (
        <>
          <section className="pfi-summary-grid" aria-label="Pipeline summary">
            <DashStatCard
              label="Events ingested"
              value={derived.totalEvents.toLocaleString()}
              desc="Across bid request, Tapad, and Experian"
              info={{
                id: 'd2i-events-ingested',
                ariaLabel: 'About events ingested',
                content: (
                  <>
                    <p>Total source records loaded into Bronze tables during the selected time window — SSP bid requests, Tapad identity graph links, and Experian audience graph records.</p>
                    <p>Each event represents one raw data point before any identity resolution is applied.</p>
                  </>
                ),
              }}
            />
            <DashStatCard
              label="Clusters created"
              value={derived.created.toLocaleString()}
              desc="New deterministic identities created"
              info={{
                id: 'd2i-clusters-created',
                ariaLabel: 'About clusters created',
                content: (
                  <>
                    <p>Number of <strong>new</strong> identity clusters formed when records couldn't be matched to any existing cluster.</p>
                    <p>Each cluster represents a distinct person or household. A high number means many genuinely new identities are appearing.</p>
                  </>
                ),
              }}
            />
            <DashStatCard
              label="Clusters expanded"
              value={derived.updated.toLocaleString()}
              desc="Existing clusters updated with new links"
              info={{
                id: 'd2i-clusters-expanded',
                ariaLabel: 'About clusters expanded',
                content: (
                  <>
                    <p>Number of times existing clusters had new records added via deterministic matching (shared HEM, UID2, PPID, or device ID).</p>
                    <p>This can exceed "Clusters created" because a single cluster can be expanded multiple times across pipeline runs as returning users generate new events.</p>
                  </>
                ),
              }}
            />
            <DashStatCard
              label="Dedup ratio"
              value={typeof derived.dedupX === 'string' && derived.dedupX === '—' ? '—' : `${derived.dedupX}x`}
              desc="Events per new cluster (window aggregate)"
              info={{
                id: 'd2i-dedup-ratio',
                ariaLabel: 'About dedup ratio',
                content: (
                  <>
                    <p><strong>Formula:</strong> <code className="inline-code">Events ingested ÷ Clusters created</code></p>
                    <p>Shows how many raw events are resolved into each new identity. A higher ratio means more data is being consolidated — e.g., 8.84x means ~9 events per new identity on average.</p>
                  </>
                ),
              }}
            />
            <DashStatCard
              label="ML candidate pairs"
              value={derived.mlPairs.toLocaleString()}
              desc="Cross-cluster pairs generated after clustering"
              info={{
                id: 'd2i-ml-candidate-pairs',
                ariaLabel: 'About ML candidate pairs',
                content: (
                  <>
                    <p>Cluster-to-cluster pairs identified by the fingerprint analysis layer <em>after</em> deterministic clustering.</p>
                    <p>These are probabilistic links — clusters that share device fingerprints or behavioral patterns but lack a confirmed identifier match. Each pair is scored by the ML model and categorized as Merge, Household Link, or Candidate Only.</p>
                  </>
                ),
              }}
            />
          </section>

          <div className="pfi-notice" role="note">
            <Shield size={18} strokeWidth={ICON_STROKE} className="pfi-notice-icon" aria-hidden />
            <p>
              This is not a funnel. Many events can belong to the same person or household. This view shows how data is
              combined, improved, and where additional matches are reviewed.
            </p>
          </div>

          <section className="pfi-flow" aria-label="Pipeline stages">
            <div className="pfi-flow-track">
              <FlowStepCard
                step={1}
                badge="Bronze"
                badgeVariant="bronze"
                Icon={Database}
                title="Ingestion"
                description="Incoming data from different sources"
              >
                <FlowMetric label="SSP bid request" value={derived.bySrc.ssp.toLocaleString()} />
                <FlowMetric label="Tapad graph" value={derived.bySrc.tapad.toLocaleString()} />
                <FlowMetric label="Experian graph" value={derived.bySrc.experian.toLocaleString()} />
                <FlowMetric label="Total events" value={derived.totalEvents.toLocaleString()} tone="accent" />
              </FlowStepCard>

              <ChevronRight className="pfi-flow-chevron" aria-hidden />

              <FlowStepCard
                step={2}
                badge="Silver"
                badgeVariant="silver"
                Icon={GitMerge}
                title="Deterministic resolution"
                description="Data is cleaned and matched using confirmed rules to build people/households"
              >
                <FlowMetric label="Data processed" value={derived.totalEvents.toLocaleString()} />
                <FlowMetric label="Confirmed matches found" value={derived.matches.toLocaleString()} />
                <FlowMetric label="New people/households created" value={derived.created.toLocaleString()} tone="success" />
                <FlowMetric
                  label="Existing people/households updated"
                  value={derived.updated.toLocaleString()}
                  tone="warning"
                />
              </FlowStepCard>

              <ChevronRight className="pfi-flow-chevron" aria-hidden />

              <FlowStepCard
                step={3}
                badge="Post-cluster"
                badgeVariant="post"
                Icon={Fingerprint}
                title="Fingerprint analysis"
                description="Look for possible additional matches using weaker signals (like device patterns)"
              >
                <FlowMetric
                  label="Events with device patterns"
                  value={derived.weakMatches > 0 ? derived.weakMatches.toLocaleString() : '—'}
                />
                <FlowMetric
                  label="Possible matches across different groups"
                  value={derived.weakMatches > 0 ? derived.weakMatches.toLocaleString() : '—'}
                />
                <FlowMetric
                  label="Possible group-to-group matches"
                  value={derived.mlPairs.toLocaleString()}
                  tone="accent"
                />
                <FlowMetric label="Used for direct merging" value="Review-driven" tone="warning" />
              </FlowStepCard>

              <ChevronRight className="pfi-flow-chevron" aria-hidden />

              <FlowStepCard
                step={4}
                badge="Gold+"
                badgeVariant="gold"
                Icon={BrainCircuit}
                title="ML review / promotion"
                description="Evaluate possible matches and decide if they should be linked"
              >
                <FlowMetric label="Strong suggested matches" value="—" />
                <FlowMetric label="Possible matches (needs review)" value={derived.mlPairs.toLocaleString()} tone="warning" />
                <FlowMetric label="Rejected matches" value="—" />
                <FlowMetric label="Final merges after review" value={derived.merged.toLocaleString()} tone="accent" />
              </FlowStepCard>
            </div>
            <p className="pfi-flow-footnote">
              Totals sum <code className="inline-code">IDR_COMPLETE</code> events in the selected window (up to 200 runs).
              ML score bands are not aggregated here yet—use Run History for per-run detail.
            </p>
          </section>
        </>
      )}

      {!derived && !isPending && !isError && <div className="empty-state">No pipeline data in this range.</div>}
    </div>
  );
}
