import type { ReactNode } from 'react';
import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { LucideIcon } from 'lucide-react';
import { Activity, Database, GitMerge, Info, KeyRound, RefreshCw, ShieldCheck, Sparkles, TrendingUp, UserPlus } from 'lucide-react';
import { fetchDashboard, type MergeSourceShare, type RuleDriver } from '../api';
import { DashStatCard } from '../components/DashStatCard';

const ICON_STROKE = 2;

function DashWidget({
  icon: Icon,
  title,
  subtitle,
  children,
  footer,
}: {
  icon: LucideIcon;
  title: string;
  subtitle: string;
  children: ReactNode;
  footer?: ReactNode;
}) {
  return (
    <div className="dash-widget">
      <div className="dash-widget-head">
        <div className="dash-widget-icon-wrap" aria-hidden>
          <Icon size={20} strokeWidth={ICON_STROKE} />
        </div>
        <div className="dash-widget-titles">
          <h2 className="dash-widget-title">{title}</h2>
          <p className="dash-widget-sub">{subtitle}</p>
        </div>
      </div>
      <div className="dash-widget-body">{children}</div>
      {footer != null && <div className="dash-widget-footer">{footer}</div>}
    </div>
  );
}

function DashInsightPill({ children, variant }: { children: ReactNode; variant: 'purple' | 'green' }) {
  return <div className={`dash-insight-pill dash-insight-pill--${variant}`}>{children}</div>;
}

function mergeContributionInsight(shares: MergeSourceShare[]): string | null {
  if (shares.length === 0) return null;
  const top = [...shares].sort((a, b) => b.pct - a.pct)[0];
  const L = top.label.toLowerCase();
  if (L.includes('pos')) return 'POS transactions drive the majority of in-store identity formation';
  if (L.includes('loyalty')) return 'Loyalty platform records anchor the largest share of known-customer profiles';
  if (L.includes('web')) return 'Web clickstream events stitch anonymous to known sessions at scale';
  if (L.includes('shopify')) return 'Shopify orders provide email-anchored online behavior signal';
  return `${top.label} contributes the largest share of formation signals right now`;
}

function driverInsight(drivers: RuleDriver[]): string | null {
  if (drivers.length === 0) return null;
  const top = drivers[0].rule_label;
  const u = top.toUpperCase();
  if (u.includes('LOYALTY')) return 'Loyalty member number is the strongest current driver of link creation';
  if (u.includes('UID2')) return 'UID2 rules are a major driver of confirmed links';
  if (u.includes('DEVICE')) return 'Device-ID-based rules are driving many confirmed links';
  if (u.includes('EMAIL') || u.includes('HEM')) return 'Email / HEM rules are a key driver of confirmed links';
  if (u.includes('PHONE')) return 'Phone rules are a key driver of confirmed links';
  return `${top} leads by active match volume`;
}

function sourceBreakdownBadge(raw: string): { text: string; variant: 'ssp' | 'tapad' | 'experian' } | null {
  const u = raw.toUpperCase();
  if (u.includes('POS')) return { text: 'POS', variant: 'ssp' };
  if (u.includes('LOYALTY')) return { text: 'Loyalty', variant: 'tapad' };
  if (u.includes('WEB') || u.includes('CLICKSTREAM')) return { text: 'Web', variant: 'experian' };
  if (u.includes('SHOPIFY')) return { text: 'Shopify', variant: 'ssp' };
  return null;
}

function sourceRecordsInsight(entries: [string, number][]): string | null {
  if (entries.length === 0) return null;
  const sorted = [...entries].sort((a, b) => b[1] - a[1]);
  const [k] = sorted[0];
  const u = k.toUpperCase();
  if (u.includes('POS')) return 'POS transactions supply the largest volume of in-store linked records';
  if (u.includes('LOYALTY')) return 'Loyalty platform contributes the most known-customer source records';
  if (u.includes('WEB') || u.includes('CLICKSTREAM')) return 'Web clickstream contributes the most distinct behavioral records';
  if (u.includes('SHOPIFY')) return 'Shopify orders contribute the most online transaction records';
  return `${k} supplies the largest volume of linked records`;
}

function idMixVariant(label: string): 'purple' | 'green' | 'dark' {
  const u = label.toUpperCase();
  if (u.includes('HEM') || u.includes('EMAIL')) return 'green';
  if (u.includes('LOYALTY') || u.includes('PHONE')) return 'purple';
  return 'dark';
}

/** True when ?dashboard_profile=1 (or same in the hash query segment). */
function dashboardProfileFromLocation(): boolean {
  if (typeof window === 'undefined') return false;
  const fromSearch = new URLSearchParams(window.location.search).get('dashboard_profile');
  if (fromSearch === '1') return true;
  const { hash } = window.location;
  const q = hash.indexOf('?');
  if (q >= 0 && new URLSearchParams(hash.slice(q)).get('dashboard_profile') === '1') return true;
  return false;
}

export default function Dashboard({
  onViewIdentities,
}: {
  onViewIdentities: () => void;
}) {
  const dashProfile = dashboardProfileFromLocation();

  const { data: stats, isPending, isError, error, refetch } = useQuery({
    queryKey: ['dashboard', dashProfile],
    queryFn: () => fetchDashboard(undefined, { profile: dashboardProfileFromLocation() }),
  });

  useEffect(() => {
    if (!dashProfile) return;
    if (!stats) return;
    const timings = stats.timings_ms;
    if (!timings || Object.keys(timings).length === 0) {
      console.warn(
        '[dashboard] ?dashboard_profile=1 but response has no timings_ms. ' +
          'Open Network → /api/dashboard and confirm the request URL includes profile=1; ' +
          'restart the API from use-cases/ssp/backend (older servers omit this field).',
      );
      return;
    }
    console.log('[dashboard] dashboard_wall_ms', stats.dashboard_wall_ms);
    console.table(Object.entries(timings).map(([query, ms]) => ({ query, ms })));
  }, [dashProfile, stats]);

  if (isPending) return <div className="loading">Loading dashboard...</div>;

  if (isError) {
    const msg =
      error instanceof Error ? error.message : 'Check that the API is running and the dev proxy points to it.';
    return (
      <div className="dashboard">
        <h1>Dashboard</h1>
        <div className="error-message">
          <p><strong>Could not load dashboard.</strong></p>
          <p className="empty-detail">{msg}</p>
          <button type="button" className="btn-secondary" style={{ marginTop: 12 }} onClick={() => void refetch()}>
            Retry
          </button>
        </div>
      </div>
    );
  }

  if (!stats) return <div className="loading">Loading dashboard...</div>;

  const cb = stats.confidence_buckets;
  const confTotal = Math.max(1, cb.high + cb.medium + cb.low);
  const maxDriver = Math.max(1, ...stats.rule_drivers.map(d => d.count));
  const maxMergePct = Math.max(...stats.merge_source_shares.map(s => s.pct), 1);
  const sourceVals = Object.values(stats.source_breakdown);
  const maxSource = Math.max(1, ...sourceVals);

  const rd = stats.run_delta;
  const nNew = rd.new_clusters ?? 0;
  const nUpd = rd.updated_clusters ?? 0;
  const hasRunDelta = Boolean(rd.run_id);

  const sourceEntries = Object.entries(stats.source_breakdown) as [string, number][];
  const idTypeEntries = Object.entries(stats.id_type_breakdown).sort((a, b) => b[1] - a[1]);
  const mergeFoot = mergeContributionInsight(stats.merge_source_shares);
  const driverFoot = driverInsight(stats.rule_drivers);
  const sbFoot = sourceRecordsInsight(sourceEntries);
  return (
    <div className="dashboard dashboard-operational">
      <header className="dashboard-hero">
        <div className="dashboard-hero-text">
          <h1>Dashboard</h1>
          <p className="dashboard-subtitle">Identity quality, growth, and consolidation across your graph.</p>
        </div>
        <div className="dashboard-hero-actions">
          <button type="button" className="generate-btn dashboard-cta-identities" onClick={onViewIdentities}>
            View Identities
          </button>
        </div>
      </header>

      <div className="stats-grid dashboard-stats-row">
        <DashStatCard
          label="Events ingested"
          value={stats.pipeline_events_ingested.toLocaleString()}
          desc="All time — POS, Loyalty, Web Clickstream, and Shopify (summed from every pipeline completion)"
        />
        <DashStatCard
          label="Clusters created"
          value={stats.pipeline_clusters_created.toLocaleString()}
          desc="All time — new deterministic identities reported across all runs"
        />
        <DashStatCard
          label="Identity Consolidation"
          value={stats.global_dedupe_ratio != null ? `${(stats.global_dedupe_ratio * 100).toFixed(1)}%` : 'N/A'}
          desc="Current graph — all active clusters vs all linked source records (reflects full history to date)"
          info={{
            id: 'dashboard-consolidation-tooltip',
            ariaLabel: 'How identity consolidation is calculated',
            content: (
              <>
                <p><strong>Formula</strong> (shown as a percentage):</p>
                <p className="stat-formula-line"><code className="inline-code">(1 − K ÷ N) × 100</code></p>
                <p>
                  <strong>N</strong> = distinct <code className="inline-code">source_record_id</code> values on{' '}
                  <strong>active</strong> clusters.<br />
                  <strong>K</strong> = number of <strong>active</strong> clusters.
                </p>
                <p>
                  <strong>Higher</strong> when many records share fewer clusters; <strong>lower</strong> when cluster count is close to record count.
                </p>
              </>
            ),
          }}
        />
        <DashStatCard
          label="Avg Confidence"
          value={stats.avg_confidence != null ? `${(stats.avg_confidence * 100).toFixed(1)}%` : 'N/A'}
          desc="All identities — mean confidence over the full identity profile table"
        />
        <DashStatCard
          label="Operational alerts"
          value={stats.alerts.length}
          desc="Full graph signals; ML upgrade counts are all-time"
          info={{
            id: 'dash-op-alerts-tooltip',
            ariaLabel: 'Operational alert details',
            tooltipClassName: 'dash-alerts-tooltip',
            content: stats.alerts.length === 0 ? (
              <p>No active alerts — graph looks steady.</p>
            ) : (
              <>
                {stats.alerts.map(a => (
                  <div key={a.key} className={`dash-alerts-tooltip-block dash-alerts-tooltip-block--${a.severity}`}>
                    <div className="dash-alerts-tooltip-title">{a.title}</div>
                    <p className="dash-alerts-tooltip-detail">{a.detail}</p>
                  </div>
                ))}
              </>
            ),
          }}
        />
      </div>

      <div className="dashboard-mid-grid dashboard-mid-mock">
        <DashWidget icon={Activity} title="What changed this run" subtitle="Latest movement in the identity graph.">
          <div className="run-delta-mock-grid">
            <div className="run-delta-mock-cell">
              <UserPlus className="run-delta-mock-icon" size={18} strokeWidth={1.5} aria-hidden />
              <span className="run-delta-mock-value">{fmtDelta(rd.new_clusters)}</span>
              <span className="run-delta-mock-label">New identities</span>
            </div>
            <div className="run-delta-mock-cell">
              <Sparkles className="run-delta-mock-icon" size={18} strokeWidth={1.5} aria-hidden />
              <span className="run-delta-mock-value">{fmtDelta(rd.candidate_matches)}</span>
              <span className="run-delta-mock-label">Candidate matches</span>
            </div>
            <div className="run-delta-mock-cell">
              <RefreshCw className="run-delta-mock-icon" size={18} strokeWidth={1.5} aria-hidden />
              <span className="run-delta-mock-value">{fmtDelta(rd.updated_clusters)}</span>
              <span className="run-delta-mock-label">Updated identities</span>
            </div>
            <div className="run-delta-mock-cell">
              <GitMerge className="run-delta-mock-icon" size={18} strokeWidth={1.5} aria-hidden />
              <span className="run-delta-mock-value">{fmtDelta(rd.merged_clusters)}</span>
              <span className="run-delta-mock-label">Merged identities</span>
            </div>
          </div>
          {hasRunDelta && (nNew > 0 || nUpd > 0) && (
            <div className="run-delta-pills">
              {(nNew >= nUpd && nNew > 0) && (
                <span className="run-delta-pill run-delta-pill--purple">Most activity is new identity creation</span>
              )}
              {nNew > 0 && nUpd <= Math.max(2, Math.floor(nNew * 0.35)) && (
                <span className="run-delta-pill run-delta-pill--amber">Few existing identities were enriched</span>
              )}
            </div>
          )}
          {!rd.run_id &&
            rd.new_clusters == null &&
            rd.updated_clusters == null &&
            rd.merged_clusters == null && (
              <p className="panel-hint dash-widget-hint">
                Deltas appear when <code className="inline-code">IDR_COMPLETE</code> events exist in the log.
              </p>
            )}
        </DashWidget>

        <DashWidget icon={ShieldCheck} title="Confidence distribution" subtitle="How much of the graph is high confidence.">
          <div className="conf-donut-row">
            <ConfDonut label="High (≥90%)" count={cb.high} total={confTotal} tone="high" />
            <ConfDonut label="Medium (60–90%)" count={cb.medium} total={confTotal} tone="medium" />
            <ConfDonut label="Low (<60%)" count={cb.low} total={confTotal} tone="low" />
          </div>
        </DashWidget>

        <DashWidget icon={Sparkles} title="Identity health" subtitle="Overall graph quality.">
          <div className="health-widget-body">
            <div className="panel-hint-wrap health-info-wrap">
              <button
                type="button"
                className="panel-hint-btn"
                aria-label="How identity health is calculated"
                aria-describedby="dashboard-health-tooltip"
              >
                <Info size={15} strokeWidth={2} aria-hidden />
              </button>
              <div
                id="dashboard-health-tooltip"
                className="panel-hint-tooltip panel-hint-tooltip--below"
                role="tooltip"
              >
                <p>{stats.health.summary}</p>
                {stats.total_identities > 0 && cb.high / stats.total_identities < 0.2 && (
                  <p><strong>Note:</strong> Confidence is concentrated in a small portion of identities.</p>
                )}
              </div>
            </div>
            <HealthGauge score={stats.health.score} />
          </div>
        </DashWidget>
      </div>

      <div className="dashboard-panels dashboard-panels-merge-rules dashboard-panels-dash-widgets">
        <DashWidget
          icon={Database}
          title="Source contribution"
          subtitle="Which inputs contribute most to identity formation"
          footer={mergeFoot ? <DashInsightPill variant="purple">{mergeFoot}</DashInsightPill> : undefined}
        >
          {stats.merge_source_shares.length === 0 ? (
            <p className="panel-hint">No match or ingest breakdown yet.</p>
          ) : (
            <div className="merge-share-list">
              {stats.merge_source_shares.map(s => (
                <div key={s.label} className="merge-share-row">
                  <span className={`merge-share-label merge-share-${mergeClass(s.label)}`}>{s.label}</span>
                  <span className="merge-share-pct">{s.pct}%</span>
                  <div className="breakdown-bar dash-bar-track">
                    <div
                      className={`breakdown-fill merge-fill-${mergeClass(s.label)}`}
                      style={{ width: `${(s.pct / maxMergePct) * 100}%` }}
                    />
                  </div>
                </div>
              ))}
            </div>
          )}
        </DashWidget>

        <DashWidget
          icon={TrendingUp}
          title="Top drivers of identity"
          subtitle="Rules creating the most confirmed links"
          footer={driverFoot ? <DashInsightPill variant="green">{driverFoot}</DashInsightPill> : undefined}
        >
          {stats.rule_drivers.length === 0 ? (
            <p className="panel-hint">No match results yet.</p>
          ) : (
            <div className="driver-list">
              {stats.rule_drivers.map(d => (
                <div key={d.rule_label} className="driver-row">
                  <span className="driver-name" title={d.rule_label}>{d.rule_label}</span>
                  <span className="driver-count">{d.count.toLocaleString()}</span>
                  <div className="breakdown-bar dash-bar-track">
                    <div className="breakdown-fill driver-fill" style={{ width: `${(d.count / maxDriver) * 100}%` }} />
                  </div>
                </div>
              ))}
            </div>
          )}
        </DashWidget>
      </div>

      <div className="dashboard-panels dashboard-panels-technical dashboard-panels-dash-widgets">
        <DashWidget
          icon={Database}
          title="Source breakdown"
          subtitle="Distinct source records currently linked in the graph"
          footer={sbFoot ? <DashInsightPill variant="purple">{sbFoot}</DashInsightPill> : undefined}
        >
          <div className="breakdown-list dash-breakdown-list">
            {sourceEntries.map(([source, count]) => {
              const mini = sourceBreakdownBadge(source);
              return (
                <div key={source} className="breakdown-row dash-breakdown-row">
                  <div className="sb-label-cell">
                    <span className="sb-technical mono" title={source}>
                      {source}
                    </span>
                    {mini && (
                      <span className={`source-mini-badge source-mini-badge--${mini.variant}`}>{mini.text}</span>
                    )}
                  </div>
                  <span className="breakdown-count">{count.toLocaleString()}</span>
                  <div className="breakdown-bar dash-bar-track">
                    <div
                      className={`breakdown-fill sb-fill-${mini?.variant ?? 'other'}`}
                      style={{ width: `${(count / maxSource) * 100}%` }}
                    />
                  </div>
                </div>
              );
            })}
          </div>
        </DashWidget>

        <DashWidget icon={KeyRound} title="Identifier mix" subtitle="Most common identifiers active in the graph">
          {idTypeEntries.length === 0 ? (
            <p className="panel-hint">No identifier types yet.</p>
          ) : (
            <div className="id-mix-grid">
              {idTypeEntries.map(([type, count]) => (
                <div key={type} className={`id-mix-pill id-mix-pill--${idMixVariant(type)}`} title={type}>
                  <span className="id-mix-pill-label">{type}</span>
                  <span className="id-mix-pill-value">{count.toLocaleString()}</span>
                </div>
              ))}
            </div>
          )}
        </DashWidget>
      </div>
    </div>
  );
}

function HealthGauge({ score }: { score: number }) {
  const clamped = Math.min(100, Math.max(0, score));
  // Half-circle geometry: viewBox 200x120, center (100,100), radius 80
  const cx = 100;
  const cy = 100;
  const r = 80;
  const polar = (pct: number) => {
    // 0% -> 180deg (left), 100% -> 0deg (right)
    const angle = Math.PI * (1 - pct / 100);
    return { x: cx + r * Math.cos(angle), y: cy - r * Math.sin(angle) };
  };
  const arcPath = (fromPct: number, toPct: number) => {
    const a = polar(fromPct);
    const b = polar(toPct);
    return `M ${a.x} ${a.y} A ${r} ${r} 0 0 1 ${b.x} ${b.y}`;
  };
  const needle = polar(clamped);
  const tone: 'low' | 'medium' | 'high' =
    clamped < 40 ? 'low' : clamped < 70 ? 'medium' : 'high';
  const label =
    clamped >= 90 ? 'Excellent' :
    clamped >= 70 ? 'Good' :
    clamped >= 40 ? 'Fair' : 'Poor';
  const ticks = [0, 20, 40, 60, 80, 100];
  return (
    <div className="health-gauge" role="img" aria-label={`Identity health score ${clamped} of 100`}>
      <div className="health-gauge-inner">
        <svg className="health-gauge-svg" viewBox="-12 0 224 130" aria-hidden>
          {/* Tracks split into 3 tone segments */}
          <path className="health-gauge-track health-gauge-track-low" d={arcPath(0, 40)} />
          <path className="health-gauge-track health-gauge-track-med" d={arcPath(40, 70)} />
          <path className="health-gauge-track health-gauge-track-high" d={arcPath(70, 100)} />
          {/* Tick labels */}
          {ticks.map(t => {
            const lp = {
              x: cx + (r + 14) * Math.cos(Math.PI * (1 - t / 100)),
              y: cy - (r + 14) * Math.sin(Math.PI * (1 - t / 100)),
            };
            return (
              <text
                key={t}
                className="health-gauge-tick"
                x={lp.x}
                y={lp.y}
                textAnchor="middle"
                dominantBaseline="middle"
              >
                {t}
              </text>
            );
          })}
          {/* Needle */}
          <line
            className="health-gauge-needle"
            x1={cx}
            y1={cy}
            x2={needle.x}
            y2={needle.y}
          />
          <circle className={`health-gauge-knob health-gauge-knob-${tone}`} cx={needle.x} cy={needle.y} r="6" />
        </svg>
        <div className="health-gauge-readout">
          <div className={`health-gauge-value health-gauge-value-${tone}`}>{clamped}<span className="health-gauge-denom">/100</span></div>
          <div className="health-gauge-label">{label}</div>
        </div>
      </div>
    </div>
  );
}

function fmtDelta(n: number | null | undefined): string {
  if (n == null) return '—';
  return n.toLocaleString();
}

function mergeClass(label: string): string {
  if (label.includes('Loyalty')) return 'tapad';
  if (label.includes('Web') || label.includes('Clickstream')) return 'experian';
  if (label.includes('POS') || label.includes('Shopify')) return 'ssp';
  return 'other';
}

function ConfBar({
  label,
  count,
  total,
  tone,
}: {
  label: string;
  count: number;
  total: number;
  tone: 'high' | 'medium' | 'low';
}) {
  const pct = total > 0 ? (count / total) * 100 : 0;
  return (
    <div className="conf-bar-row">
      <span className="conf-bar-label">{label}</span>
      <div className="conf-bar-track-wrap" aria-hidden>
        <div className="conf-bar-track">
          <div className={`conf-bar-fill conf-fill-${tone}`} style={{ width: `${pct}%` }} />
        </div>
      </div>
      <span className="conf-bar-count" title={`${pct.toFixed(1)}% of identities in this bucket`}>
        {count.toLocaleString()}
        <span className="conf-bar-pct"> ({pct < 10 && pct > 0 ? pct.toFixed(1) : Math.round(pct)}%)</span>
      </span>
    </div>
  );
}

function ConfDonut({
  label,
  count,
  total,
  tone,
}: {
  label: string;
  count: number;
  total: number;
  tone: 'high' | 'medium' | 'low';
}) {
  const pct = total > 0 ? (count / total) * 100 : 0;
  const r = 36;
  const c = 2 * Math.PI * r;
  const dash = (Math.min(100, Math.max(0, pct)) / 100) * c;
  const display = pct < 10 && pct > 0 ? pct.toFixed(1) : Math.round(pct).toString();
  return (
    <div className={`conf-donut conf-donut-${tone}`}>
      <div className="conf-donut-label">{label}</div>
      <div className="conf-donut-chart">
        <svg className="conf-donut-svg" viewBox="0 0 96 96" aria-hidden>
          <circle className="conf-donut-track" cx="48" cy="48" r={r} fill="none" strokeWidth="10" />
          <circle
            className={`conf-donut-fill conf-fill-${tone}`}
            cx="48"
            cy="48"
            r={r}
            fill="none"
            strokeWidth="10"
            strokeDasharray={`${dash} ${c - dash}`}
            strokeDashoffset={c / 4}
            strokeLinecap="round"
            transform="rotate(-90 48 48)"
          />
        </svg>
        <div className="conf-donut-center">
          <div className="conf-donut-pct">{display}%</div>
          <div className="conf-donut-count">{count.toLocaleString()}</div>
        </div>
      </div>
    </div>
  );
}
