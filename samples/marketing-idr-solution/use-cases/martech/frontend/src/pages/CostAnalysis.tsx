import { useState, useEffect } from 'react';
import { Brain, BarChart3, Clock, Zap, Lightbulb, Info } from 'lucide-react';

interface AiCostDay {
  day: string;
  calls: number;
  invocations: number;
  avg_pairs_per_invocation: number;
  credits: number;
  est_usd: number;
  avg_input_tokens: number;
  avg_output_tokens: number;
}

const NAV_ITEMS = [
  { id: 'ai-cost', label: 'AI Cost', icon: Brain },
  { id: 'benchmark', label: 'Benchmark', icon: BarChart3 },
  { id: 'stages', label: 'Stage Breakdown', icon: Clock },
  { id: 'ml-output', label: 'ML Output', icon: Zap },
  { id: 'insights', label: 'Insights', icon: Lightbulb },
];

/* ─── Section: AI Cost ─── */
function AICostSection({ aiCost, loading }: { aiCost: AiCostDay[]; loading: boolean }) {
  const totalCredits = aiCost.reduce((s, d) => s + d.credits, 0);
  const totalUsd = aiCost.reduce((s, d) => s + d.est_usd, 0);
  const totalCalls = aiCost.reduce((s, d) => s + d.calls, 0);

  return (
    <>
      <h2 className="stg-section-title"><Brain size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Cortex AI Cost (LLM Adjudication)</h2>
      <p style={{ fontSize: 13, color: 'var(--text-muted)', marginBottom: 16 }}>
        Live usage from <code>SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY</code> — the LLM adjudication task
        calls <code>claude-4-sonnet</code> every 5 minutes to review pairs in the ML score range 0.55–0.85.
        Each invocation processes up to 50 pairs (configurable batch size).
      </p>
      {loading ? (
        <p style={{ fontSize: 13 }}>Loading AI cost data…</p>
      ) : aiCost.length === 0 ? (
        <p style={{ fontSize: 13, color: 'var(--text-muted)' }}>No AI usage data available (view may have latency).</p>
      ) : (
        <>
          <div className="cost-kpi-row">
            <div className="cost-kpi">
              <div className="cost-kpi-value">{totalCalls.toLocaleString()}</div>
              <div className="cost-kpi-label">LLM Calls (7d)</div>
            </div>
            <div className="cost-kpi">
              <div className="cost-kpi-value">{totalCredits.toFixed(2)}</div>
              <div className="cost-kpi-label">Credits (7d)</div>
            </div>
            <div className="cost-kpi">
              <div className="cost-kpi-value">${totalUsd.toFixed(2)}</div>
              <div className="cost-kpi-label">Est. Cost (7d)</div>
            </div>
            <div className="cost-kpi">
              <div className="cost-kpi-value">~{aiCost.length > 0 ? aiCost[0].avg_pairs_per_invocation : 0}</div>
              <div className="cost-kpi-label">Pairs / Invocation</div>
            </div>
          </div>
          <table className="cost-table">
            <thead>
              <tr>
                <th>Day</th>
                <th>LLM Calls</th>
                <th>Invocations</th>
                <th>Pairs / Invocation</th>
                <th>Avg Tokens</th>
                <th>Credits</th>
                <th>Est. USD</th>
              </tr>
            </thead>
            <tbody>
              {aiCost.map((d) => (
                <tr key={d.day}>
                  <td>{d.day}</td>
                  <td>{d.calls.toLocaleString()}</td>
                  <td>{d.invocations}</td>
                  <td>{d.avg_pairs_per_invocation}</td>
                  <td>{d.avg_input_tokens + d.avg_output_tokens}</td>
                  <td>{d.credits.toFixed(4)}</td>
                  <td>${d.est_usd.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </>
  );
}

/* ─── Section: Benchmark ─── */
function BenchmarkSection() {
  return (
    <>
      <h2 className="stg-section-title"><BarChart3 size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Benchmark Results (~830K IDR Records per Wave)</h2>
      <table className="cost-table">
        <thead>
          <tr>
            <th></th>
            <th>X-Small (1 cr/hr)</th>
            <th>Small (2 cr/hr)</th>
            <th>Medium (4 cr/hr)</th>
            <th>Large (8 cr/hr)</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>IDR Records</td>
            <td>841,396</td>
            <td>831,184</td>
            <td>831,844</td>
            <td>823,375</td>
          </tr>
          <tr>
            <td>Duration</td>
            <td>20m 13s</td>
            <td>15m 11s</td>
            <td>8m 17s</td>
            <td>6m 15s</td>
          </tr>
          <tr className="cost-row-highlight">
            <td>Credits consumed</td>
            <td><strong>0.337</strong></td>
            <td><strong>0.506</strong></td>
            <td><strong>0.552</strong></td>
            <td><strong>0.833</strong></td>
          </tr>
          <tr className="cost-row-highlight">
            <td>Cost @ $3/credit</td>
            <td><strong>$1.01</strong></td>
            <td><strong>$1.52</strong></td>
            <td><strong>$1.66</strong></td>
            <td><strong>$2.50</strong></td>
          </tr>
          <tr className="cost-row-highlight">
            <td>Cost per 1M IDR records</td>
            <td><strong>$1.22</strong></td>
            <td><strong>$1.83</strong></td>
            <td><strong>$2.00</strong></td>
            <td><strong>$3.01</strong></td>
          </tr>
        </tbody>
      </table>
    </>
  );
}

/* ─── Section: Stage Breakdown ─── */
function StageBreakdownSection() {
  return (
    <>
      <h2 className="stg-section-title"><Clock size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Stage Breakdown (seconds)</h2>
      <table className="cost-table">
        <thead>
          <tr>
            <th>Stage</th>
            <th>X-Small</th>
            <th>Small</th>
            <th>Medium</th>
            <th>Large</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Cluster</td>
            <td>756s</td>
            <td>488s</td>
            <td>272s</td>
            <td>179s</td>
          </tr>
          <tr>
            <td>Match</td>
            <td>144s</td>
            <td>163s</td>
            <td>40s</td>
            <td>26s</td>
          </tr>
          <tr>
            <td>ML Scoring</td>
            <td>197s</td>
            <td>148s</td>
            <td>102s</td>
            <td>84s</td>
          </tr>
          <tr>
            <td>Extract</td>
            <td>45s</td>
            <td>44s</td>
            <td>33s</td>
            <td>30s</td>
          </tr>
          <tr>
            <td>Standardize + Other</td>
            <td>71s</td>
            <td>68s</td>
            <td>50s</td>
            <td>56s</td>
          </tr>
          <tr className="cost-row-highlight">
            <td><strong>Total</strong></td>
            <td><strong>1,213s</strong></td>
            <td><strong>911s</strong></td>
            <td><strong>497s</strong></td>
            <td><strong>375s</strong></td>
          </tr>
        </tbody>
      </table>
    </>
  );
}

/* ─── Section: ML Output ─── */
function MLOutputSection() {
  return (
    <>
      <h2 className="stg-section-title"><Zap size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> ML Scoring Output</h2>
      <table className="cost-table">
        <thead>
          <tr>
            <th></th>
            <th>X-Small</th>
            <th>Small</th>
            <th>Medium</th>
            <th>Large</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Clusters (new)</td>
            <td>6,206</td>
            <td>6,224</td>
            <td>6,230</td>
            <td>6,310</td>
          </tr>
          <tr>
            <td>Clusters updated</td>
            <td>3,347</td>
            <td>3,220</td>
            <td>3,243</td>
            <td>3,236</td>
          </tr>
          <tr>
            <td>Merges</td>
            <td>705</td>
            <td>762</td>
            <td>769</td>
            <td>741</td>
          </tr>
          <tr>
            <td>ML Pairs identified</td>
            <td>8,758</td>
            <td>8,834</td>
            <td>9,153</td>
            <td>9,224</td>
          </tr>
          <tr>
            <td>ML Upgraded</td>
            <td>2,356</td>
            <td>1,677</td>
            <td>1,332</td>
            <td>1,318</td>
          </tr>
        </tbody>
      </table>
    </>
  );
}

/* ─── Section: Insights ─── */
function InsightsSection() {
  return (
    <>
      <h2 className="stg-section-title"><Lightbulb size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Insights</h2>

      <h3 style={{ fontSize: 14, fontWeight: 500, marginBottom: 12 }}>Observations</h3>
      <ul className="cost-observations">
        <li>
          <strong>Cluster is the dominant bottleneck</strong> at every WH tier (62-48% of total time).
          Iterative label propagation is inherently sequential, limiting parallelism gains.
        </li>
        <li>
          <strong>Match benefits most from scale-up</strong> — 144s on X-Small vs 26s on Large (5.5x speedup).
          The join-heavy workload parallelizes well with more compute nodes.
        </li>
        <li>
          <strong>ML scoring shows steady improvement</strong> — 197s down to 84s (2.3x) from X-Small to Large,
          thanks to parallel feature materialization and UDF execution.
        </li>
        <li>
          <strong>Diminishing cost-efficiency</strong> — each doubling of compute yields less than 2x speedup,
          so cost per record increases at larger sizes.
        </li>
      </ul>

      <div className="stg-info-callout" style={{ marginTop: 20 }}>
        <span className="stg-info-callout-icon"><Info size={14} /></span>
        <div>
          <strong>Recommendation:</strong> Use X-Small for batch/scheduled runs. At $1.22 per 1M IDR records, it is the most
          cost-efficient option. Reserve Medium for near-real-time scenarios requiring sub-10 minute SLAs
          ($2.00/1M). Large ($3.01/1M) is only justified for low-latency demos or time-critical activations
          where 6-minute turnaround is essential.
        </div>
      </div>

      <h3 style={{ fontSize: 14, fontWeight: 500, marginTop: 24, marginBottom: 8 }}>Methodology</h3>
      <p style={{ fontSize: 13, color: 'var(--text-muted)', lineHeight: 1.6 }}>
        Each benchmark loaded ~830K synthetic IDR source records (bid requests + TapAd graph + Experian graph)
        with the pipeline task suspended, then resumed on the target warehouse size.
        Impression logs (~500K/wave) are excluded from IDR record counts as they do not flow through identity resolution.
        Credits calculated as: <code>(duration_seconds / 3600) × credits_per_hour</code>.
        All runs on Snowflake Gen2 Standard warehouses. Optimizations applied: incremental clustering,
        scoped matching, ML stage parallelism, embedding caps. Benchmark date: May 13, 2026.
      </p>
    </>
  );
}

/* ─── Main Component ─── */
export default function CostAnalysis() {
  const [aiCost, setAiCost] = useState<AiCostDay[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState('ai-cost');

  useEffect(() => {
    fetch('/api/ai-cost')
      .then((r) => r.json())
      .then((d) => setAiCost(d.days ?? []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const totalUsd = aiCost.reduce((s, d) => s + d.est_usd, 0);

  function getSummary(id: string): string {
    switch (id) {
      case 'ai-cost': return loading ? 'Loading…' : `$${totalUsd.toFixed(0)} · 7d`;
      case 'benchmark': return '$1.22/1M best';
      case 'stages': return '6 stages';
      case 'ml-output': return '~9K pairs';
      case 'insights': return '4 observations';
      default: return '';
    }
  }

  function renderSection() {
    switch (activeTab) {
      case 'ai-cost': return <AICostSection aiCost={aiCost} loading={loading} />;
      case 'benchmark': return <BenchmarkSection />;
      case 'stages': return <StageBreakdownSection />;
      case 'ml-output': return <MLOutputSection />;
      case 'insights': return <InsightsSection />;
      default: return null;
    }
  }

  return (
    <div className="cost-analysis">
      <h1>Cost Analysis</h1>
      <p className="page-subtitle">
        IDR pipeline cost benchmark processing ~830K IDR source records (SSP bid requests + TapAd + Experian)
        per wave across different warehouse sizes. Full pipeline: standardize, extract, match, cluster, and ML scoring.
      </p>

      <div className="stg-layout">
        <nav className="stg-nav">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.id}
              className={`stg-nav-item${activeTab === item.id ? ' stg-nav-item--active' : ''}`}
              onClick={() => setActiveTab(item.id)}
            >
              <item.icon size={16} className="stg-nav-icon" />
              <span className="stg-nav-text">
                <span className="stg-nav-label">{item.label}</span>
                <span className="stg-nav-summary">{getSummary(item.id)}</span>
              </span>
            </button>
          ))}
        </nav>

        <div className="stg-content">
          {renderSection()}
        </div>
      </div>
    </div>
  );
}
