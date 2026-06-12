import { useState, useRef, useCallback } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: '/api' });

interface GenerateResponse {
  status: string;
  stage?: string;
  message?: string;
  generated?: {
    pos_transactions: number;
    loyalty_members: number;
    web_clickstream: number;
    shopify_orders: number;
    total: number;
  };
  error?: string;
}

const STAGES = [
  { key: 'building_population', label: 'Building population' },
  { key: 'generating_events', label: 'Generating events' },
  { key: 'writing_files', label: 'Writing files' },
  { key: 'loading_to_snowflake', label: 'Loading to Snowflake' },
] as const;

function StageProgress({ currentStage }: { currentStage: string | undefined }) {
  const currentIdx = STAGES.findIndex(s => s.key === currentStage);

  return (
    <div className="generate-stages">
      {STAGES.map((stage, idx) => {
        let state: 'done' | 'active' | 'pending' = 'pending';
        if (idx < currentIdx) state = 'done';
        else if (idx === currentIdx) state = 'active';

        return (
          <div key={stage.key} className={`generate-stage generate-stage--${state}`}>
            <div className="generate-stage-indicator">
              {state === 'done' ? (
                <svg width="14" height="14" viewBox="0 0 14 14" fill="none">
                  <path d="M2.5 7L5.5 10L11.5 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
                </svg>
              ) : state === 'active' ? (
                <div className="generate-stage-spinner" />
              ) : (
                <div className="generate-stage-dot" />
              )}
            </div>
            <span className="generate-stage-label">{stage.label}</span>
          </div>
        );
      })}
    </div>
  );
}

export default function GenerateData({
  onOpenRunHistory,
  onOpenIdentities,
}: {
  onOpenRunHistory?: () => void;
  onOpenIdentities?: () => void;
}) {
  const [generating, setGenerating] = useState(false);
  const [recordScale, setRecordScale] = useState(1);
  const [currentStage, setCurrentStage] = useState<string | undefined>(undefined);
  const [result, setResult] = useState<{ success: boolean; message: string; data?: GenerateResponse } | null>(null);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  const pollStatus = useCallback(() => {
    pollRef.current = setInterval(async () => {
      try {
        const resp = await api.get<GenerateResponse>('/generate/status');
        const d = resp.data;
        if (d.stage) setCurrentStage(d.stage);
        if (d.status === 'complete') {
          stopPolling();
          setGenerating(false);
          setCurrentStage(undefined);
          setResult({ success: true, message: d.message || 'Wave complete', data: d });
        } else if (d.status === 'failed') {
          stopPolling();
          setGenerating(false);
          setCurrentStage(undefined);
          setResult({ success: false, message: d.error || 'Generation failed' });
        }
      } catch {
        stopPolling();
        setGenerating(false);
        setCurrentStage(undefined);
        setResult({ success: false, message: 'Lost connection while polling status' });
      }
    }, 2000);
  }, [stopPolling]);

  const handleGenerate = async () => {
    setGenerating(true);
    setResult(null);
    setCurrentStage(undefined);
    try {
      await api.post('/generate', { sources: [{ source: 'all', count: 1 }], record_scale: recordScale });
      pollStatus();
    } catch (err: unknown) {
      setGenerating(false);
      const msg = axios.isAxiosError(err) && err.response?.data?.detail
        ? err.response.data.detail
        : err instanceof Error ? err.message : 'Generation failed';
      setResult({ success: false, message: msg });
    }
  };

  return (
    <div className="generate-data">
      <h1>Generate Data</h1>
      <div className="page-subtitle generate-data-intro">
        <p>
          Simulate realistic POS, Loyalty, Web Clickstream, and Shopify retail/DTC data
          to see how identities are resolved across devices and households.
        </p>
        <p>
          Each click generates one <strong>wave</strong> — returning persons produce
          new events that enrich existing clusters, while new persons and households
          expand the identity graph.
        </p>
      </div>

      <div className="pipeline-callout pipeline-callout--stacked generate-data-callout">
        <p className="pipeline-callout-p pipeline-callout-section-title">
          <strong>What happens each wave</strong>
        </p>
        <ul className="pipeline-callout-list">
          <li>POS transactions, loyalty signups, web clickstream events, and Shopify orders are generated for active customers</li>
          <li>Returning customers add events to existing identity clusters across channels</li>
          <li>New customers and households introduce fresh identities and shared addresses</li>
          <li>Cross-device, cross-channel, and household connections strengthen over time</li>
        </ul>

        <p className="pipeline-callout-p pipeline-callout-section-title pipeline-callout-section-spaced">
          <strong>What to expect</strong>
        </p>
        <p className="pipeline-callout-p pipeline-callout-subhead">Within about a minute:</p>
        <ul className="pipeline-callout-list">
          <li>New identities appear as the pipeline processes the wave</li>
          <li>Existing identities gain new emails, phones, addresses, and device fingerprints</li>
          <li>Households form when R14 / R15 link clusters that share an address</li>
          <li>ML candidate pairs (R16) gain evidence and score upgrades; borderline pairs route to LLM adjudication (R17)</li>
        </ul>
        <p className="pipeline-callout-p pipeline-callout-subhead">You can then:</p>
        <ul className="pipeline-callout-list pipeline-callout-list--last">
          <li>
            View results in{' '}
            {onOpenIdentities ? (
              <button type="button" className="inline-link" onClick={onOpenIdentities}>Identities</button>
            ) : (
              'Identities'
            )}
          </li>
          <li>
            Track progress in{' '}
            {onOpenRunHistory ? (
              <button type="button" className="inline-link" onClick={onOpenRunHistory}>Run History</button>
            ) : (
              'Run History'
            )}
          </li>
        </ul>
      </div>

      <p className="generate-data-hint">
        Click multiple times to simulate successive waves — each wave strengthens connections and enriches identity profiles.
      </p>

      <div className="generate-scale-control">
        <label className="generate-scale-label">
          <span>Record scale</span>
          <span className="generate-scale-value">{recordScale}x</span>
        </label>
        <input
          type="range"
          min={1}
          max={50}
          value={recordScale}
          onChange={(e) => setRecordScale(Number(e.target.value))}
          disabled={generating}
          className="generate-scale-slider"
        />
        <span className="generate-scale-estimate">~{(40000 * recordScale).toLocaleString()} events</span>
      </div>

      <button
        type="button"
        className="generate-btn"
        onClick={handleGenerate}
        disabled={generating}
      >
        {generating ? 'Generating…' : 'Generate Wave'}
      </button>

      {generating && <StageProgress currentStage={currentStage} />}

      {result && (
        <div className={`generate-result-block ${result.success ? 'success' : 'error'}`}>
          <div className={`generate-result ${result.success ? 'success' : 'error'}`}>
            {result.success ? '✓ ' : '✗ '}{result.message}
          </div>

          {result.success && result.data && (
            <>
              {result.data.generated && (
                <div className="gen-stats-grid">
                  <div className="gen-stat-section">
                    <h4>Generated (this wave)</h4>
                    <div className="gen-stat-rows">
                      <div className="gen-stat-row"><span>POS Transactions</span><span className="mono">{(result.data.generated?.pos_transactions ?? 0).toLocaleString()}</span></div>
                      <div className="gen-stat-row"><span>Loyalty Members</span><span className="mono">{(result.data.generated?.loyalty_members ?? 0).toLocaleString()}</span></div>
                      <div className="gen-stat-row"><span>Web Clickstream</span><span className="mono">{(result.data.generated?.web_clickstream ?? 0).toLocaleString()}</span></div>
                      <div className="gen-stat-row"><span>Shopify Orders</span><span className="mono">{(result.data.generated?.shopify_orders ?? 0).toLocaleString()}</span></div>
                    </div>
                  </div>
                  <div className="gen-stat-section">
                    <h4>Total loaded into Snowflake</h4>
                    <div className="gen-stat-rows">
                      <div className="gen-stat-row"><span>Total records</span><span className="mono">{(result.data.generated?.total ?? 0).toLocaleString()}</span></div>
                    </div>
                  </div>
                </div>
              )}

              <p className="generate-post-success-note">
                Wave data loaded. The pipeline will process it automatically — check Run History or Identities within a minute to see results.
              </p>

              {(onOpenRunHistory || onOpenIdentities) && (
                <div className="generate-next-steps">
                  <span className="next-steps-label">Next steps</span>
                  {onOpenRunHistory && (
                    <button type="button" className="btn-secondary" onClick={onOpenRunHistory}>Open Run History</button>
                  )}
                  {onOpenIdentities && (
                    <button type="button" className="btn-secondary" onClick={onOpenIdentities}>Browse Identities</button>
                  )}
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}
