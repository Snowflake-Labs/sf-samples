import { useState, useRef, useCallback } from 'react';
import { FlaskConical, Play, CheckCircle2, XCircle, AlertCircle, Clock } from 'lucide-react';
import { triggerTests, fetchTestStatus, TestRunResult, TestResult } from '../api';

export default function TestRunner() {
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<TestRunResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [skipLlm, setSkipLlm] = useState(true);
  const [cleanup, setCleanup] = useState(true);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const runTests = useCallback(async () => {
    setRunning(true);
    setResult(null);
    setError(null);
    try {
      const { run_id } = await triggerTests(skipLlm, cleanup);
      pollRef.current = setInterval(async () => {
        try {
          const status = await fetchTestStatus(run_id);
          if (status.status === 'DONE') {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setResult(status.result!);
            setRunning(false);
          } else if (status.status === 'FAILED') {
            clearInterval(pollRef.current!);
            pollRef.current = null;
            setError(status.error || 'Unknown error');
            setRunning(false);
          }
        } catch (e: any) {
          // keep polling
        }
      }, 5000);
    } catch (e: any) {
      setError(e.message || 'Failed to trigger tests');
      setRunning(false);
    }
  }, [skipLlm, cleanup]);

  const statusBadge = (t: TestResult) => {
    switch (t.status) {
      case 'PASS': return <span className="stg-badge stg-badge--success"><CheckCircle2 size={14} /> Pass</span>;
      case 'FAIL': return <span className="stg-badge stg-badge--danger"><XCircle size={14} /> Fail</span>;
      case 'SKIP': return <span className="stg-badge stg-badge--muted"><AlertCircle size={14} /> Skip</span>;
      case 'ERROR': return <span className="stg-badge stg-badge--danger"><XCircle size={14} /> Error</span>;
    }
  };

  return (
    <div className="stg-page">
      <div className="stg-page-header">
        <h1><FlaskConical size={24} /> IDR Test Runner</h1>
        <p className="stg-subtitle">
          Execute SP_RUN_IDR_TESTS — full end-to-end pipeline validation (bronze → cluster)
        </p>
      </div>

      <div className="stg-card" style={{ marginBottom: '1.5rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '2rem', flexWrap: 'wrap' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
            <input type="checkbox" checked={skipLlm} onChange={e => setSkipLlm(e.target.checked)} />
            Skip LLM tests
          </label>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', cursor: 'pointer' }}>
            <input type="checkbox" checked={cleanup} onChange={e => setCleanup(e.target.checked)} />
            Cleanup test data
          </label>
          <button className="stg-btn stg-btn--primary" onClick={runTests} disabled={running}>
            {running ? (
              <><Clock size={16} className="stg-spin" /> Running (~3 min)...</>
            ) : (
              <><Play size={16} /> Run Tests</>
            )}
          </button>
        </div>
      </div>

      {error && (
        <div className="stg-card" style={{ borderLeft: '4px solid var(--color-danger)', marginBottom: '1.5rem' }}>
          <strong>Error:</strong> {error}
        </div>
      )}

      {result && (
        <>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))', gap: '1rem', marginBottom: '1.5rem' }}>
            <div className="stg-card" style={{ textAlign: 'center', borderTop: '3px solid var(--color-success)' }}>
              <div style={{ fontSize: '2rem', fontWeight: 700 }}>{result.passed}</div>
              <div style={{ color: 'var(--color-success)', fontWeight: 500 }}>Passed</div>
            </div>
            <div className="stg-card" style={{ textAlign: 'center', borderTop: '3px solid var(--color-danger)' }}>
              <div style={{ fontSize: '2rem', fontWeight: 700 }}>{result.failed}</div>
              <div style={{ color: 'var(--color-danger)', fontWeight: 500 }}>Failed</div>
            </div>
            <div className="stg-card" style={{ textAlign: 'center', borderTop: '3px solid var(--color-muted)' }}>
              <div style={{ fontSize: '2rem', fontWeight: 700 }}>{result.skipped}</div>
              <div style={{ color: 'var(--color-muted)', fontWeight: 500 }}>Skipped</div>
            </div>
            <div className="stg-card" style={{ textAlign: 'center', borderTop: '3px solid var(--color-info)' }}>
              <div style={{ fontSize: '2rem', fontWeight: 700 }}>{(result.duration_ms / 1000).toFixed(0)}s</div>
              <div style={{ color: 'var(--color-info)', fontWeight: 500 }}>Duration</div>
            </div>
          </div>

          <div className="stg-card">
            <h3 style={{ marginBottom: '1rem' }}>Test Results</h3>
            <table className="stg-table" style={{ width: '100%' }}>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Description</th>
                  <th>Status</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {result.tests.map(t => (
                  <tr key={t.id} style={{ opacity: t.status === 'SKIP' ? 0.6 : 1 }}>
                    <td><code>{t.id}</code></td>
                    <td>{t.desc}</td>
                    <td>{statusBadge(t)}</td>
                    <td style={{ fontSize: '0.85em', color: 'var(--color-muted)' }}>
                      {t.status === 'FAIL' && `actual=${t.actual}, expected=${t.expected}`}
                      {t.status === 'SKIP' && (t.reason || '')}
                      {t.status === 'ERROR' && (t.error || '')}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </div>
  );
}
