import { useEffect, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import type { SourceRecordDetail } from '../api';
import { fetchSourceRecordPayload } from '../api';
import CopyButton from './CopyButton';
import {
  formatRawPayload,
  friendlySourceTitle,
  summaryRowsFromIdentifiers,
} from '../utils/sourceRecords';

type DetailTab = 'summary' | 'extracted' | 'raw';

export default function SourceRecordModal({ record, onClose }: {
  record: SourceRecordDetail;
  onClose: () => void;
}) {
  const [tab, setTab] = useState<DetailTab>('summary');

  const { data: payload, isLoading: payloadLoading } = useQuery({
    queryKey: ['source-record-payload', record.record_id, record.source_type],
    queryFn: () => fetchSourceRecordPayload(record.record_id, record.source_type),
  });

  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [onClose]);

  const summaryRows = summaryRowsFromIdentifiers(record.identifiers);
  const rawJson = payload?.raw_payload_json ?? record.raw_payload_json ?? null;
  const rawPretty = formatRawPayload(rawJson);
  const hasRaw = Boolean(rawJson && rawJson.length > 0);
  const attrs = payload?.source_attributes ?? record.source_attributes ?? null;
  const hasAttrs = attrs != null && Object.keys(attrs).length > 0;
  const SKIP_ATTRS = new Set(['SOURCE_FILE', 'STD_BATCH_ID', 'STD_PROCESSED_AT', 'IDR_PROCESSED', 'IDR_PROCESSED_AT', 'METADATA_ACTION', 'METADATA_ISUPDATE', 'RECORD_HASH', 'INGESTED_AT']);
  const rawOrigin = payload?.raw_payload_origin ?? record.raw_payload_origin ?? null;
  const originLabel =
    rawOrigin === 'bronze'
      ? 'Bronze (as ingested)'
      : rawOrigin === 'silver_std'
        ? 'Standardized silver copy'
        : null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal modal-source-record" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <div className="modal-title-block">
            <div className="modal-kicker">{friendlySourceTitle(record.source_type)}</div>
            <div className="modal-title-row">
              <h2 className="mono modal-record-id">{record.record_id}</h2>
              <CopyButton value={record.record_id} />
              <span className={`source-pill ${record.source_type.toLowerCase().replace(/_/g, '-')}`}>{record.source_type}</span>
            </div>
            {record.created_at && (
              <div className="modal-meta-inline">
                <span className="meta-label">Linked</span>
                <span>{new Date(record.created_at).toLocaleString()}</span>
              </div>
            )}
          </div>
          <button type="button" className="modal-close" aria-label="Close" onClick={onClose}>&times;</button>
        </div>

        <div className="modal-tabs">
          <button
            type="button"
            className={`modal-tab ${tab === 'summary' ? 'active' : ''}`}
            onClick={() => setTab('summary')}
          >
            Summary
          </button>
          <button
            type="button"
            className={`modal-tab ${tab === 'extracted' ? 'active' : ''}`}
            onClick={() => setTab('extracted')}
          >
            Extracted IDs
          </button>
          <button
            type="button"
            className={`modal-tab ${tab === 'raw' ? 'active' : ''}`}
            onClick={() => setTab('raw')}
          >
            Original payload
          </button>
        </div>

        <div className="modal-body modal-body-padded">
          {tab === 'summary' && (
            <div className="source-summary-panel">
              <p className="source-help">
                Values below were <strong>extracted for identity resolution</strong>. Open <em>Original payload</em> to see the full vendor record when available.
              </p>
              {summaryRows.length === 0 ? (
                <p className="text-muted">No identifiers linked for this record.</p>
              ) : (
                <ul className="source-summary-list">
                  {summaryRows.map((row) => (
                    <li key={row.type}>
                      <span className="source-summary-label">{row.label}</span>
                      <span className="source-summary-value mono">{row.value}</span>
                      <CopyButton value={row.value} />
                    </li>
                  ))}
                </ul>
              )}
            </div>
          )}

          {tab === 'extracted' && (
            <div className="source-extracted-panel">
              <p className="source-help subtle">
                Technical names as stored after standardization (for analysts).
              </p>
              <table className="identity-table full">
                <thead>
                  <tr>
                    <th>Type</th>
                    <th>Value</th>
                  </tr>
                </thead>
                <tbody>
                  {record.identifiers.map((id, i) => (
                    <tr key={i}>
                      <td><span className="id-type-tag">{id.type}</span></td>
                      <td className="mono">{id.value}<CopyButton value={id.value} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {tab === 'raw' && (
            <div className="source-raw-panel">
              {payloadLoading && (
                <div className="loading">Loading original payload…</div>
              )}
              {!payloadLoading && hasAttrs && (
                <>
                  {originLabel && (
                    <p className="raw-origin-note">
                      <span className="raw-origin-pill">{originLabel}</span>
                      Source record as stored in the data warehouse.
                    </p>
                  )}
                  <table className="identity-table full">
                    <thead>
                      <tr>
                        <th>Column</th>
                        <th>Value</th>
                      </tr>
                    </thead>
                    <tbody>
                      {Object.entries(attrs).filter(([k]) => !SKIP_ATTRS.has(k)).map(([k, v]) => (
                        <tr key={k}>
                          <td><span className="id-type-tag">{k}</span></td>
                          <td className="mono">{v ?? <span className="text-muted">NULL</span>}{v && <CopyButton value={v} />}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )}
              {!payloadLoading && hasRaw && (
                <>
                  {hasAttrs && <h3 className="raw-section-heading">Raw payload (VARIANT)</h3>}
                  {!hasAttrs && originLabel && (
                    <p className="raw-origin-note">
                      <span className="raw-origin-pill">{originLabel}</span>
                      Use this to audit what was received before parsing.
                    </p>
                  )}
                  <div className="raw-toolbar">
                    <span className="raw-copy-wrap">
                      <CopyButton value={rawPretty} />
                      <span className="raw-copy-hint">Copy full JSON</span>
                    </span>
                  </div>
                  <pre className="raw-json-block">{rawPretty}</pre>
                </>
              )}
              {!payloadLoading && !hasAttrs && !hasRaw && (
                <div className="empty-raw">
                  <p>No raw payload is stored for this record in bronze or silver.</p>
                  <p className="text-muted small">
                    Older pipelines or truncated sandboxes may not retain OpenRTB / graph payloads.
                  </p>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
