import { useMemo, useState } from 'react';
import type { SourceRecordDetail } from '../api';
import CopyButton from './CopyButton';
import {
  compareSummary,
  fieldCompareState,
  friendlySourceTitle,
  getIdentifierValues,
  humanizeIdentifierType,
  sortIdentifierTypes,
  type FieldCompareState,
} from '../utils/sourceRecords';

export default function CompareView({ records, onBack }: {
  records: SourceRecordDetail[];
  onBack: () => void;
}) {
  const [showAdvanced, setShowAdvanced] = useState(false);

  const allTypes = useMemo(
    () => sortIdentifierTypes(Array.from(new Set(records.flatMap((r) => r.identifiers.map((id) => id.type))))),
    [records],
  );

  const primaryTypes = useMemo(() => {
    const important = new Set([
      'HEM', 'UID2', 'RAMPID', 'PPID', 'DEVICE_ID', 'COOKIE',
      'GEO_COUNTRY', 'GEO_METRO', 'GEO_ZIP', 'TAPAD_PERSON', 'EXPERIAN_PERSON', 'ID_VALUE',
    ]);
    return allTypes.filter((t) => important.has(t));
  }, [allTypes]);

  const advancedTypes = useMemo(
    () => allTypes.filter((t) => !primaryTypes.includes(t)),
    [allTypes, primaryTypes],
  );

  const visibleTypes = showAdvanced ? allTypes : (primaryTypes.length ? primaryTypes : allTypes);

  const summary = useMemo(() => compareSummary(records), [records]);

  return (
    <div className="compare-view compare-view-v2">
      <div className="compare-header-v2">
        <button type="button" className="back-btn" onClick={onBack}>&larr; Back to source list</button>
        <div className="compare-heading">
          <h2>Compare sources</h2>
          <p className="compare-sub">
            Side-by-side view of identifiers extracted from each vendor record. Green = same values; amber = some overlap; red = no agreement.
          </p>
        </div>
      </div>

      <div className="compare-summary-strip">
        <div className="compare-stat">
          <span className="compare-stat-val">{records.length}</span>
          <span className="compare-stat-label">Sources</span>
        </div>
        <div className="compare-stat stat-aligned">
          <span className="compare-stat-val">{summary.aligned}</span>
          <span className="compare-stat-label">Fully aligned</span>
        </div>
        <div className="compare-stat stat-overlap">
          <span className="compare-stat-val">{summary.overlap}</span>
          <span className="compare-stat-label">Partial overlap</span>
        </div>
        <div className="compare-stat stat-split">
          <span className="compare-stat-val">{summary.split}</span>
          <span className="compare-stat-label">No match</span>
        </div>
        <div className="compare-stat">
          <span className="compare-stat-val">{summary.onlyOne + summary.sparse}</span>
          <span className="compare-stat-label">Sparse / one source</span>
        </div>
      </div>

      <div className="compare-cards-grid">
        {records.map((r, idx) => (
          <article key={r.record_id} className="compare-card">
            <header className="compare-card-head">
              <span className="compare-card-badge">Source {idx + 1}</span>
              <span className={`source-pill compare-card-pill ${r.source_type.toLowerCase().replace(/_/g, '-')}`}>
                {friendlySourceTitle(r.source_type)}
              </span>
            </header>
            <div className="compare-card-id mono" title={r.record_id}>
              {r.record_id.length > 28 ? `${r.record_id.slice(0, 14)}…${r.record_id.slice(-10)}` : r.record_id}
              <CopyButton value={r.record_id} />
            </div>
            {r.created_at && (
              <div className="compare-card-meta">{new Date(r.created_at).toLocaleString()}</div>
            )}
          </article>
        ))}
      </div>

      {advancedTypes.length > 0 && primaryTypes.length > 0 && (
        <div className="compare-advanced-toggle">
          <button
            type="button"
            className="linkish-btn"
            onClick={() => setShowAdvanced(!showAdvanced)}
          >
            {showAdvanced ? 'Hide technical fields' : `Show all ${allTypes.length} identifier types (${advancedTypes.length} more)`}
          </button>
        </div>
      )}

      <div className="compare-fields">
        {visibleTypes.map((type) => (
          <CompareFieldRow key={type} type={type} records={records} />
        ))}
      </div>
    </div>
  );
}

function stateLabel(state: FieldCompareState): string {
  switch (state) {
    case 'aligned':
      return 'Match';
    case 'overlap':
      return 'Partial';
    case 'split':
      return 'Different';
    case 'sparse':
      return 'Empty';
    case 'only_one':
      return 'One source';
    default:
      return '';
  }
}

function CompareFieldRow({ type, records }: { type: string; records: SourceRecordDetail[] }) {
  const state = fieldCompareState(type, records);
  const rowClass = `compare-field-row state-${state}`;

  return (
    <div className={rowClass}>
      <div className="compare-field-label">
        <span className="compare-field-human">{humanizeIdentifierType(type)}</span>
        <span className="id-type-tag compare-field-type">{type}</span>
        <span className={`compare-field-badge badge-${state}`}>{stateLabel(state)}</span>
      </div>
      <div className="compare-field-cells">
        {records.map((r) => {
          const vals = getIdentifierValues(r, type);
          let cellVariant: 'empty' | 'ok' | 'warn' | 'bad' | 'neutral' = 'neutral';
          if (vals.length === 0) cellVariant = 'empty';
          else if (state === 'aligned') cellVariant = 'ok';
          else if (state === 'overlap') cellVariant = 'warn';
          else if (state === 'split') cellVariant = 'bad';
          return (
            <div key={r.record_id} className={`compare-cell compare-cell-${cellVariant}`}>
              {vals.length > 0 ? (
                <>
                  <span className="mono compare-cell-text">{vals.join(', ')}</span>
                  <CopyButton value={vals.join(', ')} />
                </>
              ) : (
                <span className="compare-missing">Not present</span>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
