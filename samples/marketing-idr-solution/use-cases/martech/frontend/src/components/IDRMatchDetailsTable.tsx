import { useMemo, useState } from 'react';
import { Download } from 'lucide-react';
import type { MatchExplanation, MatchingRule, SourceRecord } from '../api';

export interface IDRMatchDetailsTableProps {
  matches: MatchExplanation[];
  sourceRecords: SourceRecord[];
  matchingRules?: MatchingRule[];
  identityId: string;
}

type SortKey = 'score' | 'last_seen' | 'source';
type SortDir = 'asc' | 'desc';

interface RowVM {
  matched_record_id: string;
  source: string;
  linkage_reason: string;
  rule_id: string;
  rule_name: string;
  score: number;
  confidence: 'high' | 'medium' | 'low';
  last_seen_iso: string | null;
}

function classifyConfidence(score: number): 'high' | 'medium' | 'low' {
  if (score >= 0.9) return 'high';
  if (score >= 0.7) return 'medium';
  return 'low';
}

function relativeTime(iso: string | null): string {
  if (!iso) return '-';
  const d = new Date(iso);
  if (isNaN(d.getTime())) return iso.slice(0, 10);
  const diffMs = Date.now() - d.getTime();
  if (diffMs < 0) return d.toLocaleDateString();
  const sec = Math.floor(diffMs / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 30) return `${day}d ago`;
  const mo = Math.floor(day / 30);
  if (mo < 12) return `${mo}mo ago`;
  const yr = Math.floor(day / 365);
  return `${yr}y ago`;
}

function linkageReason(m: MatchExplanation): string {
  const detail = m.match_details;
  const v1 = detail?.identifier_1;
  const v2 = detail?.identifier_2;
  if (v1 && v2 && v1 === v2) return `Exact match: ${v1}`;
  if (v1 && v2) return `${v1} <-> ${v2}`;
  return m.rule_description || 'Linked via rule';
}

function buildCsv(rows: RowVM[]): string {
  const esc = (v: string | number | null | undefined): string => {
    if (v == null) return '';
    const s = String(v);
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const header = ['Matched Record', 'Source', 'Linkage Reason', 'Rule ID', 'Rule Name', 'Score', 'Confidence', 'Last Seen'];
  const lines = [header.map(esc).join(',')];
  for (const r of rows) {
    lines.push([
      r.matched_record_id,
      r.source,
      r.linkage_reason,
      r.rule_id,
      r.rule_name,
      r.score.toFixed(3),
      r.confidence,
      r.last_seen_iso ?? '',
    ].map(esc).join(','));
  }
  return lines.join('\n');
}

function downloadCsv(filename: string, csv: string) {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function IDRMatchDetailsTable({
  matches,
  sourceRecords,
  matchingRules,
  identityId,
}: IDRMatchDetailsTableProps) {
  const [sortKey, setSortKey] = useState<SortKey>('score');
  const [sortDir, setSortDir] = useState<SortDir>('desc');

  const rows = useMemo<RowVM[]>(() => {
    const recordToSource = new Map<string, string>();
    for (const sr of sourceRecords) {
      if (sr.record_id) recordToSource.set(sr.record_id, sr.source || 'UNKNOWN');
    }
    const ruleNameById = new Map<string, string>();
    for (const r of matchingRules ?? []) ruleNameById.set(r.rule_id, r.rule_name);

    return matches.map(m => {
      // Prefer identifier_2 as "Matched Record"; if it equals identity, use identifier_1.
      const matched = m.identifier_2 || m.identifier_1;
      const score = m.match_score ?? 0;
      const ruleId = m.match_rule || 'UNKNOWN';
      return {
        matched_record_id: matched,
        source: recordToSource.get(matched) || 'UNKNOWN',
        linkage_reason: linkageReason(m),
        rule_id: ruleId,
        rule_name: ruleNameById.get(ruleId) || m.rule_description || ruleId,
        score,
        confidence: classifyConfidence(score),
        last_seen_iso: m.last_seen ?? null,
      };
    });
  }, [matches, sourceRecords, matchingRules]);

  const sortedRows = useMemo(() => {
    const arr = [...rows];
    arr.sort((a, b) => {
      let cmp = 0;
      if (sortKey === 'score') cmp = a.score - b.score;
      else if (sortKey === 'source') cmp = a.source.localeCompare(b.source);
      else if (sortKey === 'last_seen') {
        const av = a.last_seen_iso ? new Date(a.last_seen_iso).getTime() : 0;
        const bv = b.last_seen_iso ? new Date(b.last_seen_iso).getTime() : 0;
        cmp = av - bv;
      }
      return sortDir === 'asc' ? cmp : -cmp;
    });
    return arr;
  }, [rows, sortKey, sortDir]);

  const onSort = (key: SortKey) => {
    if (key === sortKey) setSortDir(d => (d === 'asc' ? 'desc' : 'asc'));
    else {
      setSortKey(key);
      setSortDir(key === 'source' ? 'asc' : 'desc');
    }
  };

  const sortIndicator = (k: SortKey) =>
    sortKey === k ? (sortDir === 'asc' ? ' ^' : ' v') : '';

  const onDownload = () => {
    const csv = buildCsv(sortedRows);
    const yyyymmdd = new Date().toISOString().slice(0, 10).replace(/-/g, '');
    downloadCsv(`match_details_${identityId}_${yyyymmdd}.csv`, csv);
  };

  if (rows.length === 0) {
    return <div className="empty-hint">No match details to display.</div>;
  }

  return (
    <div className="idr-match-details">
      <div className="idr-match-details-toolbar">
        <button
          type="button"
          className="btn-secondary idr-download-btn"
          onClick={onDownload}
        >
          <Download size={14} aria-hidden />
          Download Match Details
        </button>
      </div>
      <div className="idr-table-wrap">
        <table className="idr-table">
          <thead>
            <tr>
              <th>Matched Record</th>
              <th
                className="sortable"
                onClick={() => onSort('source')}
                role="button"
              >
                Source{sortIndicator('source')}
              </th>
              <th>Linkage Reason</th>
              <th>Rule</th>
              <th
                className="num sortable"
                onClick={() => onSort('score')}
                role="button"
              >
                Score{sortIndicator('score')}
              </th>
              <th>Confidence</th>
              <th
                className="sortable"
                onClick={() => onSort('last_seen')}
                role="button"
              >
                Last Seen{sortIndicator('last_seen')}
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedRows.map((r, i) => (
              <tr key={`${r.matched_record_id}-${i}`}>
                <td>
                  <code className="idr-record-id" title={r.matched_record_id}>
                    {r.matched_record_id.slice(0, 8)}...
                  </code>
                </td>
                <td>
                  <span className={`source-pill ${r.source.toLowerCase().replace(/_/g, '-')}`}>
                    {r.source}
                  </span>
                </td>
                <td className="idr-linkage-reason">{r.linkage_reason}</td>
                <td>
                  <span className="rule-pill" title={r.rule_name}>{r.rule_id}</span>
                </td>
                <td className="num">{r.score.toFixed(3)}</td>
                <td>
                  <span className={`idr-confidence-badge idr-confidence-${r.confidence}`}>
                    {r.confidence}
                  </span>
                </td>
                <td title={r.last_seen_iso ?? ''}>{relativeTime(r.last_seen_iso)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default IDRMatchDetailsTable;
