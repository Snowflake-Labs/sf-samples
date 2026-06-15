import { useMemo, useState, useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import * as d3 from 'd3';
import axios from 'axios';
import type { MatchExplanation, SourceRecord } from '../api';
import { readCssVar } from '../theme/readCssVar';
import { useResolvedTheme } from '../theme';

/* ── Shared helpers ──────────────────────────────────────────────── */

function getSourceLabel(source: string): string {
  if (source.includes('BID_REQUEST')) return 'Bid Request';
  if (source.includes('LOYALTY')) return 'Loyalty Member';
  if (source.includes('SHOPIFY')) return 'Shopify Order';
  if (source.includes('WEB') || source.includes('CLICKSTREAM')) return 'Web Clickstream';
  if (source.includes('POS')) return 'POS Transaction';
  if (source.includes('TAPAD')) return 'Tapad Graph';
  if (source.includes('EXPERIAN')) return 'Experian Graph';
  return source.replace(/_/g, ' ');
}

function shortId(id: string, max = 16) {
  if (id.length <= max) return id;
  return `${id.slice(0, Math.max(4, max - 1))}\u2026`;
}

function confidenceTier(c: number): 'high' | 'medium' | 'low' {
  if (c >= 95) return 'high';
  if (c >= 85) return 'medium';
  return 'low';
}

const SOURCE_HUES: Record<string, string> = {
  POS_TRANSACTION: '#6366f1',
  POS_TRANSACTION_RAW: '#6366f1',
  LOYALTY_MEMBER: '#f59e0b',
  LOYALTY_MEMBER_RAW: '#f59e0b',
  WEB_CLICKSTREAM: '#0ea5e9',
  WEB_CLICKSTREAM_RAW: '#0ea5e9',
  SHOPIFY_ORDER: '#10b981',
  SHOPIFY_ORDER_RAW: '#10b981',
  SSP_BID_REQUEST: '#6366f1',
  SSP_BID_REQUEST_RAW: '#6366f1',
  TAPAD_GRAPH: '#f59e0b',
  TAPAD_GRAPH_RAW: '#f59e0b',
  EXPERIAN_GRAPH: '#10b981',
  EXPERIAN_GRAPH_RAW: '#10b981',
};
function sourceColor(src: string): string {
  return SOURCE_HUES[src] || '#94a3b8';
}

/* ── Main export ─────────────────────────────────────────────────── */

export function IDRForceGraph({
  identityId,
  matches,
  sourceRecords,
  selectedRecordId: controlledId,
  onSelectRecord,
  hideMatrix,
  hideLegend,
}: {
  identityId: string;
      matches: MatchExplanation[];
  sourceRecords: SourceRecord[];
  selectedRecordId?: string | null;
  onSelectRecord?: (id: string | null) => void;
  hideMatrix?: boolean;
  hideLegend?: boolean;
}) {
  const [internalSelected, setInternalSelected] = useState<string | null>(null);
  const isControlled = controlledId !== undefined;
  const selectedRecordId = isControlled ? controlledId ?? null : internalSelected;
  const setSelectedRecordId = (next: string | null) => {
    if (!isControlled) setInternalSelected(next);
    onSelectRecord?.(next);
  };

  const { sources, recordsBySource, recSourceMap } = useMemo(() => {
    const srcSet = new Set<string>();
    const bySource = new Map<string, SourceRecord[]>();
    const rsm = new Map<string, string>();
    sourceRecords.forEach(r => {
      srcSet.add(r.source);
      rsm.set(r.record_id, r.source);
      if (!bySource.has(r.source)) bySource.set(r.source, []);
      bySource.get(r.source)!.push(r);
    });
    return { sources: Array.from(srcSet).sort(), recordsBySource: bySource, recSourceMap: rsm };
  }, [sourceRecords]);

  if (sourceRecords.length === 0) return <div className="empty-state">No source records to display</div>;

  return (
    <>
      <RadialView
        sources={sources}
        sourceRecords={sourceRecords}
        matches={matches}
        recSourceMap={recSourceMap}
        recordsBySource={recordsBySource}
        onRecordClick={(id) => setSelectedRecordId(selectedRecordId === id ? null : id)}
        selectedRecordId={selectedRecordId}
        hideLegend={hideLegend}
      />
      {!hideMatrix && selectedRecordId && (
        <MatchMatrix
          recordId={selectedRecordId}
          matches={matches}
          recSourceMap={recSourceMap}
          clusterId={identityId}
          onClose={() => setSelectedRecordId(null)}
        />
      )}
    </>
  );
}

/* ── Radial Arc Diagram ──────────────────────────────────────────── */

function RadialView({
  sources,
  sourceRecords,
  matches,
  recSourceMap,
  recordsBySource,
  onRecordClick,
  selectedRecordId,
  hideLegend,
}: {
  sources: string[];
  sourceRecords: SourceRecord[];
  matches: MatchExplanation[];
  recSourceMap: Map<string, string>;
  recordsBySource: Map<string, SourceRecord[]>;
  onRecordClick?: (recordId: string) => void;
  selectedRecordId?: string | null;
  hideLegend?: boolean;
}) {
  const svgRef = useRef<SVGSVGElement>(null);
  const theme = useResolvedTheme();
  const [hoveredRecord, setHoveredRecord] = useState<string | null>(null);
  const [tooltip, setTooltip] = useState<{ x: number; y: number; html: string } | null>(null);

  const orderedRecords = useMemo(() => {
    const result: SourceRecord[] = [];
    sources.forEach(src => {
      const recs = recordsBySource.get(src) || [];
      recs.sort((a, b) => a.record_id.localeCompare(b.record_id));
      result.push(...recs);
    });
    return result;
  }, [sources, recordsBySource]);

  const recordIndex = useMemo(() => {
    const map = new Map<string, number>();
    orderedRecords.forEach((r, i) => map.set(r.record_id, i));
    return map;
  }, [orderedRecords]);

  useEffect(() => {
    if (!svgRef.current || orderedRecords.length === 0) return;

    const el = svgRef.current;
    const size = Math.min(el.clientWidth || 380, 380);
    const margin = 40;
    const cx = size / 2;
    const cy = size / 2;
    const outerR = size / 2 - margin;
    const bandW = 10;
    const innerR = outerR - bandW;
    const dotR = innerR + bandW / 2;
    const arcR = innerR - 8;

    const svg = d3.select(el);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${size} ${size}`);

    const g = svg.append('g').attr('transform', `translate(${cx},${cy})`);

    const confHigh = readCssVar('--graph-confidence-high') || '#16a34a';
    const confMed = readCssVar('--graph-confidence-med') || '#d97706';
    const confLow = readCssVar('--graph-confidence-low') || '#dc2626';
    const textColor = readCssVar('--text') || '#1e293b';
    const mutedColor = readCssVar('--text-muted') || '#64748b';
    const borderColor = readCssVar('--border') || '#e2e8f0';

    const n = orderedRecords.length;
    const gapAngle = 0.08;
    const totalGap = gapAngle * sources.length;
    const usableAngle = 2 * Math.PI - totalGap;

    interface SectorInfo { source: string; startAngle: number; endAngle: number; records: SourceRecord[]; }
    const sectors: SectorInfo[] = [];
    let angle = -Math.PI / 2;
    sources.forEach(src => {
      const recs = recordsBySource.get(src) || [];
      const sectorAngle = (recs.length / n) * usableAngle;
      sectors.push({ source: src, startAngle: angle, endAngle: angle + sectorAngle, records: recs });
      angle += sectorAngle + gapAngle;
    });

    const recordAngles = new Map<string, number>();
    sectors.forEach(sector => {
      const count = sector.records.length;
      const pad = count > 1 ? 0.04 : 0;
      const padStart = sector.startAngle + pad;
      const padEnd = sector.endAngle - pad;
      sector.records.forEach((r, i) => {
        const t = count > 1 ? i / (count - 1) : 0.5;
        const a = padStart + t * (padEnd - padStart);
        recordAngles.set(r.record_id, a);
      });
    });

    // d3.arc uses D3 convention (0 = 12 o'clock, CW) while our angles
    // use standard math (0 = 3 o'clock). Offset by +π/2 to align.
    const halfPi = Math.PI / 2;
    const sectorArc = d3.arc<SectorInfo>()
      .innerRadius(innerR)
      .outerRadius(outerR)
      .startAngle(d => d.startAngle + halfPi)
      .endAngle(d => d.endAngle + halfPi)
      .cornerRadius(4);

    g.append('g')
      .selectAll('path')
      .data(sectors)
      .enter()
      .append('path')
      .attr('d', sectorArc as any)
      .attr('fill', d => sourceColor(d.source))
      .attr('fill-opacity', 0.08)
      .attr('stroke', d => sourceColor(d.source))
      .attr('stroke-opacity', 0.2)
      .attr('stroke-width', 1);

    const defaultArcColor = borderColor;

    const arcGroup = g.append('g');
    matches.forEach((m, mi) => {
      const a1 = recordAngles.get(m.identifier_1);
      const a2 = recordAngles.get(m.identifier_2);
      if (a1 == null || a2 == null) return;

      const pct = Math.round(m.match_score * 100);
      const tier = confidenceTier(pct);
      const hoverColor = tier === 'high' ? confHigh : tier === 'medium' ? confMed : confLow;

      const x1 = Math.cos(a1) * arcR;
      const y1 = Math.sin(a1) * arcR;
      const x2 = Math.cos(a2) * arcR;
      const y2 = Math.sin(a2) * arcR;

      arcGroup.append('path')
        .attr('d', `M ${x1} ${y1} Q 0 0, ${x2} ${y2}`)
        .attr('fill', 'none')
        .attr('stroke', defaultArcColor)
        .attr('stroke-opacity', 0.35)
        .attr('stroke-width', 1)
        .attr('data-src', m.identifier_1)
        .attr('data-tgt', m.identifier_2)
        .attr('data-color', hoverColor)
        .attr('class', `radial-arc rarc-${mi}`);
    });

    function highlightRecord(rid: string) {
      arcGroup.selectAll<SVGPathElement, unknown>('.radial-arc').each(function () {
        const arc = d3.select(this);
        const src = arc.attr('data-src');
        const tgt = arc.attr('data-tgt');
        if (src === rid || tgt === rid) {
          arc.attr('stroke', arc.attr('data-color')).attr('stroke-opacity', 0.85).attr('stroke-width', 2.5);
    } else {
          arc.attr('stroke-opacity', 0.06).attr('stroke-width', 0.5);
        }
      });
      dots.attr('r', d => d.record_id === rid ? 7 : 4.5)
        .attr('stroke-width', d => d.record_id === rid ? 2 : 1.5);
    }

    function resetHighlight() {
      arcGroup.selectAll('.radial-arc')
        .attr('stroke', defaultArcColor).attr('stroke-opacity', 0.35).attr('stroke-width', 1);
      dots.attr('r', 4.5).attr('stroke-width', 1.5);
    }

    const dots = g.append('g')
      .selectAll('circle')
      .data(orderedRecords)
      .enter()
      .append('circle')
      .attr('cx', d => Math.cos(recordAngles.get(d.record_id)!) * dotR)
      .attr('cy', d => Math.sin(recordAngles.get(d.record_id)!) * dotR)
      .attr('r', 4.5)
      .attr('fill', d => sourceColor(d.source))
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5)
      .style('cursor', 'pointer');

    if (selectedRecordId) {
      highlightRecord(selectedRecordId);
    }

    dots.on('mouseenter', function (event, d) {
      d3.select(this).attr('r', 7).attr('stroke-width', 2);
      highlightRecord(d.record_id);

      const connCount = matches.filter(m => m.identifier_1 === d.record_id || m.identifier_2 === d.record_id).length;
      setTooltip({
        x: event.offsetX, y: event.offsetY,
        html: `<strong>${shortId(d.record_id, 20)}</strong><br/>${getSourceLabel(d.source)}<br/>${connCount} match${connCount !== 1 ? 'es' : ''}`,
      });
    })
    .on('click', function (_event, d) {
      onRecordClick?.(d.record_id);
    })
    .on('mouseleave', function (_event, d) {
      d3.select(this).attr('r', 4.5).attr('stroke-width', 1.5);
      if (selectedRecordId && selectedRecordId !== d.record_id) {
        highlightRecord(selectedRecordId);
      } else if (!selectedRecordId) {
        resetHighlight();
      }
      setTooltip(null);
    });

    g.append('text')
      .attr('text-anchor', 'middle')
      .attr('y', -6)
      .attr('font-size', '24px')
      .attr('font-weight', '700')
      .attr('fill', textColor)
      .text(orderedRecords.length);
    g.append('text')
      .attr('text-anchor', 'middle')
      .attr('y', 14)
      .attr('font-size', '11px')
      .attr('fill', mutedColor)
      .text('records');
    g.append('text')
      .attr('text-anchor', 'middle')
      .attr('y', 30)
      .attr('font-size', '11px')
      .attr('fill', mutedColor)
      .text(`${matches.length} match pairs`);

  }, [sources, orderedRecords, matches, recordsBySource, theme, onRecordClick, selectedRecordId]);

  return (
    <div className="idr-radial-wrap">
      {!hideLegend && (
        <div className="mtx-legend">
          <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--high" /> {'\u2265'}95%</span>
          <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--medium" /> 85{'\u2013'}94%</span>
          <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--low" /> &lt;85%</span>
          {sources.map(src => (
            <span key={src} className="mtx-legend-item">
              <span className="mtx-legend-swatch" style={{ background: sourceColor(src) }} /> {getSourceLabel(src)} ({recordsBySource.get(src)?.length ?? 0})
            </span>
          ))}
        </div>
      )}
      <div className="idr-radial-svg-wrap" style={{ position: 'relative' }}>
        <svg ref={svgRef} width="100%" style={{ maxWidth: 380, display: 'block', margin: '0 auto' }} />
        {tooltip && (
          <div className="idr-viz-tooltip" style={{ left: tooltip.x + 12, top: tooltip.y - 8 }} dangerouslySetInnerHTML={{ __html: tooltip.html }} />
        )}
      </div>
      <p className="idr-viz-hint">Hover a dot to highlight its connections. Click a dot to see its match details below.</p>
          </div>
  );
}

/* ── Standalone legend for the IDR radial graph ─────────────────── */

export function IDRGraphLegend({ sourceRecords }: { sourceRecords: SourceRecord[] }) {
  const { sources, recordsBySource } = (() => {
    const srcSet = new Set<string>();
    const bySource = new Map<string, SourceRecord[]>();
    sourceRecords.forEach(r => {
      srcSet.add(r.source);
      if (!bySource.has(r.source)) bySource.set(r.source, []);
      bySource.get(r.source)!.push(r);
    });
    return { sources: Array.from(srcSet).sort(), recordsBySource: bySource };
  })();

  return (
    <div className="mtx-legend">
      <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--high" /> {'\u2265'}95%</span>
      <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--medium" /> 85{'\u2013'}94%</span>
      <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--low" /> &lt;85%</span>
      {sources.map(src => (
        <span key={src} className="mtx-legend-item">
          <span className="mtx-legend-swatch" style={{ background: sourceColor(src) }} /> {getSourceLabel(src)} ({recordsBySource.get(src)?.length ?? 0})
        </span>
      ))}
    </div>
  );
}

/* ── Match Matrix (inline drill-down for a selected record) ────── */

export function MatchMatrix({
  recordId,
  matches,
  recSourceMap,
  clusterId,
  onClose,
}: {
  recordId: string;
  matches: MatchExplanation[];
  recSourceMap: Map<string, string>;
  clusterId: string;
  onClose: () => void;
}) {
  const [selectedPair, setSelectedPair] = useState<{ a: string; b: string } | null>(null);
  const connected = matches.filter(
    m => m.identifier_1 === recordId || m.identifier_2 === recordId,
  );

  connected.sort((a, b) => b.match_score - a.match_score);

  const srcLabel = recSourceMap.get(recordId);

  return (
    <div className="match-matrix">
      <div className="match-matrix-header">
        <div className="match-matrix-title">
          <span
            className="match-matrix-dot"
            style={{ background: srcLabel ? sourceColor(srcLabel) : '#94a3b8' }}
          />
          <strong>{shortId(recordId, 28)}</strong>
          <span className="match-matrix-source">
            {srcLabel ? getSourceLabel(srcLabel) : ''}
          </span>
          <span className="match-matrix-count">
            {connected.length} match{connected.length !== 1 ? 'es' : ''}
          </span>
        </div>
        <button type="button" className="match-matrix-close" onClick={onClose} aria-label="Close">
          &times;
        </button>
      </div>
      <div className="match-matrix-table-wrap">
        <table className="match-matrix-table">
          <thead>
            <tr>
              <th>Matched Record</th>
              <th>Source</th>
              <th>Linkage</th>
              <th>Score</th>
            </tr>
          </thead>
          <tbody>
            {connected.map((m, i) => {
              const otherId = m.identifier_1 === recordId ? m.identifier_2 : m.identifier_1;
              const otherSrc = recSourceMap.get(otherId);
              const pct = Math.round(m.match_score * 100);
              const tier = confidenceTier(pct);
              const linkage = m.rule_description || m.match_rule || '—';
              return (
                <tr key={i} className="match-matrix-row-clickable" onClick={() => setSelectedPair({ a: recordId, b: otherId })}>
                  <td className="mono match-matrix-rid">{shortId(otherId, 24)}</td>
                  <td>
                    {otherSrc && (
                      <span className="match-matrix-src-pill" style={{ color: sourceColor(otherSrc) }}>
                        {getSourceLabel(otherSrc)}
                      </span>
                    )}
                  </td>
                  <td className="match-matrix-linkage">{linkage}</td>
                  <td>
                    <span className={`match-matrix-score match-matrix-score--${tier}`}>
                      {pct}%
                    </span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {selectedPair && (
        <MatchPairPopup
          recordA={selectedPair.a}
          recordB={selectedPair.b}
          clusterId={clusterId}
          onClose={() => setSelectedPair(null)}
        />
      )}
    </div>
  );
}


interface PairDetail {
  record_a: { id: string; source: string; email?: string; phone?: string; first_name?: string; last_name?: string; street?: string; city?: string; state?: string; postal_code?: string; loyalty_id?: string; device_id?: string };
  record_b: { id: string; source: string; email?: string; phone?: string; first_name?: string; last_name?: string; street?: string; city?: string; state?: string; postal_code?: string; loyalty_id?: string; device_id?: string };
  edge: {
    rule_id?: string; rule_name?: string; match_type?: string; score?: number; matched_on?: string;
    ml_prob?: number; ml_model_version?: string;
    top_positive_features?: Array<{ name?: string; feature?: string; contribution?: number; value?: number }>;
    top_negative_features?: Array<{ name?: string; feature?: string; contribution?: number; value?: number }>;
    llm_verdict?: string; llm_rationale?: string; llm_ml_score?: number;
  };
}

const FEATURE_LABELS: Record<string, string> = {
  email_eq: 'Email', phone_eq: 'Phone', email_handle_eq: 'Email Handle',
  phone_last7_eq: 'Phone Last-7', loyalty_id_eq: 'Loyalty ID', device_id_eq: 'Device ID',
  jw_first_name: 'First Name (JW)', jw_last_name: 'Last Name (JW)', jw_street: 'Street (JW)',
  postal_eq: 'Postal Code', state_eq: 'State', nickname_first_eq: 'Nickname',
  EMAIL_EQ_OR_HEM: 'Email', PHONE_EQ: 'Phone', LOYALTY_ID_EQ: 'Loyalty ID',
  JW_LAST_NAME: 'Last Name (JW)', JW_FIRST_NAME: 'First Name (JW)', POSTAL_EQ: 'Postal Code',
  STATE_EQ: 'State', JW_STREET: 'Street (JW)', DEVICE_ID_EQ: 'Device ID',
  PHONE_LAST7_EQ: 'Phone Last-7', NICKNAME_FIRST_EQ: 'Nickname',
};

function MatchPairPopup({ recordA, recordB, clusterId, onClose }: {
  recordA: string; recordB: string; clusterId: string; onClose: () => void;
}) {
  const { data, isLoading } = useQuery<PairDetail>({
    queryKey: ['match-pair-detail', recordA, recordB],
    queryFn: () => axios.get('/api/match-pair-detail', {
      params: { record_a: recordA, record_b: recordB, cluster_id: clusterId },
    }).then(r => r.data),
    staleTime: 60000,
  });

  return (
    <div className="pair-popup-overlay" onClick={onClose}>
      <div className="pair-popup" onClick={e => e.stopPropagation()}>
        <div className="pair-popup-header">
          <span className="pair-popup-title">Match Detail</span>
          <button className="pair-popup-close" onClick={onClose}>&times;</button>
        </div>
        {isLoading && <div className="pair-popup-loading">Loading pair details…</div>}
        {data && (
          <>
            <div className="pair-popup-records">
              <RecordColumn label="Record A" rec={data.record_a} />
              <div className="pair-popup-divider" />
              <RecordColumn label="Record B" rec={data.record_b} />
            </div>
            <div className="pair-popup-edge">
              <div className="pair-popup-rule">
                <span className="pair-popup-rule-name">{data.edge.rule_name || data.edge.rule_id || 'Unknown'}</span>
                {data.edge.score != null && <span className="pair-popup-rule-score">{(data.edge.score * 100).toFixed(1)}%</span>}
              </div>
              {data.edge.match_type === 'DETERMINISTIC' && data.edge.matched_on && (
                <div className="pair-popup-section">
                  <div className="pair-popup-section-label">Matched On</div>
                  <div className="pair-popup-matched-on">{data.edge.matched_on}</div>
                </div>
              )}
              {(data.edge.match_type === 'ML' || data.edge.match_type === 'LLM') && (
                <div className="pair-popup-section">
                  <div className="pair-popup-section-label">ML Score: {(() => { const s = data.edge.ml_prob ?? data.edge.llm_ml_score; return s != null ? `${(s * 100).toFixed(1)}%` : '—'; })()}</div>
                  {data.edge.top_positive_features && data.edge.top_positive_features.length > 0 && (
                    <div className="pair-popup-features">
                      {data.edge.top_positive_features.map((f, i) => {
                        const fname = f.name || f.feature || '';
                        const label = FEATURE_LABELS[fname] || fname || `Feature ${i + 1}`;
                        const v = f.contribution ?? f.value;
                        const display = typeof v === 'number' ? v.toFixed(3) : v != null ? String(v) : '';
                        return (
                          <div key={i} className="pair-popup-feature pair-popup-feature--pos">
                            <span className="pair-popup-feat-name">{label}</span>
                            <span className="pair-popup-feat-val">{display}</span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                  {data.edge.top_negative_features && data.edge.top_negative_features.length > 0 && (
                    <div className="pair-popup-features">
                      {data.edge.top_negative_features.map((f, i) => {
                        const fname = f.name || f.feature || '';
                        const label = FEATURE_LABELS[fname] || fname || `Feature ${i + 1}`;
                        const v = f.contribution ?? f.value;
                        const display = typeof v === 'number' ? v.toFixed(3) : v != null ? String(v) : '';
                        return (
                          <div key={i} className="pair-popup-feature pair-popup-feature--neg">
                            <span className="pair-popup-feat-name">{label}</span>
                            <span className="pair-popup-feat-val">{display}</span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}
              {data.edge.match_type === 'LLM' && data.edge.llm_verdict && (
                <div className="pair-popup-section">
                  <div className="pair-popup-section-label">LLM Verdict: <strong>{data.edge.llm_verdict}</strong></div>
                  {data.edge.llm_rationale && <div className="pair-popup-llm-rationale">{data.edge.llm_rationale}</div>}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}

function RecordColumn({ label, rec }: { label: string; rec: PairDetail['record_a'] }) {
  const fields = [
    { k: 'Source', v: rec.source },
    { k: 'Name', v: [rec.first_name, rec.last_name].filter(Boolean).join(' ') || null },
    { k: 'Email', v: rec.email },
    { k: 'Phone', v: rec.phone },
    { k: 'Address', v: [rec.street, rec.city, rec.state, rec.postal_code].filter(Boolean).join(', ') || null },
    { k: 'Loyalty ID', v: rec.loyalty_id },
    { k: 'Device ID', v: rec.device_id },
  ];
  return (
    <div className="pair-popup-col">
      <div className="pair-popup-col-label">{label} <span className="pair-popup-col-source">({rec.source})</span></div>
      {fields.map((f, i) => f.v ? (
        <div key={i} className="pair-popup-field">
          <span className="pair-popup-field-key">{f.k}</span>
          <span className="pair-popup-field-val">{f.v}</span>
        </div>
      ) : null)}
    </div>
  );
}

/* ── Hierarchical Edge Bundling (commented out — uncomment to re-enable) ──
 *
 * interface HebNode {
 *   name: string;
 *   source?: string;
 *   record?: SourceRecord;
 *   children?: HebNode[];
 * }
 *
 * function BundleView({
 *   sources,
 *   sourceRecords,
 *   matches,
 *   recordsBySource,
 * }: {
 *   sources: string[];
 *   sourceRecords: SourceRecord[];
 *   matches: MatchExplanation[];
 *   recSourceMap?: Map<string, string>;
 *   recordsBySource: Map<string, SourceRecord[]>;
 * }) {
 *   const svgRef = useRef<SVGSVGElement>(null);
 *   const theme = useResolvedTheme();
 *   const [tooltip, setTooltip] = useState<{ x: number; y: number; html: string } | null>(null);
 *
 *   useEffect(() => {
 *     if (!svgRef.current || sourceRecords.length === 0) return;
 *
 *     const el = svgRef.current;
 *     const size = Math.min(el.clientWidth || 680, 680);
 *     const margin = 150;
 *     const innerRadius = size / 2 - margin;
 *
 *     const svg = d3.select(el);
 *     svg.selectAll('*').remove();
 *     svg.attr('viewBox', `0 0 ${size} ${size}`);
 *
 *     const g = svg.append('g').attr('transform', `translate(${size / 2},${size / 2})`);
 *
 *     const confHigh = readCssVar('--graph-confidence-high') || '#16a34a';
 *     const confMed = readCssVar('--graph-confidence-med') || '#d97706';
 *     const confLow = readCssVar('--graph-confidence-low') || '#dc2626';
 *     const textColor = readCssVar('--text') || '#1e293b';
 *     const mutedColor = readCssVar('--text-muted') || '#64748b';
 *
 *     const treeData: HebNode = {
 *       name: 'root',
 *       children: sources.map(src => ({
 *         name: src,
 *         source: src,
 *         children: (recordsBySource.get(src) || [])
 *           .sort((a, b) => a.record_id.localeCompare(b.record_id))
 *           .map(r => ({ name: r.record_id, source: src, record: r })),
 *       })),
 *     };
 *
 *     const root = d3.hierarchy(treeData);
 *     d3.cluster<HebNode>().size([2 * Math.PI, innerRadius])(root as d3.HierarchyNode<HebNode>);
 *
 *     const typedRoot = root as unknown as d3.HierarchyPointNode<HebNode>;
 *     const leaves = typedRoot.leaves();
 *     const leafMap = new Map<string, d3.HierarchyPointNode<HebNode>>();
 *     leaves.forEach(l => leafMap.set(l.data.name, l));
 *
 *     const line = d3.lineRadial<d3.HierarchyPointNode<HebNode>>()
 *       .curve(d3.curveBundle.beta(0.85))
 *       .radius(d => d.y)
 *       .angle(d => d.x);
 *
 *     const linkGroup = g.append('g').attr('fill', 'none');
 *
 *     matches.forEach(m => {
 *       const src = leafMap.get(m.identifier_1);
 *       const tgt = leafMap.get(m.identifier_2);
 *       if (!src || !tgt) return;
 *
 *       const pct = Math.round(m.match_score * 100);
 *       const tier = confidenceTier(pct);
 *       const color = tier === 'high' ? confHigh : tier === 'medium' ? confMed : confLow;
 *
 *       const pathNodes = src.path(tgt) as d3.HierarchyPointNode<HebNode>[];
 *
 *       linkGroup.append('path')
 *         .attr('d', line(pathNodes)!)
 *         .attr('stroke', color)
 *         .attr('stroke-opacity', 0.3)
 *         .attr('stroke-width', 1)
 *         .attr('data-src', m.identifier_1)
 *         .attr('data-tgt', m.identifier_2)
 *         .attr('class', 'bundle-link');
 *     });
 *
 *     const sourceGroups = typedRoot.children || [];
 *     sourceGroups.forEach(srcNode => {
 *       const srcLeaves = srcNode.leaves();
 *       if (srcLeaves.length === 0) return;
 *
 *       const minA = d3.min(srcLeaves, d => d.x)!;
 *       const maxA = d3.max(srcLeaves, d => d.x)!;
 *       const pad = 0.015;
 *
 *       const sArc = d3.arc()
 *         .innerRadius(innerRadius + 2)
 *         .outerRadius(innerRadius + 16)
 *         .startAngle(minA - pad)
 *         .endAngle(maxA + pad)
 *         .cornerRadius(3);
 *
 *       g.append('path')
 *         .attr('d', sArc({} as any)!)
 *         .attr('fill', sourceColor(srcNode.data.name))
 *         .attr('fill-opacity', 0.12)
 *         .attr('stroke', sourceColor(srcNode.data.name))
 *         .attr('stroke-opacity', 0.3)
 *         .attr('stroke-width', 1);
 *
 *       const midA = (minA + maxA) / 2;
 *       const flip = midA > Math.PI;
 *
 *       g.append('text')
 *         .attr('dy', '.31em')
 *         .attr('transform', `rotate(${midA * 180 / Math.PI - 90}) translate(${innerRadius + 24})${flip ? ' rotate(180)' : ''}`)
 *         .attr('text-anchor', flip ? 'end' : 'start')
 *         .attr('font-size', '11px')
 *         .attr('font-weight', '600')
 *         .attr('fill', textColor)
 *         .text(`${getSourceLabel(srcNode.data.name)} (${srcLeaves.length})`);
 *     });
 *
 *     const dotGroup = g.append('g');
 *     leaves.forEach(leaf => {
 *       const deg = leaf.x * 180 / Math.PI;
 *
 *       dotGroup.append('circle')
 *         .attr('transform', `rotate(${deg - 90}) translate(${leaf.y})`)
 *         .attr('r', 3.5)
 *         .attr('fill', sourceColor(leaf.data.source!))
 *         .attr('stroke', '#fff')
 *         .attr('stroke-width', 1)
 *         .style('cursor', 'pointer')
 *         .on('mouseenter', function (event) {
 *           d3.select(this).attr('r', 6).attr('stroke-width', 2);
 *
 *           const rid = leaf.data.name;
 *           linkGroup.selectAll<SVGPathElement, unknown>('.bundle-link').each(function () {
 *             const lnk = d3.select(this);
 *             if (lnk.attr('data-src') === rid || lnk.attr('data-tgt') === rid) {
 *               lnk.attr('stroke-opacity', 0.85).attr('stroke-width', 2.5);
 *             } else {
 *               lnk.attr('stroke-opacity', 0.04).attr('stroke-width', 0.5);
 *             }
 *           });
 *
 *           const connCount = matches.filter(mx => mx.identifier_1 === rid || mx.identifier_2 === rid).length;
 *           setTooltip({
 *             x: event.offsetX, y: event.offsetY,
 *             html: `<strong>${shortId(rid, 20)}</strong><br/>${getSourceLabel(leaf.data.source!)}<br/>${connCount} match${connCount !== 1 ? 'es' : ''}`,
 *           });
 *         })
 *         .on('mouseleave', function () {
 *           d3.select(this).attr('r', 3.5).attr('stroke-width', 1);
 *           linkGroup.selectAll('.bundle-link')
 *             .attr('stroke-opacity', 0.3)
 *             .attr('stroke-width', 1);
 *           setTooltip(null);
 *         });
 *     });
 *
 *     if (leaves.length <= 40) {
 *       g.append('g').selectAll('text')
 *         .data(leaves)
 *         .enter()
 *         .append('text')
 *         .attr('dy', '.31em')
 *         .attr('transform', d => {
 *           const deg = d.x * 180 / Math.PI;
 *           return `rotate(${deg - 90}) translate(${d.y + 10})${d.x > Math.PI ? ' rotate(180)' : ''}`;
 *         })
 *         .attr('text-anchor', d => d.x > Math.PI ? 'end' : 'start')
 *         .attr('font-size', '8px')
 *         .attr('fill', mutedColor)
 *         .text(d => shortId(d.data.name, 10));
 *     }
 *
 *     g.append('text').attr('text-anchor', 'middle').attr('y', -6).attr('font-size', '24px').attr('font-weight', '700').attr('fill', textColor).text(sourceRecords.length);
 *     g.append('text').attr('text-anchor', 'middle').attr('y', 14).attr('font-size', '11px').attr('fill', mutedColor).text('records');
 *     g.append('text').attr('text-anchor', 'middle').attr('y', 30).attr('font-size', '11px').attr('fill', mutedColor).text(`${matches.length} match pairs`);
 *
 *   }, [sources, sourceRecords, matches, recordsBySource, theme]);
 *
 *   return (
 *     <div className="idr-bundle-wrap">
 *       <div className="idr-bundle-svg-wrap" style={{ position: 'relative' }}>
 *         <svg ref={svgRef} width="100%" style={{ maxWidth: 680, display: 'block', margin: '0 auto' }} />
 *         {tooltip && (
 *           <div className="idr-viz-tooltip" style={{ left: tooltip.x + 12, top: tooltip.y - 8 }} dangerouslySetInnerHTML={{ __html: tooltip.html }} />
 *         )}
 *       </div>
 *       <p className="idr-viz-hint">Hover a node to highlight its connections. Edges bundle by source group.</p>
 *       <div className="mtx-legend">
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--high" /> ≥95%</span>
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--medium" /> 85–94%</span>
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--low" /> &lt;85%</span>
 *         {sources.map(src => (
 *           <span key={src} className="mtx-legend-item">
 *             <span className="mtx-legend-swatch" style={{ background: sourceColor(src) }} /> {getSourceLabel(src)}
 *           </span>
 *         ))}
 *       </div>
 *     </div>
 *   );
 * }
 *
 * ── end BundleView ─────────────────────────────────────────────── */

/* ── Force-Directed Tree (commented out — uncomment to re-enable) ──
 *
 * interface ForceNode extends d3.SimulationNodeDatum {
 *   id: string;
 *   type: 'root' | 'source' | 'record';
 *   srcKey?: string;
 *   record?: SourceRecord;
 * }
 *
 * function ForceTreeView({
 *   sources,
 *   sourceRecords,
 *   matches,
 *   recordsBySource,
 * }: {
 *   sources: string[];
 *   sourceRecords: SourceRecord[];
 *   matches: MatchExplanation[];
 *   recordsBySource: Map<string, SourceRecord[]>;
 * }) {
 *   const svgRef = useRef<SVGSVGElement>(null);
 *   const theme = useResolvedTheme();
 *   const [tooltip, setTooltip] = useState<{ x: number; y: number; html: string } | null>(null);
 *
 *   useEffect(() => {
 *     if (!svgRef.current || sourceRecords.length === 0) return;
 *
 *     const el = svgRef.current;
 *     const width = el.clientWidth || 680;
 *     const height = Math.max(420, Math.min(600, sourceRecords.length * 6));
 *
 *     const svg = d3.select(el);
 *     svg.selectAll('*').remove();
 *     svg.attr('viewBox', `0 0 ${width} ${height}`);
 *
 *     const confHigh = readCssVar('--graph-confidence-high') || '#16a34a';
 *     const confMed = readCssVar('--graph-confidence-med') || '#d97706';
 *     const confLow = readCssVar('--graph-confidence-low') || '#dc2626';
 *     const textColor = readCssVar('--text') || '#1e293b';
 *     const mutedColor = readCssVar('--text-muted') || '#64748b';
 *     const borderColor = readCssVar('--border') || '#e2e8f0';
 *
 *     interface TreeDatum { name: string; type: 'root' | 'source' | 'record'; srcKey?: string; record?: SourceRecord; children?: TreeDatum[]; }
 *
 *     const treeData: TreeDatum = {
 *       name: '__root__',
 *       type: 'root',
 *       children: sources.map(src => ({
 *         name: src,
 *         type: 'source' as const,
 *         srcKey: src,
 *         children: (recordsBySource.get(src) || [])
 *           .sort((a, b) => a.record_id.localeCompare(b.record_id))
 *           .map(r => ({ name: r.record_id, type: 'record' as const, srcKey: src, record: r })),
 *       })),
 *     };
 *
 *     const root = d3.hierarchy(treeData);
 *     const allNodes = root.descendants();
 *     const treeLinks = root.links();
 *
 *     const simNodes: ForceNode[] = allNodes.map(n => ({
 *       id: n.data.name,
 *       type: n.data.type,
 *       srcKey: n.data.srcKey,
 *       record: n.data.record,
 *     }));
 *
 *     const simLinks: d3.SimulationLinkDatum<ForceNode>[] = treeLinks.map(l => ({
 *       source: (l.source as any).data.name as string,
 *       target: (l.target as any).data.name as string,
 *     }));
 *
 *     const nodeMap = new Map<string, ForceNode>();
 *
 *     const simulation = d3.forceSimulation(simNodes)
 *       .force('link', d3.forceLink<ForceNode, d3.SimulationLinkDatum<ForceNode>>(simLinks)
 *         .id(d => d.id)
 *         .distance(d => {
 *           const src = d.source as ForceNode;
 *           return src.type === 'root' ? 100 : 40;
 *         })
 *         .strength(0.9))
 *       .force('charge', d3.forceManyBody<ForceNode>().strength(d =>
 *         d.type === 'root' ? -400 : d.type === 'source' ? -120 : -18))
 *       .force('x', d3.forceX(width / 2).strength(0.06))
 *       .force('y', d3.forceY(height / 2).strength(0.06))
 *       .force('collide', d3.forceCollide<ForceNode>().radius(d =>
 *         d.type === 'root' ? 8 : d.type === 'source' ? 16 : 5));
 *
 *     simNodes.forEach(n => nodeMap.set(n.id, n));
 *
 *     const matchLinkG = svg.append('g').attr('class', 'force-match-links');
 *
 *     const linkSel = svg.append('g')
 *       .selectAll('line')
 *       .data(simLinks)
 *       .enter()
 *       .append('line')
 *       .attr('stroke', borderColor)
 *       .attr('stroke-opacity', 0.35)
 *       .attr('stroke-width', 1);
 *
 *     const nodeSel = svg.append('g')
 *       .selectAll<SVGCircleElement, ForceNode>('circle')
 *       .data(simNodes.filter(n => n.type !== 'root'))
 *       .enter()
 *       .append('circle')
 *       .attr('r', d => d.type === 'source' ? 12 : 4.5)
 *       .attr('fill', d => d.srcKey ? sourceColor(d.srcKey) : '#94a3b8')
 *       .attr('stroke', '#fff')
 *       .attr('stroke-width', d => d.type === 'source' ? 2 : 1)
 *       .style('cursor', 'pointer');
 *
 *     const labelSel = svg.append('g')
 *       .selectAll('text')
 *       .data(simNodes.filter(n => n.type === 'source'))
 *       .enter()
 *       .append('text')
 *       .attr('font-size', '11px')
 *       .attr('font-weight', '600')
 *       .attr('fill', textColor)
 *       .attr('text-anchor', 'middle')
 *       .attr('dy', -18)
 *       .text(d => `${getSourceLabel(d.srcKey!)} (${recordsBySource.get(d.srcKey!)?.length ?? 0})`);
 *
 *     simulation.on('tick', () => {
 *       linkSel
 *         .attr('x1', d => (d.source as ForceNode).x!)
 *         .attr('y1', d => (d.source as ForceNode).y!)
 *         .attr('x2', d => (d.target as ForceNode).x!)
 *         .attr('y2', d => (d.target as ForceNode).y!);
 *       nodeSel.attr('cx', d => d.x!).attr('cy', d => d.y!);
 *       labelSel.attr('x', d => d.x!).attr('y', d => d.y!);
 *     });
 *
 *     nodeSel.call(
 *       d3.drag<SVGCircleElement, ForceNode>()
 *         .on('start', (event, d) => {
 *           if (!event.active) simulation.alphaTarget(0.3).restart();
 *           d.fx = d.x; d.fy = d.y;
 *         })
 *         .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
 *         .on('end', (event, d) => {
 *           if (!event.active) simulation.alphaTarget(0);
 *           d.fx = null; d.fy = null;
 *         }),
 *     );
 *
 *     nodeSel
 *       .on('mouseenter', function (event, d) {
 *         if (d.type === 'record') {
 *           d3.select(this).attr('r', 7).attr('stroke-width', 2);
 *           matchLinkG.selectAll('*').remove();
 *           const rid = d.id;
 *           matches.forEach(m => {
 *             if (m.identifier_1 !== rid && m.identifier_2 !== rid) return;
 *             const otherId = m.identifier_1 === rid ? m.identifier_2 : m.identifier_1;
 *             const other = nodeMap.get(otherId);
 *             if (!other) return;
 *             const pct = Math.round(m.match_score * 100);
 *             const tier = confidenceTier(pct);
 *             const color = tier === 'high' ? confHigh : tier === 'medium' ? confMed : confLow;
 *             matchLinkG.append('line')
 *               .attr('x1', d.x!).attr('y1', d.y!)
 *               .attr('x2', other.x!).attr('y2', other.y!)
 *               .attr('stroke', color)
 *               .attr('stroke-opacity', 0.55)
 *               .attr('stroke-width', 1.5);
 *           });
 *           const connCount = matches.filter(mx => mx.identifier_1 === rid || mx.identifier_2 === rid).length;
 *           setTooltip({
 *             x: event.offsetX, y: event.offsetY,
 *             html: `<strong>${shortId(rid, 20)}</strong><br/>${getSourceLabel(d.srcKey!)}<br/>${connCount} match${connCount !== 1 ? 'es' : ''}`,
 *           });
 *         } else if (d.type === 'source') {
 *           d3.select(this).attr('opacity', 0.8);
 *           const cnt = recordsBySource.get(d.srcKey!)?.length ?? 0;
 *           setTooltip({
 *             x: event.offsetX, y: event.offsetY,
 *             html: `<strong>${getSourceLabel(d.srcKey!)}</strong><br/>${cnt} record${cnt !== 1 ? 's' : ''}`,
 *           });
 *         }
 *       })
 *       .on('mouseleave', function (_event, d) {
 *         if (d.type === 'record') {
 *           d3.select(this).attr('r', 4.5).attr('stroke-width', 1);
 *           matchLinkG.selectAll('*').remove();
 *         } else {
 *           d3.select(this).attr('opacity', 1);
 *         }
 *         setTooltip(null);
 *       });
 *
 *     return () => { simulation.stop(); };
 *   }, [sources, sourceRecords, matches, recordsBySource, theme]);
 *
 *   return (
 *     <div className="idr-force-wrap">
 *       <div className="idr-force-svg-wrap" style={{ position: 'relative' }}>
 *         <svg ref={svgRef} width="100%" style={{ maxWidth: 680, display: 'block', margin: '0 auto' }} />
 *         {tooltip && (
 *           <div className="idr-viz-tooltip" style={{ left: tooltip.x + 12, top: tooltip.y - 8 }} dangerouslySetInnerHTML={{ __html: tooltip.html }} />
 *         )}
 *       </div>
 *       <p className="idr-viz-hint">Drag nodes to rearrange. Hover a record to see its match connections.</p>
 *       <div className="mtx-legend">
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--high" /> ≥95%</span>
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--medium" /> 85–94%</span>
 *         <span className="mtx-legend-item"><span className="mtx-legend-swatch mtx-legend-swatch--low" /> &lt;85%</span>
 *         {sources.map(src => (
 *           <span key={src} className="mtx-legend-item">
 *             <span className="mtx-legend-swatch" style={{ background: sourceColor(src) }} /> {getSourceLabel(src)}
 *           </span>
 *         ))}
 *       </div>
 *     </div>
 *   );
 * }
 *
 * ── end ForceTreeView ──────────────────────────────────────────── */
