import { useMemo, useState } from 'react';
import * as d3 from 'd3';
import type { MatchExplanation, SourceRecord } from '../api';

const SOURCE_PALETTE = [
  '#3B82F6', // blue
  '#10B981', // emerald
  '#F59E0B', // amber
  '#A855F7', // purple
  '#EF4444', // red
  '#06B6D4', // cyan
  '#EC4899', // pink
  '#84CC16', // lime
];

function colorForSource(source: string, allSources: string[]): string {
  const idx = allSources.indexOf(source);
  return SOURCE_PALETTE[idx % SOURCE_PALETTE.length];
}

export interface IDRChordDiagramProps {
  matches: MatchExplanation[];
  sourceRecords: SourceRecord[];
  width?: number;
  height?: number;
}

export function IDRChordDiagram({
  matches,
  sourceRecords,
  width = 480,
  height = 480,
}: IDRChordDiagramProps) {
  const [hoveredSource, setHoveredSource] = useState<string | null>(null);
  const [hoveredRibbon, setHoveredRibbon] = useState<{ a: string; b: string; count: number } | null>(null);

  const { sources, matrix, totals, totalMatches } = useMemo(() => {
    const recordToSource = new Map<string, string>();
    for (const sr of sourceRecords) {
      if (sr.record_id) recordToSource.set(sr.record_id, sr.source || 'UNKNOWN');
    }
    const sourceSet = new Set<string>();
    for (const m of matches) {
      const sa = recordToSource.get(m.identifier_1) || 'UNKNOWN';
      const sb = recordToSource.get(m.identifier_2) || 'UNKNOWN';
      sourceSet.add(sa);
      sourceSet.add(sb);
    }
    // Also include any source from sourceRecords (so single-source clusters render with one arc)
    for (const sr of sourceRecords) {
      if (sr.source) sourceSet.add(sr.source);
    }
    const sortedSources = Array.from(sourceSet).sort();
    const idx = new Map(sortedSources.map((s, i) => [s, i]));
    const n = sortedSources.length;
    const mat: number[][] = Array.from({ length: n }, () => new Array(n).fill(0));
    for (const m of matches) {
      const sa = recordToSource.get(m.identifier_1) || 'UNKNOWN';
      const sb = recordToSource.get(m.identifier_2) || 'UNKNOWN';
      const ia = idx.get(sa)!;
      const ib = idx.get(sb)!;
      // symmetric: count once on each side so chord layout is balanced
      mat[ia][ib] += 1;
      if (ia !== ib) mat[ib][ia] += 1;
    }
    const tots = mat.map(row => row.reduce((a, b) => a + b, 0));
    return { sources: sortedSources, matrix: mat, totals: tots, totalMatches: matches.length };
  }, [matches, sourceRecords]);

  const innerR = Math.min(width, height) / 2 - 80;
  const outerR = innerR + 14;

  const chord = useMemo(() => {
    if (sources.length === 0 || totalMatches === 0) return null;
    const layout = d3.chord().padAngle(0.06).sortSubgroups(d3.descending);
    return layout(matrix);
  }, [sources, matrix, totalMatches]);

  const arcGen = d3.arc<d3.ChordGroup>().innerRadius(innerR).outerRadius(outerR);
  const ribbonGen = d3.ribbon<d3.Chord, d3.ChordSubgroup>().radius(innerR);

  // Empty / single-source state
  if (sources.length === 0) {
    return (
      <div className="idr-chord-empty">
        <p>No source mapping available for this cluster.</p>
      </div>
    );
  }

  if (totalMatches === 0 || sources.length === 1) {
    return (
      <div className="idr-chord-empty">
        <svg width={width} height={height} role="img" aria-label="Single source cluster">
          <g transform={`translate(${width / 2}, ${height / 2})`}>
            <circle r={innerR} fill="none" stroke="var(--border)" strokeDasharray="4 4" />
            <circle r={outerR} fill={colorForSource(sources[0], sources)} opacity={0.85} />
          </g>
        </svg>
        <p className="idr-chord-empty-caption">
          {sources.length === 1
            ? 'Single source - no cross-source matches'
            : 'No matches between sources yet.'}
        </p>
      </div>
    );
  }

  if (!chord) return null;

  const isRibbonDimmed = (g: d3.Chord) => {
    if (!hoveredSource) return false;
    const i = sources.indexOf(hoveredSource);
    return g.source.index !== i && g.target.index !== i;
  };

  return (
    <div className="idr-chord-wrap">
      <svg width={width} height={height} role="img" aria-label="Source-to-source match chord diagram">
        <g transform={`translate(${width / 2}, ${height / 2})`}>
          {chord.groups.map(g => {
            const src = sources[g.index];
            const fill = colorForSource(src, sources);
            const dim = hoveredSource && hoveredSource !== src ? 0.25 : 1;
            const d = arcGen(g) ?? '';
            // Label position
            const angle = (g.startAngle + g.endAngle) / 2;
            const labelR = outerR + 18;
            const lx = Math.sin(angle) * labelR;
            const ly = -Math.cos(angle) * labelR;
            const anchor = angle > Math.PI ? 'end' : 'start';
            return (
              <g
                key={src}
                onMouseEnter={() => setHoveredSource(src)}
                onMouseLeave={() => setHoveredSource(null)}
                style={{ cursor: 'pointer' }}
              >
                <path d={d} fill={fill} opacity={dim} />
                <text
                  x={lx}
                  y={ly}
                  textAnchor={anchor}
                  dominantBaseline="middle"
                  fontSize={11}
                  fill="var(--text)"
                  opacity={dim}
                >
                  {src}
                </text>
              </g>
            );
          })}
          {chord.map((g, i) => {
            const a = sources[g.source.index];
            const b = sources[g.target.index];
            const fill = colorForSource(a, sources);
            const dim = isRibbonDimmed(g) ? 0.08 : 0.55;
            const d = ribbonGen(g) ?? '';
            const count = g.source.value;
            return (
              <path
                key={`r-${i}`}
                d={d}
                fill={fill}
                opacity={dim}
                stroke={fill}
                strokeOpacity={dim}
                onMouseEnter={() => setHoveredRibbon({ a, b, count })}
                onMouseLeave={() => setHoveredRibbon(null)}
                style={{ cursor: 'pointer' }}
              />
            );
          })}
        </g>
      </svg>

      <div className="idr-chord-side">
        <div className="idr-chord-summary">
          <div className="idr-chord-summary-row">
            <span className="idr-chord-summary-label">Total matches</span>
            <span className="idr-chord-summary-val">{totalMatches}</span>
          </div>
          <div className="idr-chord-summary-row">
            <span className="idr-chord-summary-label">Source systems</span>
            <span className="idr-chord-summary-val">{sources.length}</span>
          </div>
        </div>
        <ul className="idr-chord-legend">
          {sources.map(s => {
            const i = sources.indexOf(s);
            return (
              <li
                key={s}
                className={hoveredSource && hoveredSource !== s ? 'is-dimmed' : ''}
                onMouseEnter={() => setHoveredSource(s)}
                onMouseLeave={() => setHoveredSource(null)}
              >
                <span className="idr-chord-swatch" style={{ background: colorForSource(s, sources) }} />
                <span className="idr-chord-legend-name">{s}</span>
                <span className="idr-chord-legend-count">{totals[i]}</span>
              </li>
            );
          })}
        </ul>
        {hoveredRibbon && (
          <div className="idr-chord-tooltip">
            <strong>{hoveredRibbon.a}</strong>
            <span className="idr-chord-tooltip-sep">+</span>
            <strong>{hoveredRibbon.b}</strong>
            <span className="idr-chord-tooltip-count">{hoveredRibbon.count} match{hoveredRibbon.count === 1 ? '' : 'es'}</span>
          </div>
        )}
      </div>
    </div>
  );
}

export default IDRChordDiagram;
