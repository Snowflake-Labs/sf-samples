import { useMemo } from 'react';
import * as d3 from 'd3';
import type { MatchExplanation } from '../api';

export interface IDRMatchQualityDonutProps {
  matches: MatchExplanation[];
  size?: number;
}

type Bucket = 'high' | 'medium' | 'low';

const BUCKET_LABELS: Record<Bucket, string> = {
  high: 'High',
  medium: 'Medium',
  low: 'Low',
};

const BUCKET_DESCRIPTIONS: Record<Bucket, string> = {
  high: 'Score >= 0.95',
  medium: '0.85 - 0.95',
  low: '< 0.85',
};

function bucketFor(score: number): Bucket {
  if (score >= 0.95) return 'high';
  if (score >= 0.85) return 'medium';
  return 'low';
}

export function IDRMatchQualityDonut({ matches, size = 220 }: IDRMatchQualityDonutProps) {
  const counts = useMemo(() => {
    const c: Record<Bucket, number> = { high: 0, medium: 0, low: 0 };
    for (const m of matches) c[bucketFor(m.match_score ?? 0)] += 1;
    return c;
  }, [matches]);

  const total = matches.length;
  const buckets: Bucket[] = ['high', 'medium', 'low'];

  const r = size / 2;
  const inner = r * 0.78;
  const outer = r - 4;

  const arcs = useMemo(() => {
    const data = buckets.map(b => ({ bucket: b, value: counts[b] }));
    const pie = d3.pie<{ bucket: Bucket; value: number }>().value(d => d.value).sort(null).padAngle(0.02);
    const arc = d3.arc<d3.PieArcDatum<{ bucket: Bucket; value: number }>>().innerRadius(inner).outerRadius(outer).cornerRadius(2);
    return pie(data).map(p => ({
      bucket: p.data.bucket,
      d: arc(p) ?? '',
      value: p.data.value,
    }));
  }, [buckets, counts, inner, outer]);

  const fillForBucket = (b: Bucket) => {
    if (b === 'high') return 'var(--graph-confidence-high, #1DB588)';
    if (b === 'medium') return 'var(--graph-confidence-med, #FCCF54)';
    return 'var(--graph-confidence-low, #E32442)';
  };

  return (
    <div className="idr-donut-wrap">
      <svg width={size} height={size} role="img" aria-label="Match quality donut">
        <g transform={`translate(${r}, ${r})`}>
          {total === 0 ? (
            <circle r={outer} fill="none" stroke="var(--border)" strokeDasharray="4 4" />
          ) : (
            arcs.map(a => (
              <path key={a.bucket} d={a.d} fill={fillForBucket(a.bucket)} />
            ))
          )}
          <text textAnchor="middle" dominantBaseline="middle" y={-6} fontSize={28} fontWeight={500} fill="var(--text)">
            {total}
          </text>
          <text textAnchor="middle" dominantBaseline="middle" y={16} fontSize={11} fill="var(--text-muted)">
            match pair{total === 1 ? '' : 's'}
          </text>
        </g>
      </svg>
      <ul className="idr-donut-legend">
        {buckets.map(b => {
          const v = counts[b];
          const pct = total ? Math.round((v / total) * 100) : 0;
          return (
            <li key={b} className={`idr-donut-legend-row idr-donut-${b}`}>
              <span className="idr-donut-swatch" style={{ background: fillForBucket(b) }} />
              <div className="idr-donut-legend-text">
                <div className="idr-donut-legend-label">{BUCKET_LABELS[b]}</div>
                <div className="idr-donut-legend-sub">{BUCKET_DESCRIPTIONS[b]}</div>
              </div>
              <div className="idr-donut-legend-vals">
                <span className="idr-donut-legend-count">{v}</span>
                <span className="idr-donut-legend-pct">{pct}%</span>
              </div>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

export default IDRMatchQualityDonut;
