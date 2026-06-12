import { useMemo } from 'react';
import type { MatchExplanation, MatchingRule } from '../api';

export interface IDRTopRulesTableProps {
  matches: MatchExplanation[];
  matchingRules?: MatchingRule[];
  topN?: number;
  onShowAll?: () => void;
}

interface RuleRow {
  rule_id: string;
  rule_name: string;
  links: number;
  avg_score: number;
}

export function IDRTopRulesTable({ matches, matchingRules, topN = 2, onShowAll }: IDRTopRulesTableProps) {
  const rows = useMemo<RuleRow[]>(() => {
    const ruleNameById = new Map<string, string>();
    for (const r of matchingRules ?? []) ruleNameById.set(r.rule_id, r.rule_name);

    const groups = new Map<string, { links: number; sum: number }>();
    for (const m of matches) {
      const id = m.match_rule || 'UNKNOWN';
      let g = groups.get(id);
      if (!g) {
        g = { links: 0, sum: 0 };
        groups.set(id, g);
      }
      g.links += 1;
      g.sum += m.match_score ?? 0;
    }
    const out: RuleRow[] = [];
    for (const [rule_id, g] of groups) {
      out.push({
        rule_id,
        rule_name: ruleNameById.get(rule_id) || rule_id,
        links: g.links,
        avg_score: g.links ? g.sum / g.links : 0,
      });
    }
    out.sort((a, b) => b.links - a.links || b.avg_score - a.avg_score);
    return out;
  }, [matches, matchingRules]);

  const visible = rows.slice(0, topN);
  const totalLinks = rows.reduce((s, r) => s + r.links, 0) || 1;

  if (rows.length === 0) {
    return <div className="empty-hint">No matching rules applied.</div>;
  }

  return (
    <div className="idr-table-wrap">
      <table className="idr-table">
        <thead>
          <tr>
            <th>Rule</th>
            <th className="num">Links</th>
            <th className="num">Impact</th>
            <th className="num">Score</th>
          </tr>
        </thead>
        <tbody>
          {visible.map(r => (
            <tr key={r.rule_id}>
              <td>
                <span className="idr-rule-id">{r.rule_id}</span>
                <span className="idr-rule-name">{r.rule_name}</span>
              </td>
              <td className="num">{r.links}</td>
              <td className="num">{Math.round((r.links / totalLinks) * 100)}%</td>
              <td className="num">{r.avg_score.toFixed(2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > topN && (
        <button
          type="button"
          className="tab-inline-link idr-table-toggle"
          onClick={onShowAll}
        >
          See all {rows.length} rules &rarr;
        </button>
      )}
    </div>
  );
}

export { type RuleRow };
export default IDRTopRulesTable;
