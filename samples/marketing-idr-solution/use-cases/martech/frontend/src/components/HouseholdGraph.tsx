import { useMemo, useState } from 'react';
import type { HouseholdInfo, HouseholdMember } from '../api';

interface Props {
  household: HouseholdInfo;
  addressLabel?: string;
  onMemberClick?: (clusterId: string) => void;
  width?: number;
  height?: number;
}

interface MemberPos {
  x: number;
  y: number;
  member: HouseholdMember;
  rule: 'R14' | 'R15';
}

/**
 * Hub-and-spoke radial map. Center node is the shared household address;
 * each member node connects to the hub via an edge labeled with the rule
 * (R14 deterministic, R15 fuzzy). Works for any N >= 2.
 */
export function HouseholdGraph({
  household,
  addressLabel,
  onMemberClick,
  width = 640,
  height,
}: Props) {
  const [hovered, setHovered] = useState<string | null>(null);

  const memberCount = household.members.length;
  // Canvas height is adaptive so the pair layout doesn't leave 60% of the
  // SVG empty. 2 members use a wide-short canvas; 3+ use a square-ish one.
  const canvasHeight = height ?? (memberCount === 2 ? 280 : 480);

  const ruleByCluster = useMemo(() => {
    const m = new Map<string, 'R14' | 'R15'>();
    for (const e of household.edges) {
      const r: 'R14' | 'R15' = e.rule_id === 'MARTECH_R15' ? 'R15' : 'R14';
      if (!m.has(e.a_cluster_id)) m.set(e.a_cluster_id, r);
      if (!m.has(e.b_cluster_id)) m.set(e.b_cluster_id, r);
    }
    return m;
  }, [household.edges]);

  const fallbackRule: 'R14' | 'R15' =
    household.household_type === 'SAME_SURNAME' ? 'R14' : 'R15';

  const positions = useMemo(() => {
    const cx = width / 2;
    const cy = canvasHeight / 2;
    // Sort: current member first so it's drawn on top, others stable by name.
    const members = [...household.members].sort((a, b) => {
      if (a.is_current && !b.is_current) return -1;
      if (!a.is_current && b.is_current) return 1;
      const an = (a.primary_first_name || '') + ' ' + (a.primary_last_name || '');
      const bn = (b.primary_first_name || '') + ' ' + (b.primary_last_name || '');
      return an.localeCompare(bn);
    });
    // For 2 members place horizontally (left/right of hub) so labels never collide vertically.
    // For N>=3 use a radial layout starting at the top.
    const isPair = members.length === 2;
    // Hub is a rectangle (144x60), so members at the same radius from hub
    // CENTER would have unequal visible spoke lengths. Instead, place each
    // member at a fixed distance (SPOKE) from the hub EDGE along its angle.
    const HUB_HALF_W = 72;
    const HUB_HALF_H = 30;
    const SPOKE = isPair ? width / 2 - 110 - HUB_HALF_W : 90;
    const arr: MemberPos[] = members.map((member, i) => {
      let angle: number;
      if (isPair) {
        angle = i === 0 ? Math.PI : 0; // left, right
      } else {
        angle = (i / members.length) * Math.PI * 2 - Math.PI / 2;
      }
      const cosA = Math.cos(angle);
      const sinA = Math.sin(angle);
      // Distance from hub center to its rect edge along this angle.
      const tx = Math.abs(cosA) > 0.001 ? HUB_HALF_W / Math.abs(cosA) : Infinity;
      const ty = Math.abs(sinA) > 0.001 ? HUB_HALF_H / Math.abs(sinA) : Infinity;
      const edgeDist = Math.min(tx, ty);
      const r = edgeDist + SPOKE;
      return {
        x: cx + cosA * r,
        y: cy + sinA * r,
        member,
        rule: ruleByCluster.get(member.cluster_id) ?? fallbackRule,
      };
    });
    return { hub: { x: cx, y: cy }, members: arr };
  }, [household, width, canvasHeight, ruleByCluster, fallbackRule]);

  if (household.members.length === 0) {
    return <div className="empty-hint">No household members</div>;
  }

  const hubLabel = addressLabel || 'Shared address';

  return (
    <svg
      width={width}
      height={canvasHeight}
      viewBox={`0 0 ${width} ${canvasHeight}`}
      className="household-graph"
      style={{ overflow: 'visible' }}
      role="img"
      aria-label={`Household graph with ${household.members.length} members linked by shared address`}
    >
      {/* edges: each member node to the hub */}
      {positions.members.map(({ x, y, member, rule }) => {
        const isHovered = hovered === member.cluster_id;
        const stroke = isHovered ? 'var(--accent)' : 'var(--border-strong)';
        const strokeWidth = isHovered ? 3 : 2;
        const dash = rule === 'R15' ? '6 4' : undefined;
        // The hub is a 144x60 rect, so the visible portion of each spoke
        // varies in length depending on member angle. Compute where the line
        // exits the hub rect and place the chip at the midpoint of the
        // visible segment so every chip is equally spaced from member & hub.
        const HUB_HALF_W = 72;
        const HUB_HALF_H = 30;
        const dx = x - positions.hub.x;
        const dy = y - positions.hub.y;
        const tx = Math.abs(dx) > 0.001 ? HUB_HALF_W / Math.abs(dx) : Infinity;
        const ty = Math.abs(dy) > 0.001 ? HUB_HALF_H / Math.abs(dy) : Infinity;
        const tHubEdge = Math.min(tx, ty);
        const hubEdgeX = positions.hub.x + dx * tHubEdge;
        const hubEdgeY = positions.hub.y + dy * tHubEdge;
        const cxChip = (x + hubEdgeX) / 2;
        const cyChip = (y + hubEdgeY) / 2;
        return (
          <g key={`edge-${member.cluster_id}`}>
            <line
              x1={x}
              y1={y}
              x2={positions.hub.x}
              y2={positions.hub.y}
              stroke={stroke}
              strokeWidth={strokeWidth}
              strokeDasharray={dash}
            />
            <foreignObject
              x={cxChip - 24}
              y={cyChip - 12}
              width={48}
              height={24}
              style={{ pointerEvents: 'none' }}
            >
              <div className="household-edge-chip-wrap">
                <span className="household-edge-chip">{rule}</span>
              </div>
            </foreignObject>
          </g>
        );
      })}

      {/* hub: shared address */}
      <g transform={`translate(${positions.hub.x}, ${positions.hub.y})`}>
        <rect
          className="household-hub-rect"
          x={-72}
          y={-30}
          width={144}
          height={60}
          rx={10}
          ry={10}
        />
        <g transform="translate(-58, -10)">
          {/* Lucide MapPin path, scaled */}
          <path
            d="M9 0c-3.3 0-6 2.7-6 6 0 4.5 6 12 6 12s6-7.5 6-12c0-3.3-2.7-6-6-6z"
            fill="none"
            stroke="var(--text-muted)"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            transform="scale(0.9) translate(0, -2)"
          />
          <circle cx="8" cy="5.5" r="2" fill="none" stroke="var(--text-muted)" strokeWidth="1.5" />
        </g>
        <text x={-32} y={-4} className="household-hub-label">
          Shared address
        </text>
        <text x={-32} y={14} className="household-hub-sub">
          {hubLabel}
        </text>
      </g>

      {/* member nodes */}
      {positions.members.map(({ x, y, member }) => {
        const isHovered = hovered === member.cluster_id;
        const r = 28;
        const fullName = [member.primary_first_name, member.primary_last_name]
          .filter(Boolean)
          .join(' ') || member.cluster_id.slice(0, 8);
        const initials =
          (member.primary_first_name?.[0] ?? '?').toUpperCase() +
          (member.primary_last_name?.[0] ?? '').toUpperCase();
        const conf = member.confidence_score;
        const confPct = conf != null ? `${(conf * 100).toFixed(0)}%` : null;
        const confClass =
          conf != null && conf >= 0.85
            ? 'cc-overview-pill cc-overview-pill--score'
            : 'cc-overview-pill';
        // Place the label outward (away from the hub) so it never collides
        // with the edge chip or the hub. For pair layout keep label below.
        const isPairLayout = positions.members.length === 2;
        const labelAbove = !isPairLayout && y < positions.hub.y;
        const labelY = labelAbove ? -(r + 8 + 56) : r + 8;
        return (
          <g
            key={member.cluster_id}
            transform={`translate(${x}, ${y})`}
            style={{ cursor: 'pointer' }}
            onMouseEnter={() => setHovered(member.cluster_id)}
            onMouseLeave={() => setHovered(null)}
            onClick={() => onMemberClick?.(member.cluster_id)}
          >
            <circle
              r={r}
              fill={member.is_current ? 'var(--accent)' : 'var(--bg-surface)'}
              stroke={isHovered ? 'var(--accent)' : 'var(--border-strong)'}
              strokeWidth={isHovered || member.is_current ? 2.5 : 1.5}
            />
            <text
              textAnchor="middle"
              dominantBaseline="central"
              fontSize="13"
              fontWeight="600"
              fill={member.is_current ? '#fff' : 'var(--text)'}
              style={{ pointerEvents: 'none' }}
            >
              {initials}
            </text>
            <foreignObject
              x={-80}
              y={labelY}
              width={160}
              height={56}
              style={{ pointerEvents: 'none' }}
            >
              <div className="household-graph-node-label">
                <span
                  className="household-graph-name"
                  title={fullName}
                >
                  {fullName}
                </span>
                <div className="household-graph-node-meta">
                  {member.is_current && (
                    <span className="household-graph-you">You</span>
                  )}
                  {confPct && <span className={confClass}>{confPct}</span>}
                </div>
              </div>
            </foreignObject>
          </g>
        );
      })}

      {/* hover tooltip — last layer so it floats above everything */}
      {(() => {
        const hp = positions.members.find(p => p.member.cluster_id === hovered);
        if (!hp) return null;
        const r = 28;
        const isPairLayout = positions.members.length === 2;
        const labelAbove = !isPairLayout && hp.y < positions.hub.y;
        // Place tooltip on the side opposite the label so they don't overlap.
        // labelAbove → tooltip below; otherwise → tooltip above.
        const TIP_W = 240;
        const TIP_H = 168;
        const tipY = labelAbove ? r + 8 + 60 : -(r + 8 + TIP_H);
        // Clamp horizontally so tooltip never escapes the canvas.
        let tipX = -TIP_W / 2;
        const absX = hp.x + tipX;
        if (absX < 4) tipX = 4 - hp.x;
        if (absX + TIP_W > width - 4) tipX = (width - 4 - TIP_W) - hp.x;
        const m = hp.member;
        const fullName = [m.primary_first_name, m.primary_last_name].filter(Boolean).join(' ') || m.cluster_id.slice(0, 8);
        const addressParts = [
          household.shared_street,
          household.shared_city,
          household.shared_state,
          household.shared_postal,
        ].filter(Boolean);
        const address = addressParts.length ? addressParts.join(', ') : null;
        const confPct = m.confidence_score != null ? `${(m.confidence_score * 100).toFixed(1)}%` : null;
        return (
          <foreignObject
            x={hp.x + tipX}
            y={hp.y + tipY}
            width={TIP_W}
            height={TIP_H}
            style={{ pointerEvents: 'none', overflow: 'visible' }}
          >
            <div className="household-graph-tooltip" role="tooltip">
              <div className="household-graph-tooltip-name">{fullName}</div>
              <div className="household-graph-tooltip-rows">
                {m.primary_email && (
                  <div className="household-graph-tooltip-row"><span>Email</span><span className="mono">{m.primary_email}</span></div>
                )}
                {m.primary_phone && (
                  <div className="household-graph-tooltip-row"><span>Phone</span><span className="mono">{m.primary_phone}</span></div>
                )}
                {address && (
                  <div className="household-graph-tooltip-row"><span>Address</span><span className="household-graph-tooltip-addr">{address}</span></div>
                )}
                {confPct && (
                  <div className="household-graph-tooltip-row"><span>Confidence</span><span className="mono">{confPct}</span></div>
                )}
                {!m.primary_email && !m.primary_phone && !address && !confPct && (
                  <div className="household-graph-tooltip-row"><span className="household-graph-tooltip-empty">No additional PII available</span></div>
                )}
              </div>
            </div>
          </foreignObject>
        );
      })()}
    </svg>
  );
}
