import { useEffect, useRef, useState } from 'react';
import rough from 'roughjs';
import type { RoughSVG } from 'roughjs/bin/svg';

const SVG_NS = 'http://www.w3.org/2000/svg';

type Palette = {
  text: string;
  textMuted: string;
  surface: string;
  clusterMergedStroke: string;
  clusterMergedFill: string;
  clusterSeparateStroke: string;
  clusterSeparateFill: string;
  memberStroke: string;
  memberFill: string;
  detEdge: string;
  mlEdge: string;
  labelBg: string;
};

const LIGHT: Palette = {
  text: '#0f172a',
  textMuted: '#475569',
  surface: '#ffffff',
  clusterMergedStroke: '#1f6feb',
  clusterMergedFill: 'rgba(31,111,235,0.10)',
  clusterSeparateStroke: '#a07a3a',
  clusterSeparateFill: 'rgba(160,122,58,0.08)',
  memberStroke: '#1f2937',
  memberFill: '#ffffff',
  detEdge: '#1f6feb',
  mlEdge: '#c98a08',
  labelBg: '#ffffff',
};

const DARK: Palette = {
  text: '#e6edf3',
  textMuted: '#9ba8b6',
  surface: '#161b22',
  clusterMergedStroke: '#7aa9ff',
  clusterMergedFill: 'rgba(122,169,255,0.14)',
  clusterSeparateStroke: '#d4a017',
  clusterSeparateFill: 'rgba(212,160,23,0.10)',
  memberStroke: '#e5e7eb',
  memberFill: '#1b2230',
  detEdge: '#7aa9ff',
  mlEdge: '#f3c44a',
  labelBg: '#1b2230',
};

type Member = {
  id: string;
  badge: string;
  source: string;
  name: string;
  email: string;
  x: number;
  y: number;
};

const MEMBER_W = 230;
const MEMBER_H = 110;

const C1 = { x: 40, y: 50, w: 720, h: 360 };
const C2 = { x: 820, y: 230, w: 260, h: 180 };

const M1: Member = { id: 'm1', badge: 'M1', source: 'LOYALTY', name: 'Maria Lopez', email: 'maria@gmail.com', x: 80,  y: 130 };
const M2: Member = { id: 'm2', badge: 'M2', source: 'POS',     name: 'Maria Lopez', email: 'maria@gmail.com', x: 80,  y: 280 };
const M3: Member = { id: 'm3', badge: 'M3', source: 'WEB',     name: 'M. Lopez',    email: 'maria@aol.com',     x: 510, y: 200 };
const M4: Member = { id: 'm4', badge: 'M4', source: 'POS',     name: 'M. Lopez',    email: 'm.lopez@yahoo.com', x: 835, y: 280 };

function readPalette(): Palette {
  return document.documentElement.dataset.theme === 'dark' ? DARK : LIGHT;
}

function svgEl<K extends keyof SVGElementTagNameMap>(tag: K): SVGElementTagNameMap[K] {
  return document.createElementNS(SVG_NS, tag);
}

function addText(parent: SVGGElement, x: number, y: number, content: string, opts: { className?: string; fill?: string; anchor?: 'start' | 'middle' | 'end' } = {}) {
  const t = svgEl('text');
  t.setAttribute('x', String(x));
  t.setAttribute('y', String(y));
  if (opts.anchor) t.setAttribute('text-anchor', opts.anchor);
  if (opts.fill) t.setAttribute('fill', opts.fill);
  if (opts.className) t.setAttribute('class', opts.className);
  t.textContent = content;
  parent.appendChild(t);
}

function drawCluster(rc: RoughSVG, g: SVGGElement, rect: { x: number; y: number; w: number; h: number }, palette: Palette, variant: 'merged' | 'separate', label: string, sub: string) {
  const stroke = variant === 'merged' ? palette.clusterMergedStroke : palette.clusterSeparateStroke;
  const fill = variant === 'merged' ? palette.clusterMergedFill : palette.clusterSeparateFill;
  const dashed = variant === 'separate';
  const node = rc.rectangle(rect.x, rect.y, rect.w, rect.h, {
    stroke,
    strokeWidth: 2.4,
    roughness: 2.8,
    bowing: 3,
    fill,
    fillStyle: 'hachure',
    fillWeight: 1.6,
    hachureGap: 6.5,
    hachureAngle: variant === 'merged' ? -38 : 41,
    disableMultiStroke: false,
    preserveVertices: false,
    strokeLineDash: dashed ? [11, 7] : undefined,
  });
  g.appendChild(node);

  // Tag bar at top-left
  const tagY = rect.y - 12;
  addText(g, rect.x + 14, tagY, label, { className: 'rc-cluster-tag', fill: stroke });
  addText(g, rect.x + 14 + label.length * 14 + 18, tagY, sub, { className: 'rc-cluster-sub', fill: palette.textMuted });
}

function drawMember(rc: RoughSVG, g: SVGGElement, m: Member, palette: Palette) {
  const node = rc.rectangle(m.x, m.y, MEMBER_W, MEMBER_H, {
    stroke: palette.memberStroke,
    strokeWidth: 1.9,
    roughness: 2.4,
    bowing: 2.6,
    fill: palette.memberFill,
    fillStyle: 'solid',
  });
  g.appendChild(node);

  // ID badge (top-left)
  addText(g, m.x + 14, m.y + 26, m.badge, { className: 'rc-id', fill: palette.text });

  // Source pill (top-right)
  const pillW = m.source.length * 9 + 18;
  const pillX = m.x + MEMBER_W - pillW - 12;
  const pillY = m.y + 8;
  const pill = rc.rectangle(pillX, pillY, pillW, 22, {
    stroke: palette.memberStroke,
    strokeWidth: 1.1,
    roughness: 2.2,
    bowing: 2,
    fill: 'transparent',
  });
  g.appendChild(pill);
  addText(g, pillX + pillW / 2, pillY + 16, m.source, { className: 'rc-src', fill: palette.textMuted, anchor: 'middle' });

  // Name
  addText(g, m.x + 14, m.y + 60, m.name, { className: 'rc-name', fill: palette.text });
  // Email
  addText(g, m.x + 14, m.y + 88, m.email, { className: 'rc-email', fill: palette.textMuted });
}

function drawArrowhead(g: SVGGElement, fromX: number, fromY: number, toX: number, toY: number, color: string) {
  const angle = Math.atan2(toY - fromY, toX - fromX);
  const size = 9;
  const a1x = toX - size * Math.cos(angle - Math.PI / 7);
  const a1y = toY - size * Math.sin(angle - Math.PI / 7);
  const a2x = toX - size * Math.cos(angle + Math.PI / 7);
  const a2y = toY - size * Math.sin(angle + Math.PI / 7);
  const tri = svgEl('polygon');
  tri.setAttribute('points', `${toX},${toY} ${a1x},${a1y} ${a2x},${a2y}`);
  tri.setAttribute('fill', color);
  tri.setAttribute('stroke', color);
  tri.setAttribute('stroke-width', '1');
  tri.setAttribute('stroke-linejoin', 'round');
  g.appendChild(tri);
}

function drawEdge(rc: RoughSVG, g: SVGGElement, palette: Palette, from: { x: number; y: number }, to: { x: number; y: number }, color: string, dashed: boolean, label: string) {
  // Compute slightly shortened end so arrowhead sits on the card edge cleanly.
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const len = Math.hypot(dx, dy) || 1;
  const ux = dx / len;
  const uy = dy / len;
  const tipX = to.x - ux * 4;
  const tipY = to.y - uy * 4;

  const line = rc.line(from.x, from.y, tipX, tipY, {
    stroke: color,
    strokeWidth: 2.4,
    roughness: 2.6,
    bowing: 4,
    strokeLineDash: dashed ? [9, 6] : undefined,
  });
  g.appendChild(line);
  drawArrowhead(g, from.x, from.y, tipX, tipY, color);

  // Label box at midpoint
  const mx = (from.x + to.x) / 2;
  const my = (from.y + to.y) / 2;
  const padX = 12;
  const padY = 8;
  const approxW = label.length * 8.5 + padX * 2;
  const boxX = mx - approxW / 2;
  const boxY = my - 13 - padY;
  const box = rc.rectangle(boxX, boxY, approxW, 26 + padY, {
    stroke: color,
    strokeWidth: 1.3,
    roughness: 2,
    bowing: 2,
    fill: palette.labelBg,
    fillStyle: 'solid',
  });
  g.appendChild(box);
  addText(g, mx, my + 5, label, { className: 'rc-edge-label', fill: color, anchor: 'middle' });
}

function RoughClusterSvg() {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const layerRef = useRef<SVGGElement | null>(null);

  useEffect(() => {
    const svg = svgRef.current;
    const layer = layerRef.current;
    if (!svg || !layer) return;

    const draw = () => {
      while (layer.firstChild) layer.removeChild(layer.firstChild);
      const palette = readPalette();
      const rc = rough.svg(svg);

      // Clusters first (back layer)
      drawCluster(rc, layer, C1, palette, 'merged', 'C1', 'Resolved cluster - 3 members');
      drawCluster(rc, layer, C2, palette, 'separate', 'C2', 'Separate cluster - 1 member');

      // Members
      drawMember(rc, layer, M1, palette);
      drawMember(rc, layer, M2, palette);
      drawMember(rc, layer, M3, palette);
      drawMember(rc, layer, M4, palette);

      // Edges
      // M1 bottom-center -> M2 top-center
      drawEdge(
        rc,
        layer,
        palette,
        { x: M1.x + MEMBER_W / 2, y: M1.y + MEMBER_H },
        { x: M2.x + MEMBER_W / 2, y: M2.y },
        palette.detEdge,
        false,
        'R14 - same last name + email'
      );
      // M1 right-mid -> M3 left-mid
      drawEdge(
        rc,
        layer,
        palette,
        { x: M1.x + MEMBER_W, y: M1.y + MEMBER_H / 2 },
        { x: M3.x, y: M3.y + MEMBER_H / 2 },
        palette.mlEdge,
        true,
        'ML scorer - 91% match'
      );
    };

    draw();

    const obs = new MutationObserver(draw);
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);

  return (
    <svg
      ref={svgRef}
      className="hiw2-rough-cluster"
      viewBox="0 0 1120 480"
      preserveAspectRatio="xMidYMid meet"
      role="img"
      aria-labelledby="rcTitle rcDesc"
    >
      <title id="rcTitle">Cluster diagram</title>
      <desc id="rcDesc">
        Cluster C1 merges three records (Maria Lopez from LOYALTY, POS, and WEB) via R14
        deterministic and an ML scorer at 91 percent. Cluster C2 stays separate;
        m.lopez@yahoo.com does not share a strong enough signal to merge.
      </desc>
      <g ref={layerRef} />
    </svg>
  );
}

export function IdrClusterDiagram() {
  const [open, setOpen] = useState(false);
  return (
    <>
      <p className="hiw2-section-lead">
        Identity Resolution links source records into <strong>clusters</strong> using a tier of
        rules. <strong>C1</strong> merges three records that share an exact name+email anchor (R14
        deterministic) and one fuzzy ML match at 91%. <strong>C2</strong> stays separate -{' '}
        <code className="mono">m.lopez@yahoo.com</code> doesn't share a strong enough signal with
        C1 to cross the merge threshold.
      </p>
      <button
        type="button"
        className="hiw2-cluster-legend-toggle"
        onClick={() => setOpen(o => !o)}
        aria-expanded={open}
      >
        {open ? 'Hide legend' : 'Show legend'}
      </button>
      {open && (
        <div className="hiw2-cluster-legend" role="note">
          <div className="hiw2-cluster-legend__row">
            <span className="hiw2-cluster-legend__swatch hiw2-cluster-legend__swatch--det" />
            <span><strong>R14 deterministic</strong> - exact match on a strong anchor (last name + email, loyalty #, etc.)</span>
          </div>
          <div className="hiw2-cluster-legend__row">
            <span className="hiw2-cluster-legend__swatch hiw2-cluster-legend__swatch--ml" />
            <span><strong>ML scorer</strong> - learned similarity score for the long tail of look-alike records</span>
          </div>
          <div className="hiw2-cluster-legend__row">
            <span className="hiw2-cluster-legend__swatch hiw2-cluster-legend__swatch--separate" />
            <span><strong>Separate cluster</strong> - no rule fired strongly enough to merge</span>
          </div>
        </div>
      )}
      <div className="hiw2-flow-wrap hiw2-cluster-flow-wrap">
        <RoughClusterSvg />
      </div>
    </>
  );
}
