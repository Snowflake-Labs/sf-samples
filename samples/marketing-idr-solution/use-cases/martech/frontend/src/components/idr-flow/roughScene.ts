export const SVG_NS = 'http://www.w3.org/2000/svg';

export function svgEl<K extends keyof SVGElementTagNameMap>(tag: K): SVGElementTagNameMap[K] {
  return document.createElementNS(SVG_NS, tag);
}

export type TextOpts = {
  className?: string;
  fill?: string;
  anchor?: 'start' | 'middle' | 'end';
  opacity?: number;
};

export function addText(parent: SVGGElement, x: number, y: number, content: string, opts: TextOpts = {}) {
  const t = svgEl('text');
  t.setAttribute('x', String(x));
  t.setAttribute('y', String(y));
  if (opts.anchor) t.setAttribute('text-anchor', opts.anchor);
  if (opts.fill) t.setAttribute('fill', opts.fill);
  if (opts.className) t.setAttribute('class', opts.className);
  if (opts.opacity != null) t.setAttribute('opacity', String(opts.opacity));
  t.textContent = content;
  parent.appendChild(t);
  return t;
}

export function clearLayer(layer: SVGGElement) {
  while (layer.firstChild) layer.removeChild(layer.firstChild);
}

export const easeInOutCubic = (t: number) =>
  t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;

export const lerp = (a: number, b: number, t: number) => a + (b - a) * t;

export const clamp01 = (t: number) => (t < 0 ? 0 : t > 1 ? 1 : t);

export function pulse(t: number) {
  // 0 -> 0, 0.5 -> 1, 1 -> 0
  return Math.sin(clamp01(t) * Math.PI);
}

export function drawArrowhead(g: SVGGElement, fromX: number, fromY: number, toX: number, toY: number, color: string, opacity = 1) {
  const angle = Math.atan2(toY - fromY, toX - fromX);
  const size = 10;
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
  tri.setAttribute('opacity', String(opacity));
  g.appendChild(tri);
}
