import { useMemo, useState, useCallback, useEffect, useRef, type ReactNode } from 'react';
import { KeyRound, Layers3, Maximize2, Minimize2, Minus, Move, Network, Plus, RotateCcw } from 'lucide-react';
import type { WeakLink, WeakLinkPairEvidence } from '../api';

export type LinkKind = 'person' | 'household' | 'candidate';

function tierOf(wl: WeakLink): string {
  return wl.ml_tier || (wl.score_details?.tier as string) || 'CANDIDATE_ONLY';
}

export function linkKindFromStatus(status: string): LinkKind {
  const u = status.toUpperCase();
  if (u === 'MERGE_PERSON' || (u.includes('MERGE') && u.includes('PERSON'))) return 'person';
  if (u.includes('HOUSEHOLD')) return 'household';
  return 'candidate';
}

function partnerCluster(identityId: string, wl: WeakLink): string {
  return wl.cluster_id_a === identityId ? wl.cluster_id_b : wl.cluster_id_a;
}

function shortId(id: string, max = 14) {
  if (id.length <= max) return id;
  return `${id.slice(0, Math.max(6, max - 1))}…`;
}

function statusShortLabel(status: string): string {
  const u = status.toUpperCase();
  if (u === 'MERGE_PERSON') return 'Person merge';
  if (u === 'HOUSEHOLD_LINK') return 'Household';
  if (u === 'CANDIDATE_ONLY') return 'Candidate';
  if (u === 'PENDING') return 'Pending';
  return status.replace(/_/g, ' ') || 'Unknown';
}

function evidenceLine(wl: WeakLink): string {
  const rows = wl.signal_rows ?? [];
  if (rows.length > 0) return rows.map(r => r.label).slice(0, 3).join(' · ');
  if (wl.fingerprint_overlap_count > 0) return `${wl.fingerprint_overlap_count} shared fingerprint overlaps`;
  return 'Weak-link candidate';
}

type Tone = 'det' | 'prob' | 'weak';

function toneForLink(wl: WeakLink): Tone {
  const k = linkKindFromStatus(tierOf(wl));
  if (k === 'person') return 'det';
  if (k === 'household') return 'prob';
  return 'weak';
}

function edgeKindForLink(wl: WeakLink): 'person' | 'household' | 'weak' {
  const t = tierOf(wl).toUpperCase();
  if (t === 'MERGE_PERSON') return 'person';
  if (t.includes('HOUSEHOLD')) return 'household';
  return 'weak';
}

function edgeStyle(
  kind: 'member' | 'person' | 'household' | 'weak' | 'event_cluster' | 'event_ml',
): { stroke: string; strokeWidth: number; dash: string } {
  if (kind === 'event_cluster')
    return { stroke: 'var(--cc-ego-edge-event-cluster)', strokeWidth: 2.25, dash: '0' };
  if (kind === 'event_ml')
    return { stroke: 'var(--cc-ego-edge-event-ml)', strokeWidth: 1.75, dash: '3 5' };
  if (kind === 'member')
    return { stroke: 'var(--cc-ego-edge-member)', strokeWidth: 1.5, dash: '0' };
  if (kind === 'person')
    return { stroke: 'var(--cc-ego-edge-person)', strokeWidth: 3, dash: '6 4' };
  if (kind === 'household')
    return { stroke: 'var(--cc-ego-edge-household)', strokeWidth: 2.5, dash: '6 4' };
  return { stroke: 'var(--cc-ego-edge-weak)', strokeWidth: 2, dash: '5 5' };
}

type GraphNode =
  | {
      id: string;
      type: 'cluster';
      level: number;
      x: number;
      y: number;
      label: string;
      clusterId: string;
      confidencePct: number | null;
      tags: string[];
      tone: Tone;
    }
  | {
      id: string;
      type: 'fingerprint';
      level: number;
      x: number;
      y: number;
      label: string;
      wl: WeakLink;
    }
  | {
      id: string;
      type: 'event';
      level: number;
      x: number;
      y: number;
      evidence: WeakLinkPairEvidence;
      wl: WeakLink;
      /** One dot per participating source record for FINGERPRINT_MATCH (A/B). */
      eventParticipant?: 'a' | 'b';
      eventSourceRecordId?: string;
      eventRecordClusterId?: string | null;
    };

type GraphEdge = {
  source: string;
  target: string;
  kind: 'member' | 'person' | 'household' | 'weak' | 'event_cluster' | 'event_ml';
  weight: number;
  level: number;
  reason: string;
};

const VB = { w: 980, h: 580 };
/** Extra viewBox padding so bridge metric pills and outer nodes are not clipped by `overflow: hidden` on the wrap. */
const VB_PAD_X = 220;
const VB_PAD_Y_TOP = 220;
const VB_PAD_Y_BOT = 220;
const VB_VIEW = {
  x: -VB_PAD_X,
  y: -VB_PAD_Y_TOP,
  w: VB.w + 2 * VB_PAD_X,
  h: VB.h + VB_PAD_Y_TOP + VB_PAD_Y_BOT,
};
/** Concentric ring center (matches `~/Downloads/connected_graph_ego_network_mock_layout.jsx`). */
const EGO_PT = { x: 420, y: 290 };
/** Partner ring radius from ego — larger = longer spokes, more room for bridge labels. */
const EGO_GRAPH_R1 = 258;
/** Prior default radius (210); used to scale mock + guide rings when changing `EGO_GRAPH_R1`. */
const EGO_GRAPH_R1_BASE = 210;
const CC_RING_INNER = Math.round((120 * EGO_GRAPH_R1) / EGO_GRAPH_R1_BASE);
const CC_RING_MID = Math.round((250 * EGO_GRAPH_R1) / EGO_GRAPH_R1_BASE);
/** Radial step per BFS hop beyond L1 — large enough so L2 nodes sit clearly outside L1 boxes + bridge pills. */
const RING_STEP = Math.round((180 * EGO_GRAPH_R1) / EGO_GRAPH_R1_BASE);
const CC_RING_OUTER = EGO_GRAPH_R1 + RING_STEP;
const LAYOUT_SCALE = EGO_GRAPH_R1 / EGO_GRAPH_R1_BASE;
/**
 * Single-partner layout: same hardcoded geometry as the mock (`connected_graph_ego_network_mock_layout.jsx`).
 * C1 is left of ring center; C2 is +220px east of center; F1 midpoint x, y = center−30; events outside cluster boxes.
 */

/**
 * Polar angles (radians) for P partner clusters on a full circle, evenly spaced, first at 12 o'clock.
 * Uses standard math angles with SVG coords (y down): θ = -π/2 + i·2π/P.
 */
function partnerPolarAnglesRad(partnerCount: number): number[] {
  const P = Math.max(0, Math.floor(partnerCount));
  if (P === 0) return [];
  const step = (2 * Math.PI) / P;
  const start = -Math.PI / 2;
  return Array.from({ length: P }, (_, i) => start + i * step);
}

const MOCK_SINGLE = {
  egoDx: Math.round(-40 * LAYOUT_SCALE),
  partnerDx: Math.round(220 * LAYOUT_SCALE),
  fpDy: Math.round(-30 * LAYOUT_SCALE),
  /** Event anchor: left of ego cluster center (mock E1 at 300 vs C1 at 380). */
  eventEgoDx: Math.round(-80 * LAYOUT_SCALE),
  /** Event anchor: right of partner cluster center (mock E3 at 720 vs C2 at 640). */
  eventPartnerDx: Math.round(80 * LAYOUT_SCALE),
  /** Event anchor: above ring horizontal (mock events y=230 vs center y=290). */
  eventDy: Math.round(-60 * LAYOUT_SCALE),
  /** Sibling spacing when multiple events share one side. */
  eventSiblingDx: 36,
  eventSiblingDy: 14,
  /** Multiple fingerprints to same partner: stagger from mock F1. */
  fpLaneDx: Math.round(24 * LAYOUT_SCALE),
  fpLaneDy: Math.round(10 * LAYOUT_SCALE),
} as const;

function mergeClusterDepthFromApi(
  identityId: string,
  links: WeakLink[],
  apiDepth?: Record<string, number> | null,
  apiParent?: Record<string, string | null> | null,
): { depth: Record<string, number>; parent: Record<string, string | null | undefined> } {
  if (apiDepth && Object.keys(apiDepth).length > 0) {
    return {
      depth: { ...apiDepth },
      parent: apiParent ? { ...apiParent } : {},
    };
  }
  const depth: Record<string, number> = { [identityId]: 0 };
  const parent: Record<string, string | null | undefined> = { [identityId]: null };
  for (const wl of links) {
    const a = wl.cluster_id_a;
    const b = wl.cluster_id_b;
    if (a === identityId && b !== identityId) {
      if (depth[b] === undefined) {
        depth[b] = 1;
        parent[b] = identityId;
      }
    } else if (b === identityId && a !== identityId) {
      if (depth[a] === undefined) {
        depth[a] = 1;
        parent[a] = identityId;
      }
    }
  }
  return { depth, parent };
}

function layoutExpandedClusterPositions(
  egoClusterId: string,
  egoGraphX: number,
  egoGraphY: number,
  depthMap: Record<string, number>,
  parentMap: Record<string, string | null | undefined>,
  R1: number,
  ringStep: number,
  useMockSingle: boolean,
): Map<string, { x: number; y: number; theta: number }> {
  const pos = new Map<string, { x: number; y: number; theta: number }>();
  const cx = egoGraphX;
  const cy = egoGraphY;

  pos.set(egoClusterId, { x: cx, y: cy, theta: -Math.PI / 2 });

  const L1 = Object.entries(depthMap)
    .filter(([, d]) => d === 1)
    .map(([c]) => c)
    .sort();

  if (useMockSingle && L1.length === 1) {
    const p0 = L1[0];
    const px = EGO_PT.x + MOCK_SINGLE.partnerDx;
    const py = EGO_PT.y;
    pos.set(p0, { x: px, y: py, theta: Math.atan2(py - cy, px - cx) });
  } else {
    const polarAngles = partnerPolarAnglesRad(Math.max(L1.length, 1));
    L1.forEach((cid, i) => {
      const ang = polarAngles[i] ?? -Math.PI / 2;
      pos.set(cid, {
        x: cx + R1 * Math.cos(ang),
        y: cy + R1 * Math.sin(ang),
        theta: ang,
      });
    });
  }

  for (const d of [2, 3] as const) {
    const r = R1 + (d - 1) * ringStep;
    const atDepth = Object.entries(depthMap)
      .filter(([, dep]) => dep === d)
      .map(([c]) => c);
    const byParent = new Map<string, string[]>();
    const orphans: string[] = [];
    for (const cid of atDepth) {
      const p = parentMap[cid];
      if (p && pos.has(p)) {
        if (!byParent.has(p)) byParent.set(p, []);
        byParent.get(p)!.push(cid);
      } else {
        orphans.push(cid);
      }
    }
    for (const [parentId, kids] of byParent) {
      kids.sort();
      const pp = pos.get(parentId);
      if (!pp) continue;
      /**
       * Angular step between siblings: each cluster box is ~120px wide.
       * At radius r, the chord for angle step θ ≈ r·θ ≥ 140px (box + gap).
       * Minimum step = 140/r rad; cap at π/3 so one parent's kids don't wrap around.
       */
      const minStep = Math.max(0.28, 140 / Math.max(r, 100));
      const step = Math.min(Math.PI / 3, minStep);
      kids.forEach((kid, i) => {
        const off = kids.length === 1 ? 0 : (i - (kids.length - 1) / 2) * step;
        const t = pp.theta + off;
        pos.set(kid, { x: cx + r * Math.cos(t), y: cy + r * Math.sin(t), theta: t });
      });
    }
    if (orphans.length) {
      orphans.sort();
      const start = -Math.PI / 2;
      orphans.forEach((cid, i) => {
        const t = start + (2 * Math.PI * i) / Math.max(orphans.length, 1);
        pos.set(cid, { x: cx + r * Math.cos(t), y: cy + r * Math.sin(t), theta: t });
      });
    }
  }

  return pos;
}

const ZOOM_MIN = 0.6;
const ZOOM_MAX = 2.5;
const ZOOM_STEP = 0.2;

function stopPanEvent(e: React.SyntheticEvent) {
  e.stopPropagation();
}

/**
 * Quadratic Bézier from (x1,y1) to (x2,y2) with control point offset perpendicular to the chord.
 * Guarantees a visible bend (never a degenerate straight Q when bend > 0).
 */
function curvedEdgePathD(
  x1: number,
  y1: number,
  x2: number,
  y2: number,
  bendSign: 1 | -1,
  bendScale = 1,
): string {
  const dx = x2 - x1;
  const dy = y2 - y1;
  const len = Math.hypot(dx, dy) || 1;
  const mx = (x1 + x2) / 2;
  const my = (y1 + y2) / 2;
  const nx = (-dy / len) * bendSign;
  const ny = (dx / len) * bendSign;
  const bend = Math.min(62, Math.max(16, len * 0.15)) * bendScale;
  const cx = mx + nx * bend;
  const cy = my + ny * bend;
  return `M ${x1} ${y1} Q ${cx} ${cy} ${x2} ${y2}`;
}

function edgePathBendScale(kind: GraphEdge['kind']): number {
  if (kind === 'weak' || kind === 'person' || kind === 'household') return 1.12;
  if (kind === 'event_cluster' || kind === 'event_ml') return 1.08;
  return 1;
}

const MAX_EVENT_NODES = 48;

/** Hide per-record event dots and event↔cluster edges (focus on cluster↔cluster links). */
const SHOW_GRAPH_EVENTS = false;

/** SVG event dot radius (`<circle r={11} />`). */
const EVENT_NODE_CIRCLE_R = 11;
/** Minimum gap between dot edges when spreading siblings on an arc (multi-partner layout). */
const EVENT_SIBLING_MIN_GAP_PX = 14;

/**
 * Minimum radians between adjacent sibling event centers on a circular arc so dots do not overlap.
 * chord ≈ 2·r·sin(Δθ/2) ≥ 2·EVENT_NODE_CIRCLE_R + gap
 */
function minEventSiblingArcStep(radius: number, n: number): number {
  if (n <= 1 || radius < 8) return 0;
  const minChord = 2 * EVENT_NODE_CIRCLE_R + EVENT_SIBLING_MIN_GAP_PX;
  const ratio = Math.min(1, minChord / (2 * radius));
  return 2 * Math.asin(ratio);
}

/** A, B, … Z, AA, … for arbitrarily many event dots (Excel-style columns). */
function spreadsheetColumnName(zeroBasedIndex: number): string {
  let n = zeroBasedIndex + 1;
  let s = '';
  while (n > 0) {
    n -= 1;
    s = String.fromCharCode(65 + (n % 26)) + s;
    n = Math.floor(n / 26);
  }
  return s;
}

/** Stable map: event node id → graph marker (sort top→bottom, then left, then id). */
function eventDotMarkerById(nodes: GraphNode[]): Map<string, string> {
  const events = nodes.filter((n): n is Extract<GraphNode, { type: 'event' }> => n.type === 'event');
  const sorted = [...events].sort((a, b) => {
    if (a.y !== b.y) return a.y - b.y;
    if (a.x !== b.x) return a.x - b.x;
    return a.id.localeCompare(b.id);
  });
  const map = new Map<string, string>();
  sorted.forEach((n, i) => map.set(n.id, spreadsheetColumnName(i)));
  return map;
}

function humanizeEvidenceType(t: string): string {
  const parts = t.split(/[_\s]+/).filter(Boolean);
  if (parts.length === 0) return 'Evidence';
  return parts.map(p => p.charAt(0) + p.slice(1).toLowerCase()).join(' ');
}

/** Dot created from IDR_CORE_CLUSTER membership only (not ML pair evidence row). */
function eventNodeIsClusterMembership(evidence: WeakLinkPairEvidence): boolean {
  return (evidence.evidence_type || '').toUpperCase() === 'CLUSTER_SOURCE_RECORD';
}

function eventNodeIsFingerprintMatchParticipant(
  evidence: WeakLinkPairEvidence,
  participant: 'a' | 'b' | undefined,
): boolean {
  return (
    (evidence.evidence_type || '').toUpperCase() === 'FINGERPRINT_MATCH' && participant != null
  );
}

function clusterIdToGraphNodeId(
  clusterId: string | null | undefined,
  identityId: string,
  sortedOtherClusterIds: string[],
): string | null {
  if (!clusterId) return null;
  if (clusterId === identityId) return 'ego';
  const pi = sortedOtherClusterIds.indexOf(clusterId);
  return pi >= 0 ? `c${pi}` : null;
}

/** One FINGERPRINT_MATCH row lists two source records — render two event nodes (each tied to its cluster). */
type FingerprintEventSlice = {
  evidence: WeakLinkPairEvidence;
  participant?: 'a' | 'b';
  sourceRecordId?: string;
  recordClusterId?: string | null;
};

function slicesFromPairEvidence(evs: WeakLinkPairEvidence[]): FingerprintEventSlice[] {
  const out: FingerprintEventSlice[] = [];
  for (const ev of evs) {
    const t = (ev.evidence_type || '').toUpperCase();
    if (t === 'FINGERPRINT_MATCH' && ev.source_record_id_a && ev.source_record_id_b) {
      out.push({
        evidence: ev,
        participant: 'a',
        sourceRecordId: ev.source_record_id_a,
        recordClusterId: ev.record_cluster_id_a ?? null,
      });
      out.push({
        evidence: ev,
        participant: 'b',
        sourceRecordId: ev.source_record_id_b,
        recordClusterId: ev.record_cluster_id_b ?? null,
      });
    } else {
      out.push({ evidence: ev });
    }
  }
  return out;
}

/** Active IDR_CORE_CLUSTER source_record_ids for one side of the weak-link pair. */
function clusterEventsForSide(wl: WeakLink, clusterId: string): string[] {
  if (wl.cluster_id_a === clusterId) return wl.cluster_a_events ?? [];
  if (wl.cluster_id_b === clusterId) return wl.cluster_b_events ?? [];
  return [];
}

function sliceCoverKey(s: FingerprintEventSlice): string | null {
  const cid = s.recordClusterId;
  const rid = s.sourceRecordId;
  if (cid && rid) return `${cid}:${rid}`;
  return null;
}

/** One event dot per (cluster, source_record) across all weak links — avoids duplicating cluster_*_events. */
function claimClusterRecordSlice(s: FingerprintEventSlice, global: Set<string>): boolean {
  const k = sliceCoverKey(s);
  if (!k) return true;
  if (global.has(k)) return false;
  global.add(k);
  return true;
}

/** Graph dot for a cluster member not already represented by FINGERPRINT_MATCH evidence. */
function clusterMemberEvidenceSlice(recordId: string, clusterId: string): FingerprintEventSlice {
  return {
    evidence: {
      evidence_type: 'CLUSTER_SOURCE_RECORD',
      source_record_id_a: recordId,
      record_cluster_id_a: clusterId,
    },
    participant: 'a',
    sourceRecordId: recordId,
    recordClusterId: clusterId,
  };
}

/** Rows that tie two source records via a shared device fingerprint (for cross-pair event edges). */
function fingerprintMatchEvidenceRows(wl: WeakLink): WeakLinkPairEvidence[] {
  const out: WeakLinkPairEvidence[] = [];
  const take = (ev: WeakLinkPairEvidence | null | undefined) => {
    if (!ev) return;
    if ((ev.evidence_type || '').toUpperCase() !== 'FINGERPRINT_MATCH') return;
    if (!ev.source_record_id_a || !ev.source_record_id_b) return;
    if (!ev.record_cluster_id_a || !ev.record_cluster_id_b) return;
    out.push(ev);
  };
  take(wl.fingerprint_match_evidence);
  for (const ev of wl.pair_evidence ?? []) take(ev);
  return out;
}

function undirectedEdgeKey(a: string, b: string): string {
  return a < b ? `${a}|${b}` : `${b}|${a}`;
}

function buildClusterGraph(
  identityId: string,
  links: WeakLink[],
  layoutOpts?: {
    clusterDepth?: Record<string, number> | null;
    clusterParent?: Record<string, string | null> | null;
  },
): { nodes: GraphNode[]; edges: GraphEdge[] } {
  const { depth: mergedDepth, parent: mergedParent } = mergeClusterDepthFromApi(
    identityId,
    links,
    layoutOpts?.clusterDepth,
    layoutOpts?.clusterParent,
  );

  const nodes: GraphNode[] = [];
  const edges: GraphEdge[] = [];
  const egoId = 'ego';
  let evSerial = 0;

  const labelSet = new Set<string>();
  for (const wl of links) {
    if (wl.cluster_id_a === identityId || wl.cluster_id_b === identityId) {
      for (const r of wl.signal_rows ?? []) labelSet.add(r.label);
    }
  }
  const egoTags = [...labelSet].slice(0, 6);
  const egoTouching = links.filter(w => w.cluster_id_a === identityId || w.cluster_id_b === identityId);
  const scored = egoTouching.map(w => w.ml_score).filter((s): s is number => s != null);
  const egoConf = scored.length ? Math.round((scored.reduce((a, b) => a + b, 0) / scored.length) * 100) : null;

  const allClusterIds = new Set<string>();
  allClusterIds.add(identityId);
  for (const wl of links) {
    allClusterIds.add(wl.cluster_id_a);
    allClusterIds.add(wl.cluster_id_b);
  }
  for (const k of Object.keys(mergedDepth)) allClusterIds.add(k);

  /** Sort by BFS depth then id so deeper hops only append; inner clusters keep stable c0,c1,… indices (additive layout). */
  const sortedOthers = [...allClusterIds]
    .filter(id => id !== identityId)
    .sort((a, b) => {
      const da = mergedDepth[a] ?? 99;
      const db = mergedDepth[b] ?? 99;
      if (da !== db) return da - db;
      return a.localeCompare(b);
    });
  /** One direct partner: keep mock ego+partner geometry when expanding to L2+ — do not switch to polar and “redraw” the core. */
  const useMockSingle = sortedOthers.filter(cid => (mergedDepth[cid] ?? 99) === 1).length === 1;
  const egoGraphX = useMockSingle ? EGO_PT.x + MOCK_SINGLE.egoDx : EGO_PT.x;
  const egoGraphY = EGO_PT.y;

  const R1 = EGO_GRAPH_R1;
  const FP_RADIAL_FRAC = 0.55;
  const EVENT_RADIAL_INSET = 40;
  const MULTI_EVENT_OUTWARD_R = 24;
  const PARTNER_BOX_HW = 58;
  const PARTNER_BOX_HH = 28;
  const PARTNER_EVENT_PAST_BOX = 42;
  const FP_CHORD_T = FP_RADIAL_FRAC;
  const FP_BEND_BASE = 34;
  const FP_BEND_PER_LANE = 18;
  const FP_SPOKE_SPREAD = 0.052;
  const EVENT_SIBLING_STEP_MAX = 0.42;
  const EVENT_SIBLING_ARC_MAX = 1.25;

  type PendingEventLayout = {
    key: string;
    wlIdx: number;
    slice: FingerprintEventSlice;
    wl: WeakLink;
    pid: string;
    pi: number;
    thetaP: number;
    fpAng: number;
    rFp: number;
  };
  const pendingEvents: PendingEventLayout[] = [];
  const globalClusterRecordKeys = new Set<string>();

  const positions = layoutExpandedClusterPositions(
    identityId,
    egoGraphX,
    egoGraphY,
    mergedDepth,
    mergedParent,
    R1,
    RING_STEP,
    useMockSingle,
  );

  const clusterNodeByCid = new Map<string, Extract<GraphNode, { type: 'cluster' }>>();

  const egoPos = positions.get(identityId) ?? { x: egoGraphX, y: egoGraphY, theta: -Math.PI / 2 };
  const egoNode: Extract<GraphNode, { type: 'cluster' }> = {
    id: egoId,
    type: 'cluster',
    level: 0,
    x: egoPos.x,
    y: egoPos.y,
    label: 'Current cluster',
    clusterId: identityId,
    confidencePct: egoConf,
    tags: egoTags.length ? egoTags : ['Weak links'],
    tone: 'det',
  };
  nodes.push(egoNode);
  clusterNodeByCid.set(identityId, egoNode);

  sortedOthers.forEach((cid, i) => {
    const p = positions.get(cid);
    if (!p) return;
    const wls = links.filter(w => w.cluster_id_a === cid || w.cluster_id_b === cid);
    const best = wls.length > 0 ? Math.max(...wls.map(w => w.ml_score ?? 0)) : 0;
    const tags = new Set<string>();
    wls.forEach(w => (w.signal_rows ?? []).forEach(r => tags.add(r.label)));
    const rep = wls[0];
    const nodeLevel = mergedDepth[cid] ?? 1;
    const cn: Extract<GraphNode, { type: 'cluster' }> = {
      id: `c${i}`,
      type: 'cluster',
      level: nodeLevel,
      x: p.x,
      y: p.y,
      label: 'Connected cluster',
      clusterId: cid,
      confidencePct: best > 0 ? Math.round(best * 100) : null,
      tags: rep ? [...tags].slice(0, 4) : ['Weak links'],
      tone: rep ? toneForLink(rep) : 'weak',
    };
    nodes.push(cn);
    clusterNodeByCid.set(cid, cn);
  });

  links.forEach((wl, idx) => {
    const a = wl.cluster_id_a;
    const b = wl.cluster_id_b;
    const nodeA = clusterNodeByCid.get(a);
    const nodeB = clusterNodeByCid.get(b);
    if (!nodeA || !nodeB) return;

    const edgeKey = undirectedEdgeKey(a, b);
    const wlsSame = links.filter(w => undirectedEdgeKey(w.cluster_id_a, w.cluster_id_b) === edgeKey);
    const j = wlsSame.indexOf(wl);
    const laneJ = j - (wlsSame.length - 1) / 2;

    const egoN = clusterNodeByCid.get(identityId);
    const thetaP = Math.atan2(nodeB.y - nodeA.y, nodeB.x - nodeA.x);
    const chordLenAB = Math.hypot(nodeB.x - nodeA.x, nodeB.y - nodeA.y) || 1;
    const rFp = FP_RADIAL_FRAC * chordLenAB;

    let fx: number;
    let fy: number;
    let fpAng: number;
    if (useMockSingle && egoN && (a === identityId || b === identityId)) {
      const otherN = a === identityId ? nodeB : nodeA;
      const tP = Math.atan2(otherN.y - egoN.y, otherN.x - egoN.x);
      fpAng = tP + laneJ * FP_SPOKE_SPREAD;
      fx = (egoN.x + otherN.x) / 2 + laneJ * MOCK_SINGLE.fpLaneDx;
      fy = EGO_PT.y + MOCK_SINGLE.fpDy + Math.abs(laneJ) * MOCK_SINGLE.fpLaneDy;
    } else {
      const bx0 = nodeB.x - nodeA.x;
      const by0 = nodeB.y - nodeA.y;
      const cLen = Math.hypot(bx0, by0) || 1;
      const mx = nodeA.x + FP_CHORD_T * bx0;
      const my = nodeA.y + FP_CHORD_T * by0;
      const nx = -by0 / cLen;
      const ny = bx0 / cLen;
      const bend = FP_BEND_BASE + laneJ * FP_BEND_PER_LANE;
      fx = mx + nx * bend;
      fy = my + ny * bend;
      fpAng = Math.atan2(fy - nodeA.y, fx - nodeA.x);
    }

    const edgeLevel = Math.max(mergedDepth[a] ?? 0, mergedDepth[b] ?? 0, 1);

    const fpId = `fp_${idx}`;
    nodes.push({
      id: fpId,
      type: 'fingerprint',
      level: edgeLevel,
      x: fx,
      y: fy,
      label: `F${idx + 1}`,
      wl,
    });

    const bridgeKind = edgeKindForLink(wl);
    edges.push({
      source: nodeA.id,
      target: fpId,
      kind: bridgeKind,
      weight: Math.max(1, Math.round((wl.ml_score ?? 0.45) * 5)),
      level: edgeLevel,
      reason: evidenceLine(wl),
    });
    edges.push({
      source: fpId,
      target: nodeB.id,
      kind: bridgeKind,
      weight: Math.max(1, Math.round((wl.ml_score ?? 0.45) * 5)),
      level: edgeLevel,
      reason: `${statusShortLabel(tierOf(wl))}${wl.ml_score != null ? ` · ${(wl.ml_score * 100).toFixed(0)}%` : ''} · ${wl.fingerprint_overlap_count} shared FP · ${shortId(wl.pair_id, 18)}`,
    });

    if (SHOW_GRAPH_EVENTS) {
      const pid =
        wl.cluster_id_a === identityId
          ? wl.cluster_id_b
          : wl.cluster_id_b === identityId
            ? wl.cluster_id_a
            : wl.cluster_id_b;
      const pi = sortedOthers.indexOf(pid);
      const evs =
        wl.fingerprint_match_evidence != null
          ? [wl.fingerprint_match_evidence]
          : (wl.pair_evidence ?? []);
      const perLinkCap = Math.min(8, Math.max(1, Math.ceil(MAX_EVENT_NODES / Math.max(links.length, 1))));
      const evSlices = slicesFromPairEvidence(evs.slice(0, perLinkCap));
      const fingerprintSlices: FingerprintEventSlice[] = [];
      for (const s of evSlices) {
        if (claimClusterRecordSlice(s, globalClusterRecordKeys)) fingerprintSlices.push(s);
      }
      const extraSlices: FingerprintEventSlice[] = [];
      const maxExtraClusterDots = 16;
      for (const rid of clusterEventsForSide(wl, identityId)) {
        if (extraSlices.length >= maxExtraClusterDots) break;
        const slice = clusterMemberEvidenceSlice(rid, identityId);
        if (claimClusterRecordSlice(slice, globalClusterRecordKeys)) extraSlices.push(slice);
      }
      for (const rid of clusterEventsForSide(wl, pid)) {
        if (extraSlices.length >= maxExtraClusterDots) break;
        const slice = clusterMemberEvidenceSlice(rid, pid);
        if (claimClusterRecordSlice(slice, globalClusterRecordKeys)) extraSlices.push(slice);
      }
      const allEvSlices = [...fingerprintSlices, ...extraSlices].slice(0, 32);

      allEvSlices.forEach((slice, si) => {
        pendingEvents.push({
          key: `${idx}:${si}`,
          wlIdx: idx,
          slice,
          wl,
          pid,
          pi,
          thetaP,
          fpAng,
          rFp,
        });
      });
    }
  });

  if (SHOW_GRAPH_EVENTS) {
  function eventLayoutSide(p: PendingEventLayout): 'ego' | 'partner' | 'unknown' {
    if (p.slice.recordClusterId === identityId) return 'ego';
    if (p.slice.recordClusterId === p.pid) return 'partner';
    return 'unknown';
  }

  function eventGroupKey(p: PendingEventLayout): string {
    const s = eventLayoutSide(p);
    if (s === 'unknown') return `u:${p.pid}:${p.wlIdx}`;
    return `${p.pid}:${s}`;
  }

  const pendingTrimmed =
    pendingEvents.length > MAX_EVENT_NODES ? pendingEvents.slice(0, MAX_EVENT_NODES) : pendingEvents;

  const eventGroups = new Map<string, PendingEventLayout[]>();
  for (const p of pendingTrimmed) {
    const k = eventGroupKey(p);
    if (!eventGroups.has(k)) eventGroups.set(k, []);
    eventGroups.get(k)!.push(p);
  }

  const eventXY = new Map<string, { x: number; y: number }>();
  const egoClusterNode = nodes.find(
    (n): n is Extract<GraphNode, { type: 'cluster' }> => n.id === egoId && n.type === 'cluster',
  );

  for (const items of eventGroups.values()) {
    items.sort((a, b) => {
      if (a.wlIdx !== b.wlIdx) return a.wlIdx - b.wlIdx;
      const pa = a.slice.participant ?? '';
      const pb = b.slice.participant ?? '';
      if (pa !== pb) return pa.localeCompare(pb);
      return (a.slice.sourceRecordId ?? '').localeCompare(b.slice.sourceRecordId ?? '');
    });
    const n = items.length;
    const side0 = eventLayoutSide(items[0]);
    const partnerClusterNode = nodes.find(
      (n): n is Extract<GraphNode, { type: 'cluster' }> =>
        n.type === 'cluster' && n.clusterId === items[0].pid,
    );

    if (useMockSingle && egoClusterNode && partnerClusterNode) {
      /* `connected_graph_ego_network_mock_layout.jsx`: E1 (300,230), E3 (720,230) vs C1 (380,290), C2 (640,290). */
      const mid = (n - 1) / 2;
      for (let i = 0; i < n; i++) {
        const si = i - mid;
        let ex: number;
        let ey: number;
        if (side0 === 'ego') {
          ex =
            egoClusterNode.x +
            MOCK_SINGLE.eventEgoDx +
            si * MOCK_SINGLE.eventSiblingDx;
          ey = EGO_PT.y + MOCK_SINGLE.eventDy + si * MOCK_SINGLE.eventSiblingDy;
        } else if (side0 === 'partner') {
          ex =
            partnerClusterNode.x +
            MOCK_SINGLE.eventPartnerDx +
            si * MOCK_SINGLE.eventSiblingDx;
          ey = EGO_PT.y + MOCK_SINGLE.eventDy + si * MOCK_SINGLE.eventSiblingDy;
        } else {
          ex =
            (egoClusterNode.x + partnerClusterNode.x) / 2 +
            si * MOCK_SINGLE.eventSiblingDx;
          ey = EGO_PT.y + MOCK_SINGLE.eventDy - 44 + si * MOCK_SINGLE.eventSiblingDy;
        }
        eventXY.set(items[i].key, { x: ex, y: ey });
      }
      continue;
    }

    const thetaP0 = items[0].thetaP;
    const fpAng0 = items[0].fpAng;
    const rFp0 = items[0].rFp;

    let layoutRadius = 88;
    if (side0 === 'ego') {
      layoutRadius = Math.max(rFp0 - EVENT_RADIAL_INSET, 52) + MULTI_EVENT_OUTWARD_R;
    } else if (side0 === 'partner') {
      if (partnerClusterNode) {
        const ux0 = Math.cos(thetaP0);
        const uy0 = Math.sin(thetaP0);
        layoutRadius = PARTNER_BOX_HW * Math.abs(ux0) + PARTNER_BOX_HH * Math.abs(uy0) + PARTNER_EVENT_PAST_BOX;
      } else {
        layoutRadius = R1 + PARTNER_EVENT_PAST_BOX + MULTI_EVENT_OUTWARD_R;
      }
    } else {
      layoutRadius =
        Math.max((R1 + rFp0) / 2 - 26, rFp0 - EVENT_RADIAL_INSET + 8) + MULTI_EVENT_OUTWARD_R * 0.5;
    }

    const step =
      n <= 1
        ? 0
        : Math.max(
            minEventSiblingArcStep(layoutRadius, n),
            Math.min(EVENT_SIBLING_STEP_MAX, EVENT_SIBLING_ARC_MAX / Math.max(n - 1, 1)),
          );

    for (let i = 0; i < n; i++) {
      const off = (i - (n - 1) / 2) * step;
      let ang: number;
      let r: number;
      if (side0 === 'partner') {
        /* Past partner bbox along outward spoke (r < R1 from ring center sat *inside* yellow boxes). */
        if (partnerClusterNode) {
          const ux = Math.cos(thetaP0 + off);
          const uy = Math.sin(thetaP0 + off);
          const alongU = PARTNER_BOX_HW * Math.abs(ux) + PARTNER_BOX_HH * Math.abs(uy);
          const past = alongU + PARTNER_EVENT_PAST_BOX;
          eventXY.set(items[i].key, {
            x: partnerClusterNode.x + ux * past,
            y: partnerClusterNode.y + uy * past,
          });
        } else {
          ang = thetaP0 + off;
          r = R1 + PARTNER_EVENT_PAST_BOX + MULTI_EVENT_OUTWARD_R;
          eventXY.set(items[i].key, {
            x: EGO_PT.x + r * Math.cos(ang),
            y: EGO_PT.y + r * Math.sin(ang),
          });
        }
        continue;
      }
      if (side0 === 'ego') {
        ang = thetaP0 + Math.PI + off;
        r = Math.max(rFp0 - EVENT_RADIAL_INSET, 52) + MULTI_EVENT_OUTWARD_R;
      } else {
        ang = fpAng0 + off;
        r = Math.max((R1 + rFp0) / 2 - 26, rFp0 - EVENT_RADIAL_INSET + 8) + MULTI_EVENT_OUTWARD_R * 0.5;
      }
      eventXY.set(items[i].key, {
        x: EGO_PT.x + r * Math.cos(ang),
        y: EGO_PT.y + r * Math.sin(ang),
      });
    }
  }

  for (const p of pendingTrimmed) {
    if (evSerial >= MAX_EVENT_NODES) break;
    const xy = eventXY.get(p.key);
    if (!xy) continue;
    const evId = `ev_${evSerial++}`;
    const slice = p.slice;
    const ev = slice.evidence;
    const wl = p.wl;

    nodes.push({
      id: evId,
      type: 'event',
      level: 1,
      x: xy.x,
      y: xy.y,
      evidence: ev,
      wl,
      eventParticipant: slice.participant,
      eventSourceRecordId: slice.sourceRecordId,
      eventRecordClusterId: slice.recordClusterId,
    });

    const targets = new Set<string>();
    if (slice.participant && slice.recordClusterId) {
      const tid = clusterIdToGraphNodeId(slice.recordClusterId, identityId, sortedOthers);
      if (tid) targets.add(tid);
    } else {
      const na = clusterIdToGraphNodeId(ev.record_cluster_id_a, identityId, sortedOthers);
      const nb = clusterIdToGraphNodeId(ev.record_cluster_id_b, identityId, sortedOthers);
      if (na) targets.add(na);
      if (nb) targets.add(nb);
    }
    if (targets.size === 0) {
      targets.add(egoId);
      const fallbackCid =
        wl.cluster_id_a === identityId
          ? wl.cluster_id_b
          : wl.cluster_id_b === identityId
            ? wl.cluster_id_a
            : wl.cluster_id_a;
      const pn = clusterIdToGraphNodeId(fallbackCid, identityId, sortedOthers);
      if (pn) targets.add(pn);
    }
    for (const tid of targets) {
      edges.push({
        source: evId,
        target: tid,
        kind: 'event_cluster',
        weight: 1,
        level: 1,
        reason: slice.participant
          ? `Source record ${slice.participant.toUpperCase()} · cluster`
          : `Source record tied to cluster node`,
      });
    }
  }

  const eventNodes = nodes.filter((n): n is Extract<GraphNode, { type: 'event' }> => n.type === 'event');
  /** Map cluster_id:source_record_id → event node id (after global dedup, one node per record per cluster). */
  const recordClusterToEventId = new Map<string, string>();
  for (const n of eventNodes) {
    if (n.eventRecordClusterId && n.eventSourceRecordId) {
      recordClusterToEventId.set(`${n.eventRecordClusterId}:${n.eventSourceRecordId}`, n.id);
    }
  }

  const eventMlEdgeSeen = new Set<string>();

  /* Weak-link event edges: only explicit FINGERPRINT_MATCH source_record pairs (not same-pair_id chains). */
  for (const wl of links) {
    for (const ev of fingerprintMatchEvidenceRows(wl)) {
      const keyA = `${ev.record_cluster_id_a}:${ev.source_record_id_a}`;
      const keyB = `${ev.record_cluster_id_b}:${ev.source_record_id_b}`;
      const ida = recordClusterToEventId.get(keyA);
      const idb = recordClusterToEventId.get(keyB);
      if (!ida || !idb || ida === idb) continue;
      const ek = undirectedEdgeKey(ida, idb);
      if (eventMlEdgeSeen.has(ek)) continue;
      eventMlEdgeSeen.add(ek);
      edges.push({
        source: ida,
        target: idb,
        kind: 'event_ml',
        weight: 1,
        level: 1,
        reason: 'Shared device fingerprint (co-observed)',
      });
    }
  }

  } /* SHOW_GRAPH_EVENTS */

  return { nodes, edges };
}

function findNode(nodes: GraphNode[], id: string) {
  return nodes.find(n => n.id === id);
}

function SelectedEventPanel({
  identityId,
  node,
  marker,
}: {
  identityId: string;
  node: Extract<GraphNode, { type: 'event' }>;
  marker: string | undefined;
}) {
  const ev = node.evidence;
  const membership = eventNodeIsClusterMembership(ev);
  const fpPart = eventNodeIsFingerprintMatchParticipant(ev, node.eventParticipant);
  const line1 =
    marker != null
      ? `Node ${marker}`
      : node.eventSourceRecordId ?? humanizeEvidenceType(ev.evidence_type);
  const line2 = membership
    ? 'Source record on cluster'
    : fpPart
      ? 'Fingerprint match (weak-link participant)'
      : humanizeEvidenceType(ev.evidence_type);

  return (
    <>
      <div className="cc-ego-sel-title-line1">{line1}</div>
      <div className="cc-ego-sel-title-line2">{line2}</div>
      {fpPart && (
        <div className="cc-ego-sel-sub">
          Weak-link pair with cluster {shortId(partnerCluster(identityId, node.wl), 14)}
          {node.wl.ml_score != null ? ` · ${(node.wl.ml_score * 100).toFixed(0)}% ML` : ''}
        </div>
      )}
      <dl className="cc-ego-ev-dl">
        {node.eventParticipant != null && node.eventSourceRecordId && (
          <>
            <dt>{membership ? 'Source record' : `Source record (${node.eventParticipant.toUpperCase()})`}</dt>
            <dd className="mono cc-ego-ev-dd-break">{node.eventSourceRecordId}</dd>
          </>
        )}
        {node.eventParticipant != null && node.eventRecordClusterId && (
          <>
            <dt>Cluster</dt>
            <dd className="mono cc-ego-ev-dd-break">{node.eventRecordClusterId}</dd>
          </>
        )}
        {ev.observed_at && (
          <>
            <dt>Observed</dt>
            <dd>{ev.observed_at}</dd>
          </>
        )}
        {ev.device_fingerprint && (
          <>
            <dt>Device fingerprint</dt>
            <dd className="mono">{ev.device_fingerprint}</dd>
          </>
        )}
        {node.eventParticipant == null && ev.record_cluster_id_a && (
          <>
            <dt>Cluster (record A)</dt>
            <dd className="mono cc-ego-ev-dd-break">{shortId(ev.record_cluster_id_a, 36)}</dd>
          </>
        )}
        {node.eventParticipant == null && ev.source_record_id_a && (
          <>
            <dt>Source record (A)</dt>
            <dd className="mono cc-ego-ev-dd-break">{ev.source_record_id_a}</dd>
          </>
        )}
        {node.eventParticipant == null && ev.record_cluster_id_b && (
          <>
            <dt>Cluster (record B)</dt>
            <dd className="mono cc-ego-ev-dd-break">{shortId(ev.record_cluster_id_b, 36)}</dd>
          </>
        )}
        {node.eventParticipant == null && ev.source_record_id_b && (
          <>
            <dt>Source record (B)</dt>
            <dd className="mono cc-ego-ev-dd-break">{ev.source_record_id_b}</dd>
          </>
        )}
        {(ev.publisher_id_a || ev.publisher_id_b) && (
          <>
            <dt>Publishers</dt>
            <dd className="mono">
              {[ev.publisher_id_a, ev.publisher_id_b].filter(Boolean).join(' · ')}
            </dd>
          </>
        )}
        {ev.fingerprint_rarity != null && !Number.isNaN(ev.fingerprint_rarity) && (
          <>
            <dt>Fingerprint rarity</dt>
            <dd>{ev.fingerprint_rarity.toFixed(3)}</dd>
          </>
        )}
      </dl>
    </>
  );
}

/**
 * Ego-network graph (pattern from connected_graph_ego_network_mock.jsx):
 * depth slider, bridge-node toggle, SVG with level rings, selection + edge reasons.
 */
export function ConnectedClustersGraph({
  identityId,
  links,
  graphDepth: graphDepthProp,
  onGraphDepthChange,
  clusterDepth,
  clusterParent,
  children,
  lead,
  asideExtra,
  isRefreshing = false,
}: {
  identityId: string;
  links: WeakLink[];
  /** BFS depth (1–3): matches API fetch when controlled from parent. */
  graphDepth?: number;
  onGraphDepthChange?: (depth: number) => void;
  clusterDepth?: Record<string, number>;
  clusterParent?: Record<string, string | null>;
  /** KPI / summary row above the toolbar (depth, toggles, zoom). */
  children?: ReactNode;
  /** Intro copy below the toolbar and above the graph. */
  lead?: ReactNode;
  /** Extra panel(s) at the top of the right column (e.g. jump links to detail cards below). */
  asideExtra?: ReactNode;
  /** When true, dims the graph viewport and shows a spinner overlay. */
  isRefreshing?: boolean;
}) {
  const [localGraphDepth, setLocalGraphDepth] = useState(1);
  const graphDepth = graphDepthProp ?? localGraphDepth;
  const setGraphDepth = onGraphDepthChange ?? setLocalGraphDepth;

  const [showSignals, setShowSignals] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [isDragging, setIsDragging] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const dragRef = useRef({ x: 0, y: 0, panX: 0, panY: 0 });
  const panRef = useRef(pan);
  const draggingRef = useRef(false);
  const svgWrapRef = useRef<HTMLDivElement>(null);
  panRef.current = pan;

  const { nodes, edges } = useMemo(
    () =>
      buildClusterGraph(identityId, links, {
        clusterDepth: clusterDepth ?? null,
        clusterParent: clusterParent ?? null,
      }),
    [identityId, links, clusterDepth, clusterParent],
  );

  const eventMarkerById = useMemo(() => eventDotMarkerById(nodes), [nodes]);

  const [selectedId, setSelectedId] = useState<string>(() => {
    const firstPartner = nodes.find(n => n.type === 'cluster' && n.level === 1);
    return firstPartner?.id ?? 'ego';
  });

  const visibleNodes = useMemo(() => {
    return nodes.filter(n => {
      if (n.level > graphDepth) return false;
      if (n.type === 'fingerprint' && !showSignals) return false;
      if (n.type === 'event') return false;
      return true;
    });
  }, [nodes, graphDepth, showSignals]);

  const visibleIds = useMemo(() => new Set(visibleNodes.map(n => n.id)), [visibleNodes]);

  const visibleEdges = useMemo(() => {
    return edges.filter(e => {
      if (e.level > graphDepth) return false;
      return visibleIds.has(e.source) && visibleIds.has(e.target);
    });
  }, [edges, graphDepth, visibleIds]);

  const selectedNode = findNode(nodes, selectedId);

  useEffect(() => {
    if (findNode(nodes, selectedId)) return;
    const fallback = nodes.find(n => n.type === 'cluster' && n.level === 1)?.id ?? 'ego';
    setSelectedId(fallback);
  }, [nodes, selectedId]);

  const onSelect = useCallback((id: string) => {
    setSelectedId(id);
  }, []);

  /** Compute zoom + pan so all visible nodes fit in the SVG viewport with padding. */
  const fitToView = useCallback(() => {
    if (visibleNodes.length === 0) return;
    const PAD = 100;
    let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
    for (const n of visibleNodes) {
      minX = Math.min(minX, n.x);
      maxX = Math.max(maxX, n.x);
      minY = Math.min(minY, n.y);
      maxY = Math.max(maxY, n.y);
    }
    minX -= PAD; maxX += PAD; minY -= PAD; maxY += PAD;
    const bboxW = maxX - minX || 1;
    const bboxH = maxY - minY || 1;
    const fitZoom = Math.min(
      VB_VIEW.w / bboxW,
      VB_VIEW.h / bboxH,
      ZOOM_MAX,
    );
    const clampedZoom = Math.max(ZOOM_MIN, Math.min(ZOOM_MAX, Number(fitZoom.toFixed(2))));
    const bboxCx = (minX + maxX) / 2;
    const bboxCy = (minY + maxY) / 2;
    const vbCx = VB_VIEW.x + VB_VIEW.w / 2;
    const vbCy = VB_VIEW.y + VB_VIEW.h / 2;
    setPan({
      x: vbCx - bboxCx * clampedZoom,
      y: vbCy - bboxCy * clampedZoom,
    });
    setZoom(clampedZoom);
  }, [visibleNodes]);

  useEffect(() => {
    fitToView();
  }, [identityId, visibleNodes.length]);

  const zoomIn = useCallback(() => {
    setZoom(z => Math.min(ZOOM_MAX, Number((z + ZOOM_STEP).toFixed(2))));
  }, []);
  const zoomOut = useCallback(() => {
    setZoom(z => Math.max(ZOOM_MIN, Number((z - ZOOM_STEP).toFixed(2))));
  }, []);
  const resetView = useCallback(() => {
    fitToView();
  }, [fitToView]);

  const toggleFullscreen = useCallback(() => {
    setIsFullscreen(fs => !fs);
  }, []);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isFullscreen) {
        setIsFullscreen(false);
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [isFullscreen]);

  useEffect(() => {
    requestAnimationFrame(() => fitToView());
  }, [isFullscreen]);

  const handleSvgPointerDown = useCallback((e: React.PointerEvent<SVGSVGElement>) => {
    if (e.button !== 0) return;
    const svg = e.currentTarget;
    const pid = e.pointerId;
    svg.setPointerCapture(pid);
    draggingRef.current = true;
    setIsDragging(true);
    dragRef.current = {
      x: e.clientX,
      y: e.clientY,
      panX: panRef.current.x,
      panY: panRef.current.y,
    };
    const onMove = (ev: PointerEvent) => {
      if (!draggingRef.current) return;
      const dx = ev.clientX - dragRef.current.x;
      const dy = ev.clientY - dragRef.current.y;
      setPan({
        x: dragRef.current.panX + dx,
        y: dragRef.current.panY + dy,
      });
    };
    let ended = false;
    const finish = () => {
      if (ended) return;
      ended = true;
      draggingRef.current = false;
      setIsDragging(false);
      try {
        svg.releasePointerCapture(pid);
      } catch {
        /* not captured */
      }
      window.removeEventListener('pointermove', onMove);
      window.removeEventListener('pointerup', finish);
      window.removeEventListener('pointercancel', finish);
    };
    window.addEventListener('pointermove', onMove);
    window.addEventListener('pointerup', finish);
    window.addEventListener('pointercancel', finish);
  }, []);

  if (links.length === 0) return null;

  return (
    <section className="cc-ego" aria-label="Connected cluster ego graph">
      <div className="cc-ego-grid">
        <div className="cc-ego-main">
          {children != null && children !== false && !isFullscreen && (
            <div className="cc-ego-summary-slot">{children}</div>
          )}

          <div className={`cc-ego-viewport ${isFullscreen ? 'cc-ego-viewport--fullscreen' : ''}`}>
          <div className="cc-ego-controls">
            <div className="cc-ego-controls-left">
              <span className="cc-ego-controls-label">
                <Layers3 size={16} strokeWidth={2} aria-hidden />
                Connection depth
              </span>
              <input
                type="range"
                className="cc-ego-depth"
                min={1}
                max={3}
                step={1}
                value={graphDepth}
                onChange={e => setGraphDepth(Number(e.target.value))}
                aria-label="Connection depth"
              />
              <span className="cc-ego-depth-badge">Level {graphDepth}</span>
            </div>
            <div className="cc-ego-controls-zoom" role="group" aria-label="Zoom and pan">
              <button type="button" className="cc-ego-zoom-btn" onClick={zoomOut} aria-label="Zoom out">
                <Minus size={16} strokeWidth={2} aria-hidden />
              </button>
              <button type="button" className="cc-ego-zoom-btn" onClick={zoomIn} aria-label="Zoom in">
                <Plus size={16} strokeWidth={2} aria-hidden />
              </button>
              <button type="button" className="cc-ego-zoom-btn cc-ego-zoom-btn--text" onClick={resetView}>
                <RotateCcw size={14} strokeWidth={2} aria-hidden />
                <span>Reset</span>
              </button>
              <button type="button" className="cc-ego-zoom-btn" onClick={toggleFullscreen} aria-label={isFullscreen ? 'Exit fullscreen' : 'Fullscreen'}>
                {isFullscreen ? <Minimize2 size={16} strokeWidth={2} aria-hidden /> : <Maximize2 size={16} strokeWidth={2} aria-hidden />}
              </button>
              <span className="cc-ego-zoom-pill">{Math.round(zoom * 100)}%</span>
              <span className="cc-ego-zoom-hint">
                <Move size={14} strokeWidth={2} aria-hidden />
                Drag to pan
              </span>
            </div>
            <div className="cc-ego-toggles">
              <label className="cc-ego-check">
                <input type="checkbox" checked={showSignals} onChange={e => setShowSignals(e.target.checked)} />
                Show bridge nodes
              </label>
            </div>
          </div>

          {lead != null && lead !== false && !isFullscreen && <div className="cc-ego-lead-slot">{lead}</div>}

          <div className="cc-ego-svg-wrap" role="presentation" ref={svgWrapRef}>
            {isRefreshing && (
              <div className="cc-ego-loading-overlay">
                <div className="cc-ego-loading-spinner" />
                <span>Updating graph…</span>
              </div>
            )}
            <svg
              viewBox={`${VB_VIEW.x} ${VB_VIEW.y} ${VB_VIEW.w} ${VB_VIEW.h}`}
              className={`cc-ego-svg ${isDragging ? 'cc-ego-svg--grabbing' : 'cc-ego-svg--pan'}`}
              role="img"
              aria-label="Ego network graph — drag empty space to pan; use − / + and Reset to zoom"
              onPointerDown={handleSvgPointerDown}
            >
              <defs>
                <filter id="cc-ego-shadow" x="-20%" y="-20%" width="140%" height="140%">
                  <feDropShadow dx="0" dy="2" stdDeviation="3" floodOpacity="0.08" />
                </filter>
              </defs>

              <g transform={`translate(${pan.x} ${pan.y}) scale(${zoom})`}>
              <circle
                className="cc-ego-ring"
                cx={EGO_PT.x}
                cy={EGO_PT.y}
                r={CC_RING_INNER}
                fill="none"
                strokeDasharray="4 6"
              />
              <circle
                className="cc-ego-ring"
                cx={EGO_PT.x}
                cy={EGO_PT.y}
                r={CC_RING_MID}
                fill="none"
                strokeDasharray="4 6"
              />
              {graphDepth >= 2 && (
                <circle
                  className="cc-ego-ring"
                  cx={EGO_PT.x}
                  cy={EGO_PT.y}
                  r={CC_RING_OUTER}
                  fill="none"
                  strokeDasharray="4 6"
                />
              )}

              <text
                x={EGO_PT.x}
                y={VB.h + VB_PAD_Y_BOT - 18}
                textAnchor="middle"
                className="cc-ego-level-label"
              >
                {graphDepth >= 2
                  ? 'Guide rings: L1 inner · L2 expanded orbit'
                  : 'Guide rings: L1 inner · L2 partner orbit'}
              </text>

              {visibleEdges.map((edge, edgeIdx) => {
                const s = findNode(visibleNodes, edge.source);
                const t = findNode(visibleNodes, edge.target);
                if (!s || !t) return null;
                const st = edgeStyle(edge.kind);
                const hit = edge.source === selectedId || edge.target === selectedId;
                const bendSign: 1 | -1 =
                  (edge.source.length + edge.target.length + edgeIdx + edge.kind.length) % 2 === 0 ? 1 : -1;
                const d = curvedEdgePathD(
                  s.x,
                  s.y,
                  t.x,
                  t.y,
                  bendSign,
                  edgePathBendScale(edge.kind),
                );
                const baseOpacity = hit ? 1 : 0.72;
                return (
                  <path
                    key={`${edge.source}-${edge.target}-${edge.kind}-${edgeIdx}`}
                    d={d}
                    fill="none"
                    stroke={st.stroke}
                    strokeWidth={hit ? st.strokeWidth + 1 : st.strokeWidth}
                    strokeDasharray={st.dash === '0' ? undefined : st.dash}
                    opacity={baseOpacity}
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  >
                    <title>{edge.reason}</title>
                  </path>
                );
              })}

              {visibleNodes.map(node => {
                const selected = node.id === selectedId;
                if (node.type === 'fingerprint') {
                  const wl = node.wl;
                  const scoreStr =
                    wl.ml_score != null ? `${(wl.ml_score * 100).toFixed(0)}%` : '—';
                  const fpN = wl.fingerprint_overlap_count ?? 0;
                  const metricStr = `${scoreStr} · ${fpN} shared FP`;
                  const pillPadX = 10;
                  const charW = 6.85;
                  const pillW = Math.min(240, Math.max(120, Math.ceil(metricStr.length * charW) + pillPadX * 2));
                  const pillH = 26;
                  const pillX = node.x - pillW / 2;
                  const pillY = node.y + 20;
                  return (
                    <g
                      key={node.id}
                      className="cc-ego-node-g"
                      onPointerDown={stopPanEvent}
                      onClick={() => onSelect(node.id)}
                      onKeyDown={e => e.key === 'Enter' && onSelect(node.id)}
                      role="button"
                      tabIndex={0}
                    >
                      <title>{`${node.label} — ${metricStr} — ${evidenceLine(wl)}`}</title>
                      <rect
                        x={node.x - 16}
                        y={node.y - 16}
                        width={32}
                        height={32}
                        rx={6}
                        transform={`rotate(45 ${node.x} ${node.y})`}
                        className={`cc-ego-fp ${selected ? 'cc-ego-fp--selected' : ''}`}
                        filter="url(#cc-ego-shadow)"
                      />
                      <text x={node.x} y={node.y + 4} textAnchor="middle" className="cc-ego-fp-text">
                        {node.label}
                      </text>
                      <rect
                        x={pillX}
                        y={pillY}
                        width={pillW}
                        height={pillH}
                        rx={8}
                        className="cc-ego-fp-metric-bg"
                      />
                      <text
                        x={node.x}
                        y={pillY + pillH / 2}
                        textAnchor="middle"
                        dominantBaseline="middle"
                        className="cc-ego-fp-metric"
                      >
                        {metricStr}
                      </text>
                    </g>
                  );
                }
                if (node.type === 'event') {
                  const et = humanizeEvidenceType(node.evidence.evidence_type);
                  const mark = eventMarkerById.get(node.id) ?? '·';
                  const tip =
                    node.eventParticipant != null
                      ? `${et} · record ${node.eventParticipant.toUpperCase()} — details in panel`
                      : `${et} — open side panel for full detail`;
                  const a11y = `Event node ${mark}: ${tip}`;
                  return (
                    <g
                      key={node.id}
                      className="cc-ego-node-g"
                      onPointerDown={stopPanEvent}
                      onClick={() => onSelect(node.id)}
                      onKeyDown={e => e.key === 'Enter' && onSelect(node.id)}
                      role="button"
                      tabIndex={0}
                      aria-label={a11y}
                    >
                      <title>{`${mark} — ${tip}`}</title>
                      <circle
                        cx={node.x}
                        cy={node.y}
                        r={11}
                        className={`cc-ego-ev ${selected ? 'cc-ego-ev--selected' : ''}`}
                      />
                      <text
                        x={node.x}
                        y={node.y}
                        textAnchor="middle"
                        dominantBaseline="central"
                        className={`cc-ego-ev-mark ${mark.length > 1 ? 'cc-ego-ev-mark--narrow' : ''}`}
                      >
                        {mark}
                      </text>
                    </g>
                  );
                }
                const w = node.level === 0 ? 144 : 116;
                const h = node.level === 0 ? 68 : 56;
                return (
                  <g
                    key={node.id}
                    className="cc-ego-node-g"
                    onPointerDown={stopPanEvent}
                    onClick={() => onSelect(node.id)}
                    onKeyDown={e => e.key === 'Enter' && onSelect(node.id)}
                    role="button"
                    tabIndex={0}
                  >
                    <title>{node.clusterId}</title>
                    <rect
                      x={node.x - w / 2}
                      y={node.y - h / 2}
                      width={w}
                      height={h}
                      rx={18}
                      className={`cc-ego-cluster cc-ego-cluster--${node.tone} ${selected ? 'cc-ego-cluster--selected' : ''}`}
                      filter="url(#cc-ego-shadow)"
                    />
                    <text x={node.x} y={node.y - 4} textAnchor="middle" className="cc-ego-cluster-id">
                      {node.level === 0 ? shortId(node.clusterId, 12) : shortId(node.clusterId, 10)}
                    </text>
                    <text x={node.x} y={node.y + 14} textAnchor="middle" className="cc-ego-cluster-sub">
                      {node.label}
                    </text>
                  </g>
                );
              })}

              </g>
            </svg>
          </div>
          </div>{/* cc-ego-viewport */}
        </div>

        <aside className="cc-ego-aside">
          {asideExtra}

          <div className="cc-ego-card">
            <div className="cc-ego-card-title">
              <Network size={16} strokeWidth={2} aria-hidden />
              Selected node
            </div>
            {selectedNode?.type === 'cluster' && (
              <>
                <div className="cc-ego-sel-title-line1">{shortId(selectedNode.clusterId, 22)}</div>
                <div className="cc-ego-sel-title-line2">{selectedNode.label}</div>
                <div className="cc-ego-badges">
                  {selectedNode.confidencePct != null && (
                    <span className="cc-ego-badge">{selectedNode.confidencePct}% confidence</span>
                  )}
                  {selectedNode.tags.map(t => (
                    <span key={t} className="cc-ego-badge cc-ego-badge--tag">
                      {t}
                    </span>
                  ))}
                </div>
              </>
            )}
            {selectedNode?.type === 'fingerprint' && (
              <>
                <div className="cc-ego-sel-title-line1">{selectedNode.label}</div>
                <div className="cc-ego-sel-title-line2">Cluster pair link (bridge)</div>
                <div className="cc-ego-badges">
                  <span className="cc-ego-badge cc-ego-badge--key">
                    <KeyRound size={12} strokeWidth={2} aria-hidden />
                    ML pair score
                  </span>
                  {selectedNode.wl.ml_score != null && (
                    <span className="cc-ego-badge">
                      {(selectedNode.wl.ml_score * 100).toFixed(0)}% model score
                    </span>
                  )}
                  <span className="cc-ego-badge">
                    {selectedNode.wl.fingerprint_overlap_count ?? 0} distinct shared fingerprints
                  </span>
                </div>
                <p className="cc-ego-sel-detail">{evidenceLine(selectedNode.wl)}</p>
              </>
            )}
            {selectedNode?.type === 'event' && (
              <SelectedEventPanel
                identityId={identityId}
                node={selectedNode}
                marker={eventMarkerById.get(selectedNode.id)}
              />
            )}
          </div>
        </aside>
      </div>
    </section>
  );
}
