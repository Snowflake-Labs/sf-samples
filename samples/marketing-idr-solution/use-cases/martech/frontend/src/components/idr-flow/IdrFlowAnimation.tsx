import { useCallback, useEffect, useRef, useState } from 'react';
import rough from 'roughjs';
import type { RoughSVG } from 'roughjs/bin/svg';

import { LIGHT, type Palette } from './palette';
import {
  addText,
  clamp01,
  clearLayer,
  drawArrowhead,
  easeInOutCubic,
  lerp,
  pulse,
  svgEl,
} from './roughScene';
import {
  BEATS,
  FPS,
  TOTAL_FRAMES,
  TOTAL_MS,
  frameToMs,
  msToBeat,
  type BeatId,
} from './timeline';

const W = 1280;
const H = 720;

// ── Layout regions ──────────────────────────────────────────────────────────

const RAIL_X0 = 60;
const RAIL_X1 = 1220;
const RAIL_Y = 38;

const INBOX_X = 30;
const RECORD_W = 240;
const RECORD_H = 56;
const QUEUE_GAP = 22;
const QUEUE_Y0 = 130;

// Engine: horizontally centered in the viewport (W=1280 → center 640)
const ENGINE = { x: 435, y: 130, w: 320, h: 460 };
// Resting position of a record card while it's "inside" the engine — vertical center
const ENGINE_REST = {
  x: ENGINE.x + (ENGINE.w - RECORD_W) / 2,
  y: ENGINE.y + (ENGINE.h - RECORD_H) / 2,
};

const HITL = { x: ENGINE.x, y: ENGINE.y + ENGINE.h + 18, w: ENGINE.w, h: 72 };

const C1_RECT = { x: 880, y: 130, w: 360, h: 218 };
const C2_RECT = { x: 880, y: 364, w: 360, h: 156 };
const C3_RECT = { x: 880, y: 536, w: 360, h: 94 };

const CARD_X_IN_CLUSTER = C1_RECT.x + (C1_RECT.w - RECORD_W) / 2; // 920

const C1_SLOTS = [
  { x: CARD_X_IN_CLUSTER, y: C1_RECT.y + 32 },
  { x: CARD_X_IN_CLUSTER, y: C1_RECT.y + 32 + 62 },
  { x: CARD_X_IN_CLUSTER, y: C1_RECT.y + 32 + 124 },
];
const C2_SLOTS = [
  { x: CARD_X_IN_CLUSTER, y: C2_RECT.y + 32 },
  { x: CARD_X_IN_CLUSTER, y: C2_RECT.y + 32 + 62 },
];
const C3_SLOTS = [
  { x: CARD_X_IN_CLUSTER, y: C3_RECT.y + 32 },
];

// ── Data model ──────────────────────────────────────────────────────────────

type RecordData = {
  id: string;
  name: string;
  email: string;
  phone: string;
  address: string;
};

const RECORDS: RecordData[] = [
  { id: 'R1', name: 'Maria Rodriguez',  email: 'maria.r@gmail.com',    phone: '555-0100', address: '123 Oak St, Austin'   },
  { id: 'R2', name: 'Mariia Rodriguez', email: 'maria.r@gmail.com',    phone: '555-0100', address: '123 Oak St, Austin'   },
  { id: 'R3', name: 'David Chen',       email: 'david.chen@yahoo.com', phone: '555-0200', address: '540 Pine Ave, Seattle' },
  { id: 'R4', name: 'Marie Rodriquez',  email: 'm.rod@work.com',       phone: '555-0100', address: '125 Oak St, Austin'   },
  { id: 'R5', name: 'Dave Chenn',       email: 'dchen@hotmail.com',    phone: '555-0299', address: '540 Pine Ave, Seattle' },
  { id: 'R6', name: 'Sarah Park',       email: 'sarah.park@me.com',    phone: '555-0300', address: '88 Cedar Ln, Boston'   },
];

type ClusterId = 'C1' | 'C2' | 'C3';
type RuleKind = 'NEW' | 'DET' | 'ML' | 'LLM_HITL';

type MatchEvent = {
  recIdx: number;
  beat: BeatId;
  cluster: ClusterId;
  slot: number;
  ruleKind: RuleKind;
  rule?: { id: string; label: string; score?: number };
};

const EVENTS: MatchEvent[] = [
  { recIdx: 0, beat: 'b1', cluster: 'C1', slot: 0, ruleKind: 'NEW' },
  { recIdx: 1, beat: 'b2', cluster: 'C1', slot: 1, ruleKind: 'DET',
    rule: { id: 'MARTECH_R10', label: 'R10 \u00b7 Fuzzy(first) + last + email' } },
  { recIdx: 2, beat: 'b3', cluster: 'C2', slot: 0, ruleKind: 'NEW' },
  { recIdx: 3, beat: 'b4', cluster: 'C1', slot: 2, ruleKind: 'ML',
    rule: { id: 'MARTECH_R16', label: 'R16 \u00b7 ML score 0.90', score: 0.90 } },
  { recIdx: 4, beat: 'b5', cluster: 'C2', slot: 1, ruleKind: 'LLM_HITL',
    rule: { id: 'MARTECH_R17', label: 'R17 \u00b7 LLM verdict: MATCH', score: 0.80 } },
  { recIdx: 5, beat: 'b6', cluster: 'C3', slot: 0, ruleKind: 'NEW' },
];

const BEAT_ORDER: BeatId[] = ['b0','b1','b2','b3','b4','b5','b6','b7'];

// Pills shown to the right of each member card inside its cluster.
// Reflects how the record was matched. R2: DET (R10). R4: ML 90%. R5: ML 80% → LLM → human approved.
type MemberPillKind = 'DET' | 'ML' | 'LLM' | 'HITL';
const MEMBER_PILLS: { [recIdx: number]: { label: string; kind: MemberPillKind }[] } = {
  1: [{ label: 'DET',     kind: 'DET' }],
  3: [{ label: 'ML 90%',  kind: 'ML' }],
  4: [
    { label: 'ML 80%',    kind: 'ML' },
    { label: 'LLM',       kind: 'LLM' },
    { label: '\u2713 Human', kind: 'HITL' },
  ],
};

function eventForBeat(b: BeatId): MatchEvent | null {
  return EVENTS.find((e) => e.beat === b) ?? null;
}

function clusterSlotPos(cluster: ClusterId, slot: number) {
  if (cluster === 'C1') return C1_SLOTS[slot];
  if (cluster === 'C2') return C2_SLOTS[slot];
  return C3_SLOTS[slot];
}

function queueSlotPos(idx: number) {
  return { x: INBOX_X, y: QUEUE_Y0 + idx * (RECORD_H + QUEUE_GAP) };
}

function priorPosition(recIdx: number, currentBeat: BeatId): { x: number; y: number } {
  const curIdx = BEAT_ORDER.indexOf(currentBeat);
  for (const e of EVENTS) {
    if (e.recIdx !== recIdx) continue;
    const eIdx = BEAT_ORDER.indexOf(e.beat);
    if (eIdx < curIdx) return clusterSlotPos(e.cluster, e.slot);
  }
  return queueSlotPos(recIdx);
}

function eventForRec(i: number): MatchEvent | undefined {
  return EVENTS.find((e) => e.recIdx === i);
}

function isFutureRecord(i: number, currentBeat: BeatId): boolean {
  const e = eventForRec(i);
  if (!e) return false;
  return BEAT_ORDER.indexOf(e.beat) > BEAT_ORDER.indexOf(currentBeat);
}

function ruleColor(kind: RuleKind, p: Palette): string {
  switch (kind) {
    case 'NEW': return p.textMuted;
    case 'DET': return p.det;
    case 'ML':  return p.ml;
    case 'LLM_HITL': return p.llm;
  }
}

// ── Rough drawing helpers ───────────────────────────────────────────────────

function rRect(rc: RoughSVG, g: SVGGElement, x: number, y: number, w: number, h: number, opts: any) {
  g.appendChild(rc.rectangle(x, y, w, h, opts));
}
function rLine(rc: RoughSVG, g: SVGGElement, x1: number, y1: number, x2: number, y2: number, opts: any) {
  g.appendChild(rc.line(x1, y1, x2, y2, opts));
}

function drawTimeRail(rc: RoughSVG, g: SVGGElement, p: Palette, ms: number, seed: number) {
  rLine(rc, g, RAIL_X0, RAIL_Y, RAIL_X1, RAIL_Y, {
    stroke: p.textMuted, strokeWidth: 1.6, roughness: 1.6, bowing: 1, seed,
  });
  const ticks: { ms: number; label: string }[] = [
    { ms: 4_000,  label: 't1' },
    { ms: 9_000,  label: 't2' },
    { ms: 21_000, label: 't3' },
  ];
  ticks.forEach((tk, i) => {
    const x = lerp(RAIL_X0, RAIL_X1, tk.ms / TOTAL_MS);
    rLine(rc, g, x, RAIL_Y - 8, x, RAIL_Y + 8, {
      stroke: p.textMuted, strokeWidth: 1.5, roughness: 1.4, seed: seed + 13 + i,
    });
    addText(g, x, RAIL_Y - 14, tk.label, {
      className: 'ifa-rail-label', fill: p.text, anchor: 'middle',
    });
  });
  // Cursor
  const cx = lerp(RAIL_X0, RAIL_X1, clamp01(ms / TOTAL_MS));
  g.appendChild(rc.circle(cx, RAIL_Y, 14, {
    stroke: p.det, strokeWidth: 2.2, roughness: 1.6,
    fill: p.det, fillStyle: 'solid', seed: seed + 31,
  }));
}

function drawEngine(rc: RoughSVG, g: SVGGElement, p: Palette, seed: number) {
  // Snowflake-blue tinted engine background
  rRect(rc, g, ENGINE.x, ENGINE.y, ENGINE.w, ENGINE.h, {
    stroke: '#11567F', strokeWidth: 2.2, roughness: 2.4, bowing: 2.4,
    fill: 'rgba(41, 181, 232, 0.14)', fillStyle: 'solid', seed,
  });
  // Technique palette at bottom of engine
  const techs = [
    { label: 'DET',   color: p.det },
    { label: 'FUZZY', color: p.fuzzy },
    { label: 'ML',    color: p.ml },
    { label: 'LLM',   color: p.llm },
  ];
  const pillW = 60;
  const pillH = 26;
  const py = ENGINE.y + ENGINE.h - 44;
  const totalW = techs.length * pillW + (techs.length - 1) * 8;
  const startX = ENGINE.x + (ENGINE.w - totalW) / 2;
  techs.forEach((tk, i) => {
    const x = startX + i * (pillW + 8);
    rRect(rc, g, x, py, pillW, pillH, {
      stroke: tk.color, strokeWidth: 1.2, roughness: 1.6, bowing: 1.4,
      fill: 'transparent', seed: seed + 21 + i * 7,
    });
    addText(g, x + pillW / 2, py + 18, tk.label, {
      className: 'ifa-engine-pill', fill: tk.color, anchor: 'middle',
    });
  });
}

function drawRecordCard(rc: RoughSVG, g: SVGGElement, p: Palette,
                        x: number, y: number, r: RecordData,
                        opacity: number, emphasized: boolean, seed: number,
                        highlightFirstColor?: string) {
  if (opacity <= 0.001) return;
  const grp = svgEl('g');
  grp.setAttribute('opacity', String(opacity));
  g.appendChild(grp);

  grp.appendChild(rc.rectangle(x, y, RECORD_W, RECORD_H, {
    stroke: p.memberStroke,
    strokeWidth: emphasized ? 2.0 : 1.7,
    roughness: 2.2, bowing: 2.4,
    fill: p.memberFill, fillStyle: 'solid',
    seed,
  }));

  // ID badge
  addText(grp, x + 12, y + 16, r.id, { className: 'ifa-id', fill: p.text });
  // Name (optionally highlighting the first word)
  if (highlightFirstColor) {
    const sp = r.name.indexOf(' ');
    const first = sp >= 0 ? r.name.slice(0, sp) : r.name;
    const rest = sp >= 0 ? r.name.slice(sp) : '';
    addText(grp, x + 36, y + 16, first, {
      className: 'ifa-name ifa-name-hl', fill: highlightFirstColor,
    });
    if (rest) {
      addText(grp, x + 36 + first.length * 7.6, y + 16, rest, {
        className: 'ifa-name', fill: p.text,
      });
    }
  } else {
    addText(grp, x + 36, y + 16, r.name, { className: 'ifa-name', fill: p.text });
  }
  // Email
  addText(grp, x + 12, y + 31, r.email, { className: 'ifa-email', fill: p.text });
  // Phone + address
  addText(grp, x + 12, y + 47, `${r.phone} \u00b7 ${r.address}`, {
    className: 'ifa-attr', fill: p.textMuted,
  });
}

function drawCluster(rc: RoughSVG, g: SVGGElement, p: Palette,
                     rect: { x: number; y: number; w: number; h: number },
                     id: ClusterId, name: string, members: number, seed: number) {
  const merged = members >= 2;
  const stroke = merged ? p.clusterMergedStroke : p.clusterSeparateStroke;
  const fill = merged ? p.clusterMergedFill : p.clusterSeparateFill;
  rRect(rc, g, rect.x, rect.y, rect.w, rect.h, {
    stroke, strokeWidth: 2.4, roughness: 2.6, bowing: 2.6,
    fill, fillStyle: 'hachure', fillWeight: 1.4, hachureGap: 8,
    hachureAngle: merged ? -36 : 42,
    seed,
  });
  addText(g, rect.x + 12, rect.y + 20, id, {
    className: 'ifa-cluster-tag', fill: stroke,
  });
  addText(g, rect.x + 50, rect.y + 20,
    `${name} \u00b7 ${members} member${members === 1 ? '' : 's'}`,
    { className: 'ifa-cluster-sub', fill: p.textMuted });
}

function memberPillColor(kind: MemberPillKind, p: Palette): string {
  switch (kind) {
    case 'DET':  return p.det;
    case 'ML':   return p.ml;
    case 'LLM':  return p.llm;
    case 'HITL': return p.hitl;
  }
}

function drawMemberPills(rc: RoughSVG, g: SVGGElement, p: Palette,
                         cardX: number, cardY: number,
                         pills: { label: string; kind: MemberPillKind }[],
                         seed: number) {
  const pillW = 50;
  const pillH = 14;
  const gap = 3;
  const totalH = pills.length * pillH + (pills.length - 1) * gap;
  const x = cardX + RECORD_W + 6;
  let py = cardY + (RECORD_H - totalH) / 2;
  pills.forEach((pl, i) => {
    const color = memberPillColor(pl.kind, p);
    rRect(rc, g, x, py, pillW, pillH, {
      stroke: color, strokeWidth: 1.2, roughness: 1.4, bowing: 1.4,
      fill: 'transparent', seed: seed + i * 7,
    });
    addText(g, x + pillW / 2, py + pillH - 4, pl.label, {
      className: 'ifa-member-pill', fill: color, anchor: 'middle',
    });
    py += pillH + gap;
  });
}

function drawRuleChip(rc: RoughSVG, g: SVGGElement, p: Palette,
                      kind: RuleKind, lines: string[], opacity: number, seed: number,
                      boxYOverride?: number) {
  if (opacity <= 0.001) return;
  const grp = svgEl('g');
  grp.setAttribute('opacity', String(clamp01(opacity)));
  g.appendChild(grp);
  const color = ruleColor(kind, p);
  const wide = lines.length >= 3;
  const w = lines.length >= 4 ? 300 : (wide ? 288 : 260);
  const h = lines.length === 1 ? 38
          : lines.length === 2 ? 58
          : lines.length === 3 ? 78
          : 110;
  const cx = ENGINE.x + ENGINE.w / 2;
  const boxX = cx - w / 2;
  const boxY = boxYOverride ?? (ENGINE.y + 56);
  grp.appendChild(rc.rectangle(boxX, boxY, w, h, {
    stroke: color, strokeWidth: 2.0, roughness: 2.0, bowing: 2,
    fill: p.labelBg, fillStyle: 'solid', seed,
  }));
  if (lines.length === 1) {
    addText(grp, cx, boxY + 24, lines[0], {
      className: 'ifa-rule-chip', fill: color, anchor: 'middle',
    });
  } else if (lines.length === 2) {
    addText(grp, cx, boxY + 22, lines[0], {
      className: 'ifa-rule-chip', fill: color, anchor: 'middle',
    });
    addText(grp, cx, boxY + 44, lines[1], {
      className: 'ifa-rule-chip-sub', fill: color, anchor: 'middle',
    });
  } else if (lines.length === 3) {
    addText(grp, cx, boxY + 20, lines[0], {
      className: 'ifa-rule-chip', fill: color, anchor: 'middle',
    });
    addText(grp, cx, boxY + 42, lines[1], {
      className: 'ifa-rule-chip-sub', fill: color, anchor: 'middle',
    });
    addText(grp, cx, boxY + 62, lines[2], {
      className: 'ifa-rule-chip-sub', fill: color, anchor: 'middle',
    });
  } else {
    // 4-line layout: title, subtitle, then a name-compare block (R1 / R2 with red 'i' on R2)
    addText(grp, cx, boxY + 22, lines[0], {
      className: 'ifa-rule-chip', fill: color, anchor: 'middle',
    });
    addText(grp, cx, boxY + 44, lines[1], {
      className: 'ifa-rule-chip-sub', fill: color, anchor: 'middle',
    });
    // Name-compare block — render in monospace so the columns align
    const nx = boxX + 56;
    const cw = 7.0; // monospace char width at 11.5px
    addText(grp, nx, boxY + 70, lines[2], {
      className: 'ifa-rule-chip-mono', fill: p.text,
    });
    // lines[3] is rendered specially: "R2 : Mari" + "i" (RED) + "ia Rodriguez"
    const r2 = lines[3];
    const redIdx = r2.indexOf('Mari') + 4; // first 'i' that is the extra one (Mari[i]ia)
    if (redIdx >= 4) {
      const pre = r2.slice(0, redIdx);
      const post = r2.slice(redIdx + 1);
      addText(grp, nx, boxY + 90, pre, {
        className: 'ifa-rule-chip-mono', fill: p.text,
      });
      addText(grp, nx + pre.length * cw, boxY + 90, 'i', {
        className: 'ifa-rule-chip-mono-red', fill: '#dc2626',
      });
      addText(grp, nx + (pre.length + 1) * cw, boxY + 90, post, {
        className: 'ifa-rule-chip-mono', fill: p.text,
      });
    } else {
      addText(grp, nx, boxY + 90, r2, {
        className: 'ifa-rule-chip-mono', fill: p.text,
      });
    }
  }
}

function drawHumanDoodle(rc: RoughSVG, g: SVGGElement, cx: number, cy: number,
                         color: string, seed: number) {
  g.appendChild(rc.circle(cx, cy - 26, 22, {
    stroke: color, strokeWidth: 1.8, roughness: 1.8,
    fill: 'transparent', seed,
  }));
  g.appendChild(rc.line(cx, cy - 14, cx, cy + 18, {
    stroke: color, strokeWidth: 1.8, roughness: 1.6, seed: seed + 1,
  }));
  g.appendChild(rc.line(cx - 16, cy - 2, cx + 16, cy - 2, {
    stroke: color, strokeWidth: 1.8, roughness: 1.6, seed: seed + 2,
  }));
  g.appendChild(rc.line(cx, cy + 18, cx - 12, cy + 36, {
    stroke: color, strokeWidth: 1.8, roughness: 1.6, seed: seed + 3,
  }));
  g.appendChild(rc.line(cx, cy + 18, cx + 12, cy + 36, {
    stroke: color, strokeWidth: 1.8, roughness: 1.6, seed: seed + 4,
  }));
}

function drawHitlPanel(rc: RoughSVG, g: SVGGElement, p: Palette,
                       progress: number, approved: boolean, seed: number) {
  if (progress <= 0.001) return;
  const grp = svgEl('g');
  grp.setAttribute('opacity', String(clamp01(progress)));
  g.appendChild(grp);
  rRect(rc, grp, HITL.x, HITL.y, HITL.w, HITL.h, {
    stroke: p.hitl, strokeWidth: 2.0, roughness: 2.0, bowing: 2.4,
    fill: p.labelBg, fillStyle: 'solid', seed,
  });
  drawHumanDoodle(rc, grp, HITL.x + 40, HITL.y + 56, p.hitl, seed + 11);
  addText(grp, HITL.x + 84, HITL.y + 32, 'Human review', {
    className: 'ifa-hitl-title', fill: p.text,
  });
  addText(grp, HITL.x + 84, HITL.y + 54,
    approved ? 'LLM said MATCH \u2014 reviewer confirmed' : 'LLM said MATCH \u2014 awaiting confirm',
    { className: 'ifa-hitl-sub', fill: p.textMuted });
  if (approved) {
    const pillX = HITL.x + 84;
    const pillY = HITL.y + 70;
    const pillW = 138;
    rRect(rc, grp, pillX, pillY, pillW, 28, {
      stroke: p.hitl, strokeWidth: 1.6, roughness: 1.8, bowing: 2,
      fill: 'rgba(22,163,74,0.16)', fillStyle: 'solid', seed: seed + 17,
    });
    addText(grp, pillX + 16, pillY + 19, '\u2713', {
      className: 'ifa-hitl-check', fill: p.hitl,
    });
    addText(grp, pillX + 36, pillY + 19, 'Approved', {
      className: 'ifa-hitl-pill', fill: p.hitl,
    });
  }
}

function drawArrow(rc: RoughSVG, g: SVGGElement,
                   from: { x: number; y: number }, to: { x: number; y: number },
                   color: string, seed: number) {
  rLine(rc, g, from.x, from.y, to.x, to.y, {
    stroke: color, strokeWidth: 1.8, roughness: 1.4, bowing: 2, seed,
  });
  drawArrowhead(g, from.x, from.y, to.x, to.y, color, 0.85);
}

// ── Frame state ─────────────────────────────────────────────────────────────

type RecordPos = { x: number; y: number; opacity: number; emphasized: boolean };

type FrameState = {
  ms: number;
  beatId: BeatId;
  positions: RecordPos[];
  c1Members: number;
  c2Members: number;
  c3Members: number;
  c1Visible: boolean;
  c2Visible: boolean;
  c3Visible: boolean;
  ruleChip: { kind: RuleKind; lines: string[]; opacity: number } | null;
  secondaryChip: { kind: RuleKind; lines: string[]; opacity: number } | null;
  hitl: { progress: number; approved: boolean } | null;
  arrow: { from: { x: number; y: number }; to: { x: number; y: number }; color: string } | null;
  highlights: (string | null)[];
  seed: number;
};

function buildFrameState(frame: number): FrameState {
  const ms = frameToMs(frame);
  const beat = msToBeat(ms);
  const t = clamp01(beat.t);
  const seed = beat.seed;

  // Default positions: prior (cluster) for processed records, else queue slot.
  // Records whose event hasn't fired yet are rendered at reduced opacity.
  const positions: RecordPos[] = RECORDS.map((_, i) => {
    const p = priorPosition(i, beat.id as BeatId);
    const future = isFutureRecord(i, beat.id as BeatId);
    return { x: p.x, y: p.y, opacity: future ? 0.32 : 1, emphasized: false };
  });

  let c1 = 0, c2 = 0, c3 = 0;
  let c1V = false, c2V = false, c3V = false;
  const highlights: (string | null)[] = RECORDS.map(() => null);
  const curIdx = BEAT_ORDER.indexOf(beat.id as BeatId);
  for (const e of EVENTS) {
    const eIdx = BEAT_ORDER.indexOf(e.beat);
    if (eIdx < curIdx) {
      if (e.cluster === 'C1') { c1++; c1V = true; }
      if (e.cluster === 'C2') { c2++; c2V = true; }
      if (e.cluster === 'C3') { c3++; c3V = true; }
    }
  }

  let ruleChip: FrameState['ruleChip'] = null;
  let secondaryChip: FrameState['secondaryChip'] = null;
  let hitl: FrameState['hitl'] = null;
  let arrow: FrameState['arrow'] = null;

  const evt = eventForBeat(beat.id as BeatId);
  if (evt) {
    const isHitl = evt.ruleKind === 'LLM_HITL';
    const transitEnd = isHitl ? 0.18 : 0.30;
    const engineEnd = isHitl ? 0.85 : 0.65;
    const recIdx = evt.recIdx;
    const start = queueSlotPos(recIdx);
    const target = clusterSlotPos(evt.cluster, evt.slot);

    let phase: 'transit' | 'engine' | 'eject';
    if (t < transitEnd) phase = 'transit';
    else if (t < engineEnd) phase = 'engine';
    else phase = 'eject';

    if (phase === 'transit') {
      const k = easeInOutCubic(t / transitEnd);
      positions[recIdx] = {
        x: lerp(start.x, ENGINE_REST.x, k),
        y: lerp(start.y, ENGINE_REST.y, k),
        opacity: 1,
        emphasized: t > transitEnd * 0.4,
      };
      arrow = {
        from: { x: start.x + RECORD_W + 4, y: start.y + RECORD_H / 2 },
        to:   { x: ENGINE.x - 6,            y: ENGINE_REST.y + RECORD_H / 2 },
        color: LIGHT.textMuted,
      };
    } else if (phase === 'engine') {
      positions[recIdx] = {
        x: ENGINE_REST.x, y: ENGINE_REST.y, opacity: 1, emphasized: true,
      };
      const localT = (t - transitEnd) / (engineEnd - transitEnd);
      if (isHitl) {
        // Always show ML chip (top) describing why we escalated.
        ruleChip = {
          kind: 'ML',
          lines: ['R16 \u00b7 ML score 0.80', 'borderline \u2192 escalate to LLM'],
          opacity: 1,
        };
        if (localT < 0.30) {
          // LLM not yet visible.
        } else if (localT < 0.60) {
          secondaryChip = {
            kind: 'LLM_HITL',
            lines: ['R17 \u00b7 LLM verdict: MATCH', 'awaiting human review'],
            opacity: 1,
          };
          hitl = { progress: clamp01((localT - 0.30) / 0.20), approved: false };
        } else {
          secondaryChip = {
            kind: 'LLM_HITL',
            lines: ['R17 \u00b7 LLM + Human review', 'human approved \u2192 join cluster'],
            opacity: 1,
          };
          hitl = { progress: 1, approved: true };
        }
      } else if (evt.ruleKind === 'NEW') {
        ruleChip = {
          kind: 'NEW',
          lines: ['No rule fires', 'first of its kind \u2192 singleton cluster'],
          opacity: 1,
        };
      } else if (evt.ruleKind === 'DET') {
        ruleChip = {
          kind: 'DET',
          lines: [
            evt.rule!.label,
            'deterministic match \u2192 join cluster',
            'R1 : Maria  Rodriguez',
            'R2 : Mariia Rodriguez',
          ],
          opacity: 1,
        };
      } else if (evt.ruleKind === 'ML') {
        ruleChip = {
          kind: 'ML',
          lines: [evt.rule!.label, 'auto-merge (score \u2265 0.85)'],
          opacity: 1,
        };
      }
    } else {
      const k = easeInOutCubic((t - engineEnd) / (1 - engineEnd));
      positions[recIdx] = {
        x: lerp(ENGINE_REST.x, target.x, k),
        y: lerp(ENGINE_REST.y, target.y, k),
        opacity: 1,
        emphasized: true,
      };
      arrow = {
        from: { x: ENGINE.x + ENGINE.w + 6, y: ENGINE_REST.y + RECORD_H / 2 },
        to:   { x: target.x - 6,             y: target.y + RECORD_H / 2 },
        color: ruleColor(evt.ruleKind, LIGHT),
      };
      // Persist chip / HITL during eject
      if (isHitl) {
        ruleChip = {
          kind: 'ML',
          lines: ['R16 \u00b7 ML score 0.80', 'borderline \u2192 escalate to LLM'],
          opacity: 1,
        };
        secondaryChip = { kind: 'LLM_HITL', lines: ['R17 \u00b7 LLM + Human review', 'human approved'], opacity: 1 };
        hitl = { progress: 1, approved: true };
      } else if (evt.ruleKind === 'DET') {
        ruleChip = {
          kind: 'DET',
          lines: [
            evt.rule!.label,
            'joining',
            'R1 : Maria  Rodriguez',
            'R2 : Mariia Rodriguez',
          ],
          opacity: 1,
        };
      } else if (evt.ruleKind === 'ML') {
        ruleChip = { kind: 'ML', lines: [evt.rule!.label, 'joining'], opacity: 1 };
      } else {
        ruleChip = { kind: 'NEW', lines: ['No rule fires', 'first of its kind \u2192 singleton cluster'], opacity: 1 };
      }
      // Show destination cluster early so the eject lands inside it
      if (evt.cluster === 'C1') c1V = true;
      if (evt.cluster === 'C2') c2V = true;
      if (evt.cluster === 'C3') c3V = true;
      if (k > 0.92) {
        if (evt.cluster === 'C1') c1++;
        if (evt.cluster === 'C2') c2++;
        if (evt.cluster === 'C3') c3++;
      }
    }
  }

  return {
    ms,
    beatId: beat.id as BeatId,
    positions,
    c1Members: c1, c2Members: c2, c3Members: c3,
    c1Visible: c1V || c1 > 0,
    c2Visible: c2V || c2 > 0,
    c3Visible: c3V || c3 > 0,
    ruleChip,
    secondaryChip,
    hitl,
    arrow,
    highlights,
    seed,
  };
}

// ── Frame renderer ──────────────────────────────────────────────────────────

function drawFrame(rc: RoughSVG, layer: SVGGElement, frame: number) {
  clearLayer(layer);
  const p = LIGHT;
  const state = buildFrameState(frame);

  // Stable seeds keep persistent elements (rail, engine, records, clusters)
  // from re-shaping every beat. Beat-seed only the transient overlays.
  drawTimeRail(rc, layer, p, state.ms, 9001);

  // Lane labels (all on same line)
  addText(layer, INBOX_X + RECORD_W / 2, ENGINE.y - 14, 'Incoming records', {
    className: 'ifa-lane-label', fill: p.textMuted, anchor: 'middle',
  });
  addText(layer, ENGINE.x + ENGINE.w / 2, ENGINE.y - 14, 'IDR Engine', {
    className: 'ifa-lane-label', fill: p.textMuted, anchor: 'middle',
  });
  addText(layer, C1_RECT.x + C1_RECT.w / 2, C1_RECT.y - 14, 'Resolved identities', {
    className: 'ifa-lane-label', fill: p.textMuted, anchor: 'middle',
  });

  drawEngine(rc, layer, p, 9100);

  // Clusters (drawn before records so cards sit on top)
  if (state.c1Visible) drawCluster(rc, layer, p, C1_RECT, 'C1', 'Maria Rodriguez', state.c1Members, 9201);
  if (state.c2Visible) drawCluster(rc, layer, p, C2_RECT, 'C2', 'David Chen',      state.c2Members, 9301);
  if (state.c3Visible) drawCluster(rc, layer, p, C3_RECT, 'C3', 'Sarah Park',      state.c3Members, 9401);

  // Active arrow (transit or eject) — beat-seeded since it's transient
  if (state.arrow) {
    drawArrow(rc, layer, state.arrow.from, state.arrow.to, state.arrow.color, state.seed + 1500);
  }

  // Record cards — each record has a stable seed across the whole animation
  state.positions.forEach((pos, i) => {
    drawRecordCard(rc, layer, p, pos.x, pos.y, RECORDS[i], pos.opacity, pos.emphasized,
                   9500 + i * 31, state.highlights[i] ?? undefined);
  });

  // Member pills — show how each settled record was matched (right of card, inside cluster)
  RECORDS.forEach((_, i) => {
    const pills = MEMBER_PILLS[i];
    if (!pills) return;
    const e = EVENTS.find((ev) => ev.recIdx === i);
    if (!e) return;
    const settled =
      (e.cluster === 'C1' && state.c1Members > e.slot) ||
      (e.cluster === 'C2' && state.c2Members > e.slot) ||
      (e.cluster === 'C3' && state.c3Members > e.slot);
    if (!settled) return;
    const slot = clusterSlotPos(e.cluster, e.slot);
    drawMemberPills(rc, layer, p, slot.x, slot.y, pills, 9700 + i * 19);
  });

  // Rule chip(s) — when secondary exists, stack primary above secondary inside the engine
  if (state.ruleChip && state.secondaryChip) {
    const primaryY   = ENGINE.y + 24;
    const secondaryY = ENGINE.y + 24 + 58 + 10; // 2-line chip is 58 tall, 10px gap
    drawRuleChip(rc, layer, p, state.ruleChip.kind, state.ruleChip.lines,
                 state.ruleChip.opacity, state.seed + 1700, primaryY);
    drawRuleChip(rc, layer, p, state.secondaryChip.kind, state.secondaryChip.lines,
                 state.secondaryChip.opacity, state.seed + 1750, secondaryY);
  } else if (state.ruleChip) {
    drawRuleChip(rc, layer, p, state.ruleChip.kind, state.ruleChip.lines,
                 state.ruleChip.opacity, state.seed + 1700);
  }
  if (state.hitl) {
    drawHitlPanel(rc, layer, p, state.hitl.progress, state.hitl.approved, state.seed + 1800);
  }
}

declare global {
  interface Window {
    __idrAnim?: {
      ready: boolean;
      totalFrames: number;
      fps: number;
      width: number;
      height: number;
      gotoFrame: (i: number) => void;
    };
  }
}

export function IdrFlowAnimation() {
  const svgRef = useRef<SVGSVGElement | null>(null);
  const layerRef = useRef<SVGGElement | null>(null);
  const rcRef = useRef<RoughSVG | null>(null);
  const rafRef = useRef<number>(0);
  const startTimeRef = useRef<number>(0);
  const pausedAtRef = useRef<number>(0);

  const [playing, setPlaying] = useState(false);
  const [finished, setFinished] = useState(false);
  const [frame, setFrame] = useState(0);
  const initializedRef = useRef(false);

  const totalDurationMs = (TOTAL_FRAMES / FPS) * 1000;

  const renderAt = useCallback((f: number) => {
    const layer = layerRef.current;
    const rc = rcRef.current;
    if (!layer || !rc) return;
    drawFrame(rc, layer, f);
  }, []);

  useEffect(() => {
    const svg = svgRef.current;
    const layer = layerRef.current;
    if (!svg || !layer) return;

    rcRef.current = rough.svg(svg);

    if (!initializedRef.current) {
      initializedRef.current = true;
      drawFrame(rcRef.current, layer, 0);
    }

    window.__idrAnim = {
      ready: true,
      totalFrames: TOTAL_FRAMES,
      fps: FPS,
      width: W,
      height: H,
      gotoFrame: (i: number) => {
        const clamped = Math.max(0, Math.min(TOTAL_FRAMES - 1, Math.round(i)));
        renderAt(clamped);
      },
    };

    return () => { delete window.__idrAnim; };
  }, [renderAt]);

  useEffect(() => {
    if (!playing) return;

    const recorder = !!(window as any).__idrAnim_capture;
    if (recorder) {
      renderAt(0);
      return;
    }

    setFinished(false);
    startTimeRef.current = performance.now() - pausedAtRef.current;
    let stopped = false;

    const loop = () => {
      if (stopped) return;
      const elapsed = performance.now() - startTimeRef.current;
      if (elapsed >= totalDurationMs) {
        renderAt(TOTAL_FRAMES - 1);
        setFrame(TOTAL_FRAMES - 1);
        pausedAtRef.current = 0;
        setPlaying(false);
        setFinished(true);
        return;
      }
      const f = Math.floor((elapsed / 1000) * FPS);
      renderAt(f);
      setFrame(f);
      rafRef.current = requestAnimationFrame(loop);
    };
    loop();

    return () => {
      stopped = true;
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, [playing, renderAt, totalDurationMs]);

  const handlePlayPause = () => {
    if (playing) {
      pausedAtRef.current = performance.now() - startTimeRef.current;
      setPlaying(false);
    } else {
      // Resuming from end → restart
      if (frame >= TOTAL_FRAMES - 1) {
        pausedAtRef.current = 0;
        setFrame(0);
        setFinished(false);
      }
      setPlaying(true);
    }
  };

  const handleReplay = () => {
    pausedAtRef.current = 0;
    setFrame(0);
    setFinished(false);
    setPlaying(true);
  };

  const handleScrub = (f: number) => {
    const clamped = Math.max(0, Math.min(TOTAL_FRAMES - 1, f));
    setPlaying(false);
    setFinished(clamped >= TOTAL_FRAMES - 1);
    setFrame(clamped);
    pausedAtRef.current = (clamped / FPS) * 1000;
    renderAt(clamped);
  };

  const seconds = (frame / FPS).toFixed(1);
  const totalSeconds = ((TOTAL_FRAMES - 1) / FPS).toFixed(1);

  return (
    <div className="idr-anim-frame">
      <svg
        ref={svgRef}
        className="idr-anim-stage"
        viewBox={`0 0 ${W} ${H}`}
        width={W}
        height={H}
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="IDR record flow animation"
      >
        <g ref={layerRef} />
      </svg>
      <div className="idr-anim-controls">
        {finished ? (
          <button className="idr-btn" onClick={handleReplay} title="Replay">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <path d="M4 10a6 6 0 0 1 10.3-4.2L12 8h6V2l-2.1 2.1A8 8 0 1 0 18 10h-2a6 6 0 0 1-12 0z" fill="currentColor" />
            </svg>
            <span>Replay</span>
          </button>
        ) : (
          <button className="idr-btn" onClick={handlePlayPause} title={playing ? 'Pause' : 'Play'}>
            {playing ? (
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <rect x="4" y="3" width="4" height="14" rx="1" fill="currentColor" />
                <rect x="12" y="3" width="4" height="14" rx="1" fill="currentColor" />
              </svg>
            ) : (
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
                <path d="M5 3.5v13l11-6.5z" fill="currentColor" />
              </svg>
            )}
            <span>{playing ? 'Pause' : 'Play'}</span>
          </button>
        )}
        <button className="idr-btn idr-btn-secondary" onClick={handleReplay} title="Restart">
          <svg width="16" height="16" viewBox="0 0 20 20" fill="none">
            <path d="M4 10a6 6 0 0 1 10.3-4.2L12 8h6V2l-2.1 2.1A8 8 0 1 0 18 10h-2a6 6 0 0 1-12 0z" fill="currentColor" />
          </svg>
        </button>
        <input
          className="idr-scrubber"
          type="range"
          min={0}
          max={TOTAL_FRAMES - 1}
          step={1}
          value={frame}
          onChange={(e) => handleScrub(parseInt(e.target.value, 10))}
          aria-label="Animation timeline scrubber"
        />
        <span className="idr-anim-time">{seconds}s / {totalSeconds}s</span>
      </div>
    </div>
  );
}

void BEATS;
