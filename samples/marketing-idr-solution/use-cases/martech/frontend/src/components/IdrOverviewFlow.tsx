import { memo, useCallback, useState } from 'react';
import { Maximize2, Minimize2 } from 'lucide-react';
import {
  Background,
  BackgroundVariant,
  Controls,
  getNodesBounds,
  Handle,
  MiniMap,
  type Node,
  type Edge,
  MarkerType,
  type NodeProps,
  type OnMoveEnd,
  Position,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type ReactFlowInstance,
} from '@xyflow/react';
import { toPng } from 'html-to-image';
import '@xyflow/react/dist/style.css';

type KvRow = { k: string; v: string };

type SourceNodeData = {
  title: string;
  rows: KvRow[];
  /** Defaults to INPUT; bronze sources use BRONZE */
  badge?: string;
};

type IdrTechniqueKey = 'det' | 'fuzzy' | 'canon' | 'ml' | 'llm';
type IdrTechnique = {
  key: IdrTechniqueKey;
  label: string;
  subtitle: string;
};
type IdrEngineData = {
  techniques: IdrTechnique[];
  featurePills: string[];
};

type IdentityGraphData = Record<string, never>;

type ResolvedEntityData = {
  title: string;
  anchors: string;
  reason: string;
  targetHandleId: string;
};

type CleanRoomData = {
  title: string;
  subtitle: string;
  details: string[];
};

type PartnerData = {
  title: string;
  subtitle: string;
  details: string[];
};

type OutcomeCardData = {
  icon: string;
  title: string;
  badge: string;
  items: string[];
  colorKey: string;
};

const SourceNode = memo(function SourceNode({ data }: NodeProps<Node<SourceNodeData>>) {
  return (
    <div className="hiw2-card hiw2-card--source">
      <Handle type="source" position={Position.Right} className="hiw2-handle" />
      <div className="hiw2-card__head">
        <span className="hiw2-card__title">{data.title}</span>
        <span className="hiw2-pill hiw2-pill--input">{data.badge ?? 'INPUT'}</span>
      </div>
      <dl className="hiw2-kv">
        {data.rows.map(row => (
          <div key={row.k} className="hiw2-kv__row">
            <dt>{row.k}</dt>
            <dd className="mono">{row.v}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
});

const IdrEngineNode = memo(function IdrEngineNode({ data }: NodeProps<Node<IdrEngineData>>) {
  return (
    <div className="hiw2-engine" style={{ height: 640, boxSizing: 'border-box' }}>
      <Handle type="target" position={Position.Left} id="in-0" className="hiw2-handle hiw2-handle--t" />
      <Handle type="target" position={Position.Left} id="in-1" className="hiw2-handle hiw2-handle--m1" />
      <Handle type="target" position={Position.Left} id="in-2" className="hiw2-handle hiw2-handle--m2" />
      <Handle type="target" position={Position.Left} id="in-3" className="hiw2-handle hiw2-handle--b" />
      <Handle type="source" position={Position.Right} id="out" className="hiw2-handle" />
      <div className="hiw2-engine__boundary-label">IDR Engine</div>
      <div className="hiw2-engine__inner">
        <div className="hiw2-engine__sub">Single logical unit · individual resolution</div>
        <div className="hiw2-tech-stack">
          {data.techniques.map(t => (
            <div key={t.key} className="hiw2-tech-row">
              <span className={`hiw2-tech-row__pill hiw2-pill hiw2-pill--rule-${t.key}`}>{t.key.toUpperCase()}</span>
              <div className="hiw2-tech-row__body">
                <div className="hiw2-tech-row__name">{t.label}</div>
                <div className="hiw2-tech-row__subtitle">{t.subtitle}</div>
              </div>
            </div>
          ))}
        </div>
        <div className="hiw2-pill-row">
          {data.featurePills.map(p => (
            <span key={p} className="hiw2-pill hiw2-pill--feature">
              {p}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
});

const IdentityGraphNode = memo(function IdentityGraphNode(_props: NodeProps<Node<IdentityGraphData>>) {
  return (
    <div className="hiw2-card hiw2-card--graph">
      <Handle type="target" position={Position.Left} className="hiw2-handle" />
      <Handle type="source" position={Position.Top} id="out-top" className="hiw2-handle hiw2-handle--ig-out-top" />
      <Handle type="source" position={Position.Bottom} id="out-bot" className="hiw2-handle hiw2-handle--ig-out-bot" />
      <div className="hiw2-card__head hiw2-card__head--simple">
        <span className="hiw2-card__title">Identity graph</span>
      </div>
      <p className="hiw2-card__lede">Unified cluster view — person, IDs &amp; household links</p>
      <div className="hiw2-graph-viz" aria-hidden>
        <svg viewBox="0 0 220 140" className="hiw2-graph-viz__svg">
          <line x1="110" y1="70" x2="52" y2="28" stroke="#0284c7" strokeOpacity="0.5" strokeWidth="1.75" />
          <line x1="110" y1="70" x2="48" y2="112" stroke="#0284c7" strokeOpacity="0.5" strokeWidth="1.75" />
          <line x1="110" y1="70" x2="178" y2="36" stroke="#0284c7" strokeOpacity="0.5" strokeWidth="1.75" />
          <line x1="110" y1="70" x2="182" y2="104" stroke="#0284c7" strokeOpacity="0.5" strokeWidth="1.75" />
          <circle cx="110" cy="70" r="22" fill="#e0f2fe" stroke="#0284c7" strokeWidth="2" />
          <text x="110" y="76" textAnchor="middle" fill="#0f172a" fontSize="15" fontWeight="800">
            P
          </text>
          <circle cx="52" cy="28" r="14" fill="#ffffff" stroke="#5ba3d9" strokeWidth="1.5" />
          <text x="52" y="33" textAnchor="middle" fill="#0f172a" fontSize="11" fontWeight="800">
            EM
          </text>
          <circle cx="48" cy="112" r="14" fill="#ffffff" stroke="#5ba3d9" strokeWidth="1.5" />
          <text x="48" y="117" textAnchor="middle" fill="#0f172a" fontSize="11" fontWeight="800">
            GR
          </text>
          <circle cx="178" cy="36" r="14" fill="#ffffff" stroke="#5ba3d9" strokeWidth="1.5" />
          <text x="178" y="41" textAnchor="middle" fill="#0f172a" fontSize="11" fontWeight="800">
            HH
          </text>
          <circle cx="182" cy="104" r="14" fill="#ffffff" stroke="#5ba3d9" strokeWidth="1.5" />
          <text x="182" y="109" textAnchor="middle" fill="#0f172a" fontSize="11" fontWeight="800">
            PP
          </text>
        </svg>
      </div>
    </div>
  );
});

const ResolvedEntityNode = memo(function ResolvedEntityNode({ data }: NodeProps<Node<ResolvedEntityData>>) {
  return (
    <div className="hiw2-card hiw2-card--resolved">
      <Handle
        type="target"
        position={Position.Left}
        id={data.targetHandleId}
        className={`hiw2-handle hiw2-handle--res-${data.targetHandleId}`}
      />
      <Handle type="source" position={Position.Right} id="res-out" className="hiw2-handle" />
      <div className="hiw2-card__head">
        <span className="hiw2-card__title">{data.title}</span>
        <span className="hiw2-pill hiw2-pill--resolved">RESOLVED ENTITY</span>
      </div>
      <dl className="hiw2-kv hiw2-kv--tight">
        <div className="hiw2-kv__row">
          <dt>ANCHORS</dt>
          <dd>{data.anchors}</dd>
        </div>
        <div className="hiw2-kv__row">
          <dt>REASON</dt>
          <dd>{data.reason}</dd>
        </div>
      </dl>
    </div>
  );
});

const CleanRoomNode = memo(function CleanRoomNode({ data }: NodeProps<Node<CleanRoomData>>) {
  return (
    <div className="hiw2-engine hiw2-engine--cleanroom">
      <Handle type="target" position={Position.Left} id="cr-in-top" className="hiw2-handle hiw2-handle--cr-in-top" />
      <Handle type="target" position={Position.Left} id="cr-in-bot" className="hiw2-handle hiw2-handle--cr-in-bot" />
      <Handle type="target" position={Position.Top} id="cr-from-partner" className="hiw2-handle" />
      <Handle type="source" position={Position.Right} id="cr-to-outcomes" className="hiw2-handle" />
      <div className="hiw2-engine__boundary-label">{data.title}</div>
      <div className="hiw2-engine__inner">
        <div className="hiw2-pass">
          <p className="hiw2-card__lede" style={{ marginBottom: 8 }}>{data.subtitle}</p>
          <ul className="hiw2-cr-list">
            {data.details.map(d => (
              <li key={d}>{d}</li>
            ))}
          </ul>
        </div>
      </div>
    </div>
  );
});

const PartnerNode = memo(function PartnerNode({ data }: NodeProps<Node<PartnerData>>) {
  return (
    <div className="hiw2-card hiw2-card--partner">
      <Handle type="source" position={Position.Bottom} id="partner-out" className="hiw2-handle" />
      <div className="hiw2-card__head">
        <span className="hiw2-card__title">{data.title}</span>
        <span className="hiw2-pill hiw2-pill--partner">PARTNER</span>
      </div>
      <p className="hiw2-card__lede">{data.subtitle}</p>
      <ul className="hiw2-cr-list">
        {data.details.map(d => (
          <li key={d}>{d}</li>
        ))}
      </ul>
    </div>
  );
});

const OutcomeCardNode = memo(function OutcomeCardNode({ data }: NodeProps<Node<OutcomeCardData>>) {
  return (
    <div className={`hiw2-card hiw2-outcome-card hiw2-outcome-card--${data.colorKey}`}>
      <Handle type="target" position={Position.Left} id="outcome-in" className="hiw2-handle" />
      <div className="hiw2-outcome-card__head">
        <span className="hiw2-outcome-card__title">{data.title}</span>
      </div>
      <ul className="hiw2-outcome-card__list">
        {data.items.map(item => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </div>
  );
});

const nodeTypes = {
  hiw2Source: SourceNode,
  hiw2Engine: IdrEngineNode,
  hiw2Identity: IdentityGraphNode,
  hiw2Resolved: ResolvedEntityNode,
  hiw2CleanRoom: CleanRoomNode,
  hiw2Partner: PartnerNode,
  hiw2OutcomeCard: OutcomeCardNode,
};

/** Approximate rendered heights + stride (keep in sync with `.hiw2-card` / `.hiw2-card--resolved` CSS). */
const SRC_GAP = 60;
const SRC_CARD_H = 233;
const RESOLVED_CARD_H = 196;
/** Nudge the whole source column to use more vertical canvas (less empty top/bottom). */
const SRC_BLOCK_Y = -60;

const SRC_ROW_STRIDE = SRC_CARD_H + SRC_GAP;
/** Y positions for the 4 source cards (top → bottom): POS, Loyalty, Shopify, Web. */
const SRC_POS_Y = SRC_BLOCK_Y;
const SRC_LOYALTY_Y = SRC_BLOCK_Y + SRC_ROW_STRIDE;
const SRC_SHOPIFY_Y = SRC_BLOCK_Y + SRC_ROW_STRIDE * 2;
const SRC_WEB_Y = SRC_BLOCK_Y + SRC_ROW_STRIDE * 3;
/** Vertical band of the main column (top of POS card → bottom of Web card); identity graph centers on this. */
const DIAGRAM_TOP = SRC_POS_Y;
const DIAGRAM_BOTTOM = SRC_WEB_Y + SRC_CARD_H;
const DIAGRAM_MID_Y = DIAGRAM_TOP + (DIAGRAM_BOTTOM - DIAGRAM_TOP) / 2;
/** Shared horizontal axis (px): engine mid handles, identity left handle, engine `out` — all vertically centered on nodes. */
const ENGINE_AXIS_Y = DIAGRAM_MID_Y;
/** Approximate node heights (keep in sync with rendered cards / engine). */
const ENGINE_H_EST = 640;
const IDENTITY_GRAPH_H_LAYOUT = 296;
const IDENTITY_GRAPH_Y_NUDGE = 0;

const RESOLVED_PERSON_Y = DIAGRAM_TOP;
const RESOLVED_HOUSEHOLD_Y = DIAGRAM_BOTTOM - RESOLVED_CARD_H;
const IDR_ENGINE_Y = Math.round(ENGINE_AXIS_Y - ENGINE_H_EST / 2);
const IDENTITY_GRAPH_Y = Math.round(ENGINE_AXIS_Y - IDENTITY_GRAPH_H_LAYOUT / 2 + IDENTITY_GRAPH_Y_NUDGE);
const CLEAN_ROOM_H_EST = 310;
const PARTNER_H_EST = 158;
const PARTNER_GAP = 70;
const CLEAN_ROOM_Y = Math.round(ENGINE_AXIS_Y - CLEAN_ROOM_H_EST / 2);
const CLEAN_ROOM_X = 1920;
const CLEAN_ROOM_W = 274;
const PARTNER_X = Math.round(CLEAN_ROOM_X + CLEAN_ROOM_W / 2 - 260 / 2);
const PARTNER_Y = CLEAN_ROOM_Y - PARTNER_H_EST - PARTNER_GAP;
const OUTCOME_CARD_H = 110;
const OUTCOME_CARD_GAP = 14;
const OUTCOMES_STACK_H = OUTCOME_CARD_H * 4 + OUTCOME_CARD_GAP * 3;
const OUTCOMES_X = CLEAN_ROOM_X + CLEAN_ROOM_W + 90;
const OUTCOMES_START_Y = Math.round(ENGINE_AXIS_Y - OUTCOMES_STACK_H / 2);

const initialNodes: Node[] = [
  {
    id: 'src-pos',
    type: 'hiw2Source',
    position: { x: 0, y: SRC_POS_Y },
    data: {
      badge: 'SOURCE',
      title: 'POS_TRANSACTION_RAW',
      rows: [
        { k: 'TXN_ID', v: 'PK · in-store transaction grain' },
        { k: 'RAW_PAYLOAD', v: 'EMAIL · PHONE · LOYALTY_ID · NAME · ADDRESS' },
        { k: 'TS · TOTAL', v: 'Receipt timestamp · basket total' },
      ],
    },
  },
  {
    id: 'src-loyalty',
    type: 'hiw2Source',
    position: { x: 0, y: SRC_LOYALTY_Y },
    data: {
      badge: 'SOURCE',
      title: 'LOYALTY_MEMBER_RAW',
      rows: [
        { k: 'MEMBER_ID', v: 'PK · canonical first-party customer row' },
        { k: 'RAW_PAYLOAD', v: 'EMAIL · HEM · PHONE · NAME · ADDRESS' },
        { k: 'TIER · POINTS', v: 'Program tier + balance' },
      ],
    },
  },
  {
    id: 'src-shopify',
    type: 'hiw2Source',
    position: { x: 0, y: SRC_SHOPIFY_Y },
    data: {
      badge: 'SOURCE',
      title: 'SHOPIFY_ORDER_RAW',
      rows: [
        { k: 'ORDER_ID', v: 'PK · online order grain' },
        { k: 'RAW_PAYLOAD', v: 'EMAIL · PHONE · NAME · BILLING/SHIPPING ADDR' },
        { k: 'TOTAL_PRICE', v: 'Order amount + currency' },
      ],
    },
  },
  {
    id: 'src-web',
    type: 'hiw2Source',
    position: { x: 0, y: SRC_WEB_Y },
    data: {
      badge: 'SOURCE',
      title: 'WEB_CLICKSTREAM_RAW',
      rows: [
        { k: 'EVENT_ID', v: 'PK · session/event grain' },
        { k: 'RAW_PAYLOAD', v: 'DEVICE_ID · COOKIE · UID2 · RAMPID · EMAIL?' },
        { k: 'LOGGED_IN_MEMBER_ID', v: 'Loyalty link when authenticated' },
      ],
    },
  },
  {
    id: 'idr-engine',
    type: 'hiw2Engine',
    position: { x: 620, y: IDR_ENGINE_Y },
    data: {
      techniques: [
        {
          key: 'det',
          label: 'Deterministic anchors',
          subtitle: 'Exact match on email, HEM, loyalty #, phone, device IDs, UID2, RampID',
        },
        {
          key: 'fuzzy',
          label: 'Fuzzy name & address',
          subtitle: 'Jaro-Winkler ≥ 0.92 with email / phone / postal anchors',
        },
        {
          key: 'canon',
          label: 'Canonical / nickname',
          subtitle: 'Bob ↔ Robert canonicalization on first names',
        },
        {
          key: 'ml',
          label: 'ML scorer',
          subtitle: 'LightGBM-style classifier on engineered features · score ≥ 0.85',
        },
        {
          key: 'llm',
          label: 'LLM adjudicator',
          subtitle: 'Cortex AI MATCH / NO_MATCH on borderline band [0.55, 0.85)',
        },
      ],
      featurePills: ['NAME-FUZZY', 'ADDRESS', 'CHANNEL-OVERLAP', 'RECENCY', 'CROSS-SOURCE'],
    },
  },
  {
    id: 'identity-graph',
    type: 'hiw2Identity',
    position: { x: 1160, y: IDENTITY_GRAPH_Y },
    data: {},
  },
  {
    id: 'resolved-person',
    type: 'hiw2Resolved',
    position: { x: 1450, y: RESOLVED_PERSON_Y },
    data: {
      title: 'Resolved Customer A',
      anchors: 'Email + Loyalty # + Device ID',
      reason: 'R01 + R03 + R04 — deterministic anchors',
      targetHandleId: 'person',
    },
  },
  {
    id: 'resolved-household',
    type: 'hiw2Resolved',
    position: { x: 1450, y: RESOLVED_HOUSEHOLD_Y },
    data: {
      title: 'Resolved Household 11',
      anchors: 'Same surname + postal code + street',
      reason: 'R14 (full address) + fuzzy / ML reinforcement',
      targetHandleId: 'household',
    },
  },
  {
    id: 'clean-room',
    type: 'hiw2CleanRoom',
    position: { x: CLEAN_ROOM_X, y: CLEAN_ROOM_Y },
    data: {
      title: 'Snowflake Data Clean Room',
      subtitle: 'Privacy-safe overlap with retail media partners',
      details: [
        'PII never leaves account boundary',
        'Differential privacy on aggregations',
        'Approved query templates only',
        'Audit log for every computation',
        'Support for SQL, custom functions, ML models',
      ],
    },
  },
  {
    id: 'partner',
    type: 'hiw2Partner',
    position: { x: PARTNER_X, y: PARTNER_Y },
    data: {
      title: 'Retail Media / DSP Partner',
      subtitle: 'Audience activation & closed-loop measurement',
      details: [
        'Receive match rates (no raw PII)',
        'Activate segments via clean room',
        'Lift studies and conversion attribution',
      ],
    },
  },
  {
    id: 'outcome-enrich',
    type: 'hiw2OutcomeCard',
    position: { x: OUTCOMES_X, y: OUTCOMES_START_Y },
    data: { icon: '\uD83D\uDD17', title: 'Loyalty + Web Stitch', badge: 'ENRICH', items: ['Stitch anonymous web sessions to loyalty profiles', 'Cross-channel identifier enrichment'], colorKey: 'enrich' },
  },
  {
    id: 'outcome-plan',
    type: 'hiw2OutcomeCard',
    position: { x: OUTCOMES_X, y: OUTCOMES_START_Y + OUTCOME_CARD_H + OUTCOME_CARD_GAP },
    data: { icon: '\uD83D\uDCCB', title: 'CDP Audience Planning', badge: 'PLAN', items: ['Deduplicated reach across POS / Web / Shopify'], colorKey: 'plan' },
  },
  {
    id: 'outcome-activate',
    type: 'hiw2OutcomeCard',
    position: { x: OUTCOMES_X, y: OUTCOMES_START_Y + (OUTCOME_CARD_H + OUTCOME_CARD_GAP) * 2 },
    data: { icon: '\uD83C\uDFAF', title: 'Personalize / RM Activation', badge: 'ACTIVATE', items: ['Onsite & email personalization', 'Retail media DSP segments'], colorKey: 'activate' },
  },
  {
    id: 'outcome-measure',
    type: 'hiw2OutcomeCard',
    position: { x: OUTCOMES_X, y: OUTCOMES_START_Y + (OUTCOME_CARD_H + OUTCOME_CARD_GAP) * 3 },
    data: { icon: '\uD83D\uDCCA', title: 'Closed-Loop Attribution', badge: 'MEASURE', items: ['POS-attributed online ads', 'Loyalty incremental lift'], colorKey: 'measure' },
  },
];

const arrowEnd = {
  type: MarkerType.ArrowClosed,
  width: 14,
  height: 14,
  color: '#0284c7',
};

const smoothPath = { borderRadius: 10 };

const initialEdges = [
  {
    id: 'e-pos',
    source: 'src-pos',
    target: 'idr-engine',
    targetHandle: 'in-0',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-loyalty',
    source: 'src-loyalty',
    target: 'idr-engine',
    targetHandle: 'in-1',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-shopify',
    source: 'src-shopify',
    target: 'idr-engine',
    targetHandle: 'in-2',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-web',
    source: 'src-web',
    target: 'idr-engine',
    targetHandle: 'in-3',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-eng-ig',
    type: 'straight',
    source: 'idr-engine',
    sourceHandle: 'out',
    target: 'identity-graph',
    markerEnd: arrowEnd,
  },
  {
    id: 'e-ig-p',
    source: 'identity-graph',
    sourceHandle: 'out-top',
    target: 'resolved-person',
    targetHandle: 'person',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-ig-hh',
    source: 'identity-graph',
    sourceHandle: 'out-bot',
    target: 'resolved-household',
    targetHandle: 'household',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
  },
  {
    id: 'e-res-p-cr',
    source: 'resolved-person',
    target: 'clean-room',
    targetHandle: 'cr-in-top',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-res-hh-cr',
    source: 'resolved-household',
    target: 'clean-room',
    targetHandle: 'cr-in-bot',
    pathOptions: smoothPath,
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-partner-cr',
    type: 'straight',
    source: 'partner',
    sourceHandle: 'partner-out',
    target: 'clean-room',
    targetHandle: 'cr-from-partner',
    label: 'Privacy-safe data join',
    labelStyle: { fontSize: 11, fill: '#888', fontWeight: 500 },
    labelBgStyle: { fill: '#ffffff', fillOpacity: 1 },
    labelBgPadding: [6, 4] as [number, number],
    labelBgBorderRadius: 4,
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-cr-enrich',
    type: 'smoothstep',
    source: 'clean-room',
    sourceHandle: 'cr-to-outcomes',
    target: 'outcome-enrich',
    targetHandle: 'outcome-in',
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-cr-plan',
    type: 'smoothstep',
    source: 'clean-room',
    sourceHandle: 'cr-to-outcomes',
    target: 'outcome-plan',
    targetHandle: 'outcome-in',
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-cr-activate',
    type: 'smoothstep',
    source: 'clean-room',
    sourceHandle: 'cr-to-outcomes',
    target: 'outcome-activate',
    targetHandle: 'outcome-in',
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
  {
    id: 'e-cr-measure',
    type: 'smoothstep',
    source: 'clean-room',
    sourceHandle: 'cr-to-outcomes',
    target: 'outcome-measure',
    targetHandle: 'outcome-in',
    markerEnd: arrowEnd,
    style: { stroke: '#0284c7', strokeWidth: 2 },
  },
] as unknown as Edge[];

function IdrOverviewFlowInner({
  isFullscreen,
  onToggleFullscreen,
}: {
  isFullscreen: boolean;
  onToggleFullscreen: () => void;
}) {
  const [nodes, , onNodesChange] = useNodesState(initialNodes);
  const [edges, , onEdgesChange] = useEdgesState(initialEdges);
  const [zoom, setZoom] = useState(0.75);

  const onInit = useCallback((instance: ReactFlowInstance) => {
    window.requestAnimationFrame(() => {
      instance.fitView({ padding: 0.01, maxZoom: 0.75, minZoom: 0.75, duration: 0 });
    });
    setTimeout(() => {
      const vp = instance.getViewport();
      instance.setViewport({ x: 20, y: vp.y, zoom: 0.75 }, { duration: 0 });
    }, 150);
  }, []);

  const onMoveEnd: OnMoveEnd = useCallback((_event, viewport) => {
    setZoom(viewport.zoom);
  }, []);

  const { getNodes } = useReactFlow();

  const exportImage = useCallback(() => {
    const viewport = document.querySelector('.hiw2-react-flow .react-flow__viewport') as HTMLElement | null;
    if (!viewport) return;

    const nodes = getNodes();
    const bounds = getNodesBounds(nodes);
    const scale = 2;
    const padding = 50;
    const imageWidth = Math.ceil((bounds.width + padding * 2) * scale);
    const imageHeight = Math.ceil((bounds.height + padding * 2) * scale);
    const tx = (-bounds.x + padding) * scale;
    const ty = (-bounds.y + padding) * scale;

    // Fix edge SVGs: set explicit dimensions so they survive cloning
    const edgeSvgs = viewport.querySelectorAll<SVGSVGElement>('svg.react-flow__edges');
    const savedSvgStyles: string[] = [];
    edgeSvgs.forEach((svg) => {
      savedSvgStyles.push(svg.style.cssText);
      svg.style.width = `${bounds.width + padding * 2}px`;
      svg.style.height = `${bounds.height + padding * 2}px`;
      svg.style.position = 'absolute';
    });

    // Fix marker refs for html-to-image
    const base = window.location.href.replace(/#.*$/, '');
    const markerEls = viewport.querySelectorAll('[marker-end], [marker-start]');
    const savedMarkers: { el: Element; attr: string; val: string }[] = [];
    markerEls.forEach((el) => {
      for (const attr of ['marker-end', 'marker-start']) {
        const val = el.getAttribute(attr);
        if (val?.startsWith('url(#')) {
          savedMarkers.push({ el, attr, val });
          el.setAttribute(attr, val.replace('url(#', `url(${base}#`));
        }
      }
    });

    toPng(viewport, {
      width: imageWidth,
      height: imageHeight,
      backgroundColor: '#ffffff',
      pixelRatio: 1,
      filter: (node: Element) => {
        const cls = (node as HTMLElement).classList;
        if (!cls) return true;
        return !cls.contains('react-flow__minimap') &&
               !cls.contains('react-flow__controls') &&
               !cls.contains('react-flow__background');
      },
      style: {
        width: `${imageWidth}px`,
        height: `${imageHeight}px`,
        transform: `translate(${tx}px, ${ty}px) scale(${scale})`,
      },
    }).then((dataUrl) => {
      restore();
      const link = document.createElement('a');
      link.download = 'martech-idr-how-it-works.png';
      link.href = dataUrl;
      link.click();
    }).catch(() => restore());

    function restore() {
      edgeSvgs.forEach((svg, i) => { svg.style.cssText = savedSvgStyles[i]; });
      savedMarkers.forEach(({ el, attr, val }) => el.setAttribute(attr, val));
    }
  }, [getNodes]);

  return (
    <>
    <div className="hiw2-toolbar">
      <span className="hiw2-zoom-label">{Math.round(zoom * 100)}%</span>
      <button type="button" className="hiw2-export-btn" onClick={exportImage}>Export PNG</button>
      <button
        type="button"
        className="hiw2-export-btn hiw2-expand-btn"
        onClick={onToggleFullscreen}
        aria-label={isFullscreen ? 'Exit fullscreen' : 'Expand to fullscreen'}
      >
        {isFullscreen ? <Minimize2 size={14} strokeWidth={1.5} /> : <Maximize2 size={14} strokeWidth={1.5} />}
        <span>{isFullscreen ? 'Exit' : 'Expand'}</span>
      </button>
    </div>
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      defaultEdgeOptions={{
        type: 'smoothstep',
        style: { stroke: '#0284c7', strokeWidth: 2 },
      }}
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      panOnDrag
      zoomOnScroll
      panOnScroll={false}
      zoomOnPinch
      zoomOnDoubleClick
      selectionOnDrag={false}
      preventScrolling
      minZoom={0.28}
      maxZoom={1.68}
      onInit={onInit}
      onMoveEnd={onMoveEnd}
      className="hiw2-react-flow"
    >
      <Controls showInteractive={false} />
      <MiniMap
        nodeStrokeWidth={3}
        pannable
        zoomable
        nodeColor={() => '#0284c7'}
        maskColor="rgba(2,132,199,0.08)"
        className="hiw2-minimap"
      />
      <Background gap={18} size={1} variant={BackgroundVariant.Dots} color="var(--border)" />
    </ReactFlow>
    </>
  );
}

export function IdrOverviewFlow() {
  const [isFullscreen, setIsFullscreen] = useState(false);
  return (
    <div className={`hiw2-flow-wrap ${isFullscreen ? 'hiw2-flow-wrap--fullscreen' : ''}`}>
      <ReactFlowProvider>
        <IdrOverviewFlowInner
          isFullscreen={isFullscreen}
          onToggleFullscreen={() => setIsFullscreen(v => !v)}
        />
      </ReactFlowProvider>
    </div>
  );
}
