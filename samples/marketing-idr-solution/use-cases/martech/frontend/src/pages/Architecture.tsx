import { useState, useEffect, useRef, type ReactNode } from 'react';
import { GitBranch, Maximize2, Minimize2, RefreshCw, Sparkles, Grid3x3, Cloud, Gauge } from 'lucide-react';
import mermaid from 'mermaid';

const erDiagram = `erDiagram
    SSP_BID_REQUEST_RAW {
        TEXT AUCTION_ID PK
        VARIANT RAW_PAYLOAD
        TIMESTAMP_NTZ EVENT_TS
        TEXT SCENARIO
    }
    TAPAD_GRAPH_RAW {
        TEXT RECORD_ID PK
        TEXT TAPAD_ID
        TEXT PERSON_ID
        TEXT ID_TYPE
        TEXT ID_VALUE
        FLOAT CONFIDENCE
    }
    EXPERIAN_GRAPH_RAW {
        TEXT RECORD_ID PK
        TEXT EXPERIAN_ID
        TEXT PERSON_ID
        TEXT ID_TYPE
        TEXT ID_VALUE
        FLOAT CONFIDENCE
    }
    SSP_IMPRESSION_LOG_RAW {
        TEXT IMPRESSION_ID PK
        TEXT AUCTION_ID FK
        TEXT DSP_ID
        FLOAT CLEARING_PRICE
    }
    STD_SSP_BID_REQUEST_RAW {
        TEXT AUCTION_ID PK
        TEXT HEM
        TEXT UID2
        TEXT RAMPID
        TEXT PPID
        TEXT DEVICE_ID
        TEXT COOKIE
        TEXT DEVICE_FINGERPRINT
        BOOLEAN IDR_PROCESSED
    }
    STD_TAPAD_GRAPH_RAW {
        TEXT RECORD_ID PK
        TEXT TAPAD_ID
        TEXT PERSON_ID
        TEXT ID_VALUE_STD
        BOOLEAN IDR_PROCESSED
    }
    STD_EXPERIAN_GRAPH_RAW {
        TEXT RECORD_ID PK
        TEXT EXPERIAN_ID
        TEXT PERSON_ID
        TEXT ID_VALUE_STD
        TEXT UID2
        TEXT HEM
        BOOLEAN IDR_PROCESSED
    }
    IDR_CORE_ENTITY_IDENTIFIERS {
        TEXT IDENTIFIER_ID PK
        TEXT IDENTIFIER_TYPE
        TEXT IDENTIFIER_VALUE_NORMALIZED
        BOOLEAN IS_ACTIVE
    }
    IDR_CORE_IDENTIFIER_LINK {
        TEXT LINK_ID PK
        TEXT SOURCE_RECORD_ID FK
        TEXT SOURCE_TYPE
        TEXT IDENTIFIER_ID FK
        BOOLEAN IS_ACTIVE
    }
    IDR_CORE_MATCH_RESULTS {
        TEXT NEW_SOURCE_RECORD_ID PK
        TEXT MATCHED_SOURCE_RECORD_ID FK
        TEXT RULE_NAME
        FLOAT MATCH_SCORE
        BOOLEAN IS_ACTIVE
    }
    IDR_CORE_MATCH_LOG {
        TEXT MATCH_ID PK
        ARRAY SOURCE_RECORD_IDS
        TEXT DECISION
        BOOLEAN IS_CURRENT
    }
    IDR_CORE_CLUSTER {
        TEXT CLUSTER_ID PK
        TEXT MATCH_ID FK
        VARIANT SOURCE_RECORD_IDS
        TEXT STATUS
        TEXT MERGED_INTO
    }
    IDR_CORE_CLUSTER_MEMBERSHIP {
        TEXT SOURCE_RECORD_ID PK
        TEXT CLUSTER_ID FK
    }
    IDR_CORE_CLUSTER_LOG {
        TEXT LOG_ID PK
        TEXT CLUSTER_ID FK
        TEXT EVENT_TYPE
        TEXT MATCH_ID
    }
    IDR_ML_FINGERPRINT_OBSERVATIONS {
        TEXT OBSERVATION_ID PK
        TEXT DEVICE_FINGERPRINT
        TEXT SOURCE_RECORD_ID FK
        TEXT GEO_METRO
        TEXT PUBLISHER_ID
    }
    IDR_ML_FINGERPRINT_STATS {
        TEXT DEVICE_FINGERPRINT PK
        NUMBER CLUSTER_COUNT
        NUMBER EVENT_COUNT
        FLOAT RARITY_SCORE
    }
    IDR_ML_CANDIDATE_PAIRS {
        TEXT PAIR_ID PK
        TEXT CLUSTER_ID_A FK
        TEXT CLUSTER_ID_B FK
        TEXT STATUS
        FLOAT ML_SCORE
        TEXT ML_TIER
    }
    IDR_ML_CANDIDATE_PAIR_EVIDENCE {
        TEXT EVIDENCE_ID PK
        TEXT PAIR_ID FK
        TEXT EVIDENCE_TYPE
        TEXT DEVICE_FINGERPRINT
        FLOAT FINGERPRINT_RARITY
    }
    IDR_ML_FEEDBACK {
        TEXT FEEDBACK_ID PK
        TEXT PAIR_ID FK
        NUMBER LABEL
        TEXT LABEL_SOURCE
    }
    IDR_ML_TRAINING_DATA {
        TEXT PAIR_ID PK
        NUMBER LABEL
        FLOAT AVG_EMBEDDING_SIMILARITY
        FLOAT EVIDENCE_VELOCITY
        NUMBER HAS_DETERMINISTIC_ANCHOR
    }
    IDR_CORE_IDENTITY_PROFILE {
        TEXT IDENTITY_ID PK
        TEXT PRIMARY_HEM
        TEXT PRIMARY_UID2
        TEXT PRIMARY_RAMPID
        TEXT PRIMARY_DEVICE_ID
        NUMBER DEVICE_COUNT
        NUMBER SOURCE_COUNT
        FLOAT CONFIDENCE_SCORE
    }
    IMPRESSION_LOG {
        TEXT IMPRESSION_ID PK
        TEXT AUCTION_ID FK
        TEXT DSP_ID
        TEXT CHANNEL
        TEXT IFA
        TEXT UID2
        TEXT HEM
        NUMBER CLEARING_PRICE
    }
    SSP_BID_REQUEST_RAW ||--|| STD_SSP_BID_REQUEST_RAW : "standardize"
    TAPAD_GRAPH_RAW ||--|| STD_TAPAD_GRAPH_RAW : "standardize"
    EXPERIAN_GRAPH_RAW ||--|| STD_EXPERIAN_GRAPH_RAW : "standardize"
    SSP_IMPRESSION_LOG_RAW ||--|| IMPRESSION_LOG : "transform"
    STD_SSP_BID_REQUEST_RAW ||--o{ IDR_CORE_IDENTIFIER_LINK : "extract"
    STD_TAPAD_GRAPH_RAW ||--o{ IDR_CORE_IDENTIFIER_LINK : "extract"
    STD_EXPERIAN_GRAPH_RAW ||--o{ IDR_CORE_IDENTIFIER_LINK : "extract"
    IDR_CORE_IDENTIFIER_LINK }o--|| IDR_CORE_ENTITY_IDENTIFIERS : "references"
    IDR_CORE_IDENTIFIER_LINK ||--o{ IDR_CORE_MATCH_RESULTS : "match pairs"
    IDR_CORE_MATCH_RESULTS ||--o| IDR_CORE_MATCH_LOG : "logs"
    IDR_CORE_MATCH_RESULTS ||--o{ IDR_CORE_CLUSTER : "clusters"
    IDR_CORE_CLUSTER ||--o{ IDR_CORE_CLUSTER_MEMBERSHIP : "members"
    IDR_CORE_CLUSTER ||--o{ IDR_CORE_CLUSTER_LOG : "audit"
    IDR_CORE_IDENTIFIER_LINK ||--o{ IDR_ML_FINGERPRINT_OBSERVATIONS : "fingerprints"
    IDR_ML_FINGERPRINT_OBSERVATIONS }o--|| IDR_ML_FINGERPRINT_STATS : "aggregates"
    IDR_CORE_CLUSTER ||--o{ IDR_ML_CANDIDATE_PAIRS : "candidates"
    IDR_ML_CANDIDATE_PAIRS ||--o{ IDR_ML_CANDIDATE_PAIR_EVIDENCE : "evidence"
    IDR_ML_CANDIDATE_PAIRS ||--o{ IDR_ML_FEEDBACK : "feedback"
    IDR_ML_CANDIDATE_PAIRS ||--o| IDR_ML_TRAINING_DATA : "training"
    IDR_CORE_CLUSTER ||--|| IDR_CORE_IDENTITY_PROFILE : "profile"
    IDR_CORE_IDENTITY_PROFILE ||--o{ IMPRESSION_LOG : "resolution"
`;

function MermaidDiagram({ chart, registerExport }: { chart: string; registerExport?: (fn: () => void) => void }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mermaidRef = useRef<HTMLDivElement>(null);
  const [zoom, setZoom] = useState(250);
  const zoomRef = useRef(250);
  const panRef = useRef({ x: 0, y: 0 });
  const baseVB = useRef({ x: 0, y: 0, w: 1600, h: 2000 });
  const isPanning = useRef(false);
  const panStart = useRef({ x: 0, y: 0 });
  const panStartVB = useRef({ x: 0, y: 0 });

  useEffect(() => { zoomRef.current = zoom; }, [zoom]);

  const applyViewBox = () => {
    const svg = mermaidRef.current?.querySelector('svg');
    if (!svg) return;
    const s = 100 / zoomRef.current;
    const vbW = baseVB.current.w * s;
    const vbH = baseVB.current.h * s;
    const vbX = panRef.current.x - (vbW - baseVB.current.w) / 2;
    const vbY = panRef.current.y - (vbH - baseVB.current.h) / 2;
    svg.setAttribute('viewBox', `${vbX} ${vbY} ${vbW} ${vbH}`);
  };

  const [themeKey, setThemeKey] = useState(() => (typeof document !== 'undefined' && document.documentElement.dataset.theme === 'dark') ? 'dark' : 'light');

  useEffect(() => {
    const obs = new MutationObserver(() => {
      const next = document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light';
      setThemeKey((prev) => (prev === next ? prev : next));
    });
    obs.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    if (!mermaidRef.current) return;
    const isDark = themeKey === 'dark';
    const bg = isDark ? '#1c2d48' : '#ffffff';
    const fg = isDark ? '#e2e8f0' : '#000000';
    mermaid.initialize({
      startOnLoad: false,
      theme: 'base',
      themeVariables: {
        primaryColor: bg,
        primaryTextColor: fg,
        primaryBorderColor: '#29b5e8',
        lineColor: '#29b5e8',
        secondaryColor: bg,
        tertiaryColor: bg,
        textColor: fg,
        mainBkg: bg,
        nodeBorder: '#29b5e8',
        clusterBkg: bg,
        titleColor: fg,
        edgeLabelBackground: bg,
        attributeBackgroundColorEven: bg,
        attributeBackgroundColorOdd: bg,
      },
    });
    mermaidRef.current.removeAttribute('data-processed');
    mermaidRef.current.innerHTML = chart;
    mermaid.run({ nodes: [mermaidRef.current] }).then(() => {
      setTimeout(() => {
        const svg = mermaidRef.current?.querySelector('svg');
        if (!svg) return;
        if (isDark) {
          const darkBg = '#1c2d48';
          const darkText = '#e2e8f0';
          let styleEl = svg.querySelector('style.dark-override');
          if (!styleEl) {
            styleEl = document.createElementNS('http://www.w3.org/2000/svg', 'style');
            styleEl.setAttribute('class', 'dark-override');
            svg.prepend(styleEl);
          }
          styleEl.textContent = `
            rect { fill: ${darkBg} !important; }
            text { fill: ${darkText} !important; }
            tspan { fill: ${darkText} !important; }
            foreignObject div, foreignObject span, foreignObject p { color: ${darkText} !important; background: transparent !important; background-color: transparent !important; }
          `;
          const isLight = (fillVal: string): boolean => {
            if (!fillVal) return false;
            const v = fillVal.trim().toLowerCase();
            if (['#fff', '#ffffff', 'white', 'rgb(255, 255, 255)', 'rgb(255,255,255)'].includes(v)) return true;
            const hslMatch = v.match(/^hsl\(\s*[\d.]+\s*,\s*[\d.]+%?\s*,\s*([\d.]+)%/);
            if (hslMatch && parseFloat(hslMatch[1]) > 60) return true;
            return false;
          };
          svg.querySelectorAll<SVGElement>('rect, path, polygon, circle, ellipse').forEach((el) => {
            const f = el.getAttribute('fill');
            if (f && f !== 'none' && isLight(f)) el.setAttribute('fill', darkBg);
            const sf = el.style?.fill;
            if (sf && sf !== 'none' && isLight(sf)) el.style.fill = darkBg;
          });
          svg.querySelectorAll<HTMLElement>('foreignObject *').forEach((el) => {
            const s = el.style;
            if (s) {
              if (isLight(s.backgroundColor || '')) s.backgroundColor = 'transparent';
              if (isLight(s.background || '')) s.background = 'transparent';
            }
          });
        }
        const vb = svg.getAttribute('viewBox');
        if (vb) {
          const parts = vb.split(/[\s,]+/).map(Number);
          baseVB.current = { x: parts[0], y: parts[1], w: parts[2], h: parts[3] };
        }
        svg.style.width = '100%';
        svg.style.height = '100%';
        svg.style.maxWidth = 'none';
        panRef.current = { x: baseVB.current.x, y: baseVB.current.y + (baseVB.current.h * (100 / 250) - baseVB.current.h) / 2 };
        zoomRef.current = 250;
        setZoom(250);
        applyViewBox();
      }, 50);
    });
  }, [chart, themeKey]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const handler = (e: WheelEvent) => {
      e.preventDefault();
      e.stopPropagation();
      const delta = e.deltaY > 0 ? -10 : 10;
      const newZoom = Math.min(Math.max(30, zoomRef.current + delta), 500);
      zoomRef.current = newZoom;
      setZoom(newZoom);
      applyViewBox();
    };
    el.addEventListener('wheel', handler, { passive: false });
    return () => el.removeEventListener('wheel', handler);
  }, []);

  const handleMouseDown = (e: React.MouseEvent) => {
    if (e.button !== 0) return;
    isPanning.current = true;
    panStart.current = { x: e.clientX, y: e.clientY };
    panStartVB.current = { ...panRef.current };
    (e.currentTarget as HTMLElement).style.cursor = 'grabbing';
  };

  const handleMouseMove = (e: React.MouseEvent) => {
    if (!isPanning.current) return;
    const svg = mermaidRef.current?.querySelector('svg');
    if (!svg || !containerRef.current) return;
    const rect = containerRef.current.getBoundingClientRect();
    const s = 100 / zoomRef.current;
    const ratioX = (baseVB.current.w * s) / rect.width;
    const ratioY = (baseVB.current.h * s) / rect.height;
    panRef.current = {
      x: panStartVB.current.x - (e.clientX - panStart.current.x) * ratioX,
      y: panStartVB.current.y - (e.clientY - panStart.current.y) * ratioY,
    };
    applyViewBox();
  };

  const handleMouseUp = (e: React.MouseEvent) => {
    isPanning.current = false;
    (e.currentTarget as HTMLElement).style.cursor = 'grab';
  };

  const resetView = () => {
    zoomRef.current = 250;
    setZoom(250);
    panRef.current = { x: baseVB.current.x, y: baseVB.current.y + (baseVB.current.h * (100 / 250) - baseVB.current.h) / 2 };
    applyViewBox();
  };

  const zoomIn = () => {
    const z = Math.min(zoomRef.current + 25, 500);
    zoomRef.current = z;
    setZoom(z);
    applyViewBox();
  };

  const zoomOut = () => {
    const z = Math.max(zoomRef.current - 25, 30);
    zoomRef.current = z;
    setZoom(z);
    applyViewBox();
  };

  const exportPng = () => {
    const svg = mermaidRef.current?.querySelector('svg');
    if (!svg) return;
    const clone = svg.cloneNode(true) as SVGSVGElement;
    clone.setAttribute('viewBox', `${baseVB.current.x} ${baseVB.current.y} ${baseVB.current.w} ${baseVB.current.h}`);
    clone.setAttribute('width', String(baseVB.current.w * 2));
    clone.setAttribute('height', String(baseVB.current.h * 2));
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
    const foreignObjects = clone.querySelectorAll('foreignObject');
    foreignObjects.forEach(fo => {
      const text = fo.textContent?.trim() || '';
      const x = parseFloat(fo.getAttribute('x') || '0');
      const y = parseFloat(fo.getAttribute('y') || '0');
      const h = parseFloat(fo.getAttribute('height') || '20');
      const svgText = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      svgText.setAttribute('x', String(x + 4));
      svgText.setAttribute('y', String(y + h / 2 + 4));
      svgText.setAttribute('font-size', '12');
      svgText.setAttribute('font-family', 'sans-serif');
      svgText.setAttribute('fill', '#000000');
      svgText.textContent = text;
      fo.parentNode?.replaceChild(svgText, fo);
    });
    const serializer = new XMLSerializer();
    const svgStr = serializer.serializeToString(clone);
    const svgDataUrl = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgStr);
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = baseVB.current.w * 2;
      canvas.height = baseVB.current.h * 2;
      const ctx = canvas.getContext('2d')!;
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
      canvas.toBlob((blob) => {
        if (!blob) return;
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.download = 'ssp360-er-diagram.png';
        link.href = url;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        setTimeout(() => URL.revokeObjectURL(url), 100);
      }, 'image/png');
    };
    img.src = svgDataUrl;
  };

  useEffect(() => {
    if (registerExport) registerExport(exportPng);
  }, [registerExport]);

  return (
    <div className="mermaid-viewer">
      <div className="mermaid-viewer__controls">
        <button onClick={zoomIn} title="Zoom in">+</button>
        <button onClick={zoomOut} title="Zoom out">−</button>
        <button className="mermaid-viewer__btn-text" onClick={resetView}>Reset</button>
        <button onClick={exportPng} title="Export as PNG"><svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M7 1v9M3 7l4 4 4-4M2 13h10"/></svg></button>
        <span className="mermaid-viewer__zoom-level">{zoom}%</span>
      </div>
      <div
        ref={containerRef}
        className="mermaid-viewer__canvas"
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
      >
        <div
          ref={mermaidRef}
          className="mermaid"
        />
      </div>
    </div>
  );
}

const stageDetails: Record<number, { title: string; body: ReactNode }> = {
  1: {
    title: '1. Extract — Bronze to Silver source records',
    body: (
      <>
        <p>Reads from stream-populated Bronze tables. Creates source records in <code>IDR_CORE_IDENTIFIER_LINK</code> and identifiers in <code>IDR_CORE_ENTITY_IDENTIFIERS</code>.</p>
        <p>Each raw record produces multiple identifier rows (HEM, UID2, RampID, PPID, DeviceID, Cookie, IP).</p>
        <pre className="arch-sql">{`-- Per source table (e.g., SSP_BID_REQUEST_RAW):
INSERT INTO IDR_CORE_IDENTIFIER_LINK (SOURCE_RECORD_ID, IDENTIFIER_ID, SOURCE_TYPE, IS_ACTIVE)
SELECT auction_id, id.IDENTIFIER_ID, 'SSP_BID_REQUEST_RAW', TRUE
FROM STD_SSP_BID_REQUEST_RAW s
JOIN IDR_CORE_ENTITY_IDENTIFIERS id ON ...`}</pre>
      </>
    ),
  },
  2: {
    title: '2. Standardize — Normalize identifiers',
    body: (
      <>
        <p>Two passes: built-in standardization (lowercase, trim, hash normalization) + custom SP for domain-specific rules (e.g., email canonicalization, phone formatting).</p>
        <p>Uses a standardization cache to avoid re-processing identical values across waves.</p>
        <pre className="arch-sql">{`-- Cache hit check:
SELECT IDENTIFIER_VALUE_NORMALIZED FROM IDR_STD_CACHE
WHERE IDENTIFIER_TYPE = :type AND IDENTIFIER_VALUE_RAW = :raw`}</pre>
      </>
    ),
  },
  3: {
    title: '3. Deterministic and Fuzzy Matching — Rules engine',
    body: (
      <>
        <p>Finds pairs of source records that share an identifier value. Uses a scoped pivot to avoid scanning the full 19M+ identifier table:</p>
        <pre className="arch-sql">{`-- Scoped to identifiers touching new sources only:
WITH new_sources AS (
  SELECT source_record_id FROM IDR_CORE_IDENTIFIER_LINK
  WHERE created_at > :last_run
),
candidate_sources AS (
  -- Sources sharing any identifier value with new_sources
  SELECT DISTINCT il2.source_record_id
  FROM new_sources ns
  JOIN IDR_CORE_IDENTIFIER_LINK il1 ON ns.source_record_id = il1.source_record_id
  JOIN IDR_CORE_ENTITY_IDENTIFIERS ei ON il1.identifier_id = ei.identifier_id
  JOIN IDR_CORE_IDENTIFIER_LINK il2 ON il2.identifier_id IN (
    SELECT identifier_id FROM IDR_CORE_ENTITY_IDENTIFIERS
    WHERE identifier_type = ei.identifier_type
      AND identifier_value_normalized = ei.identifier_value_normalized
  )
)
-- Then pivot source_identifiers from candidate set only`}</pre>
        <p><strong>Rules:</strong> HEM Exact, UID2 Exact, RampID Exact, PPID Exact, DeviceID Exact, Cookie Exact. Each rule generates edges in <code>IDR_CORE_MATCH_RESULTS</code>.</p>
      </>
    ),
  },
  4: {
    title: '4. Cluster — Incremental label propagation',
    body: (
      <>
        <p>Connected-component resolution using iterative label propagation. Uses <code>IDR_CORE_CLUSTER_MEMBERSHIP</code> for O(1) affected-cluster discovery.</p>
        <pre className="arch-sql">{`-- Step 1: Find affected clusters via denormalized lookup
SELECT DISTINCT cm.cluster_id
FROM IDR_CORE_MATCH_RESULTS mr
JOIN IDR_CORE_CLUSTER_MEMBERSHIP cm
  ON mr.source_record_id_a = cm.source_record_id
     OR mr.source_record_id_b = cm.source_record_id
WHERE mr.created_at > :last_run;

-- Step 2: Extract subgraph edges for affected clusters only
-- Step 3: Iterative label propagation (WHILE loop, seeded with current labels)
-- Step 4: Apply new labels → create/update/merge clusters
-- Step 5: Update CLUSTER_MEMBERSHIP denormalized table`}</pre>
        <p><strong>Key optimization:</strong> Only processes clusters touched by new edges, not the full 350K+ cluster graph.</p>
      </>
    ),
  },
  5: {
    title: '5. ML Scoring — Probabilistic identity linking',
    body: (
      <>
        <p>Three sub-stages for pairs that share weak signals (device fingerprints) but no deterministic anchor:</p>
        <ul>
          <li><strong>Fingerprint Observations:</strong> Accumulate device fingerprint sightings per source record</li>
          <li><strong>Candidate Generation:</strong> Find cluster-pairs sharing rare fingerprints across different clusters</li>
          <li><strong>Scoring:</strong> 30+ features (embedding similarity, geo/IP overlap, temporal patterns, rarity) → logistic regression UDF → tier assignment</li>
        </ul>
        <pre className="arch-sql">{`-- ML scoring UDF (called per pair):
IDR_ML_EXPLAIN(
  fingerprint_overlap_count, event_overlap_count, co_occurrence_time_span_hrs,
  publisher_diversity, c1_event_count, c2_event_count, ...,
  avg_fingerprint_rarity, evidence_velocity, cluster_size_ratio,
  has_deterministic_anchor
) → {score, tier, feature_importances, top_positive, top_negative}`}</pre>
        <p><strong>Tiers:</strong> MERGE_PERSON (score &gt; 0.9 + anchor), HOUSEHOLD_LINK (score &gt; 0.6), CANDIDATE_ONLY</p>
      </>
    ),
  },
  6: {
    title: '6. Profile — Golden record assembly',
    body: (
      <>
        <p>Builds a denormalized profile view per cluster for fast querying. Updates only clusters modified in this run.</p>
        <pre className="arch-sql">{`-- Update profile index for affected clusters:
MERGE INTO IDR_CORE_PROFILE_INDEX tgt
USING (
  SELECT cluster_id, OBJECT_AGG(...) AS profile_data
  FROM affected_clusters JOIN identifiers ...
  GROUP BY cluster_id
) src ON tgt.cluster_id = src.cluster_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...`}</pre>
      </>
    ),
  },
};

function ArchVizFrame({
  children,
  onExportPng,
  innerRef,
  className,
}: {
  children: ReactNode;
  onExportPng?: () => void;
  innerRef?: React.Ref<HTMLDivElement>;
  className?: string;
}) {
  const [isFullscreen, setIsFullscreen] = useState(false);
  return (
    <div
      ref={innerRef}
      className={`hiw2-flow-wrap arch-viz-frame ${isFullscreen ? 'hiw2-flow-wrap--fullscreen' : ''} ${className ?? ''}`}
    >
      <div className="hiw2-toolbar">
        {onExportPng && (
          <button type="button" className="hiw2-export-btn" onClick={onExportPng}>Export PNG</button>
        )}
        <button
          type="button"
          className="hiw2-export-btn hiw2-expand-btn"
          onClick={() => setIsFullscreen(v => !v)}
          aria-label={isFullscreen ? 'Exit fullscreen' : 'Expand to fullscreen'}
        >
          {isFullscreen ? <Minimize2 size={14} strokeWidth={1.5} /> : <Maximize2 size={14} strokeWidth={1.5} />}
          <span>{isFullscreen ? 'Exit' : 'Expand'}</span>
        </button>
      </div>
      {children}
    </div>
  );
}

const NAV_ITEMS = [
  { id: 'process-flow', label: 'Process Flow', icon: RefreshCw, summary: '6 stages' },
  { id: 'native-patterns', label: 'Native Patterns', icon: Sparkles, summary: '8 features' },
  { id: 'data-model', label: 'Data Model', icon: Grid3x3, summary: 'ER diagram' },
  { id: 'deployment', label: 'Deployment', icon: Cloud, summary: 'SPCS + WH' },
  { id: 'performance', label: 'Performance', icon: Gauge, summary: '2 profiles' },
];

export default function Architecture() {
  const [panelOpen, setPanelOpen] = useState(false);
  const [expandedStage, setExpandedStage] = useState<number | null>(null);
  const [tableDdl, setTableDdl] = useState<{ fqn: string; ddl: string } | null>(null);
  const [tableDdlLoading, setTableDdlLoading] = useState(false);
  const erExportRef = useRef<(() => void) | null>(null);
  const deployFrameRef = useRef<HTMLDivElement>(null);
  const [activeTab, setActiveTab] = useState('process-flow');

  const exportDeploymentPng = () => {
    const svg = deployFrameRef.current?.querySelector('svg');
    if (!svg) return;
    const clone = svg.cloneNode(true) as SVGSVGElement;
    const vb = svg.getAttribute('viewBox') || '0 0 1100 700';
    const [, , w, h] = vb.split(/\s+/).map(Number);
    clone.setAttribute('width', String(w * 2));
    clone.setAttribute('height', String(h * 2));
    clone.setAttribute('xmlns', 'http://www.w3.org/2000/svg');
    const svgStr = new XMLSerializer().serializeToString(clone);
    const url = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svgStr);
    const img = new Image();
    img.onload = () => {
      const canvas = document.createElement('canvas');
      canvas.width = w * 2;
      canvas.height = h * 2;
      const ctx = canvas.getContext('2d')!;
      ctx.fillStyle = '#ffffff';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      ctx.drawImage(img, 0, 0);
      canvas.toBlob((blob) => {
        if (!blob) return;
        const link = document.createElement('a');
        link.download = 'ssp360-deployment-architecture.png';
        link.href = URL.createObjectURL(blob);
        link.click();
        setTimeout(() => URL.revokeObjectURL(link.href), 100);
      }, 'image/png');
    };
    img.src = url;
  };

  const openTableDdl = async (schema: string, table: string) => {
    setPanelOpen(true);
    setExpandedStage(null);
    setTableDdlLoading(true);
    setTableDdl(null);
    try {
      const res = await fetch(`/api/table-ddl?schema=${schema}&table=${table}`);
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();
      setTableDdl(data);
    } catch (e: any) {
      setTableDdl({ fqn: `${schema}.${table}`, ddl: `-- Error fetching DDL:\n-- ${e.message}` });
    } finally {
      setTableDdlLoading(false);
    }
  };

  function renderSection() {
    switch (activeTab) {
      case 'process-flow':
        return (
          <>
            <h2 className="stg-section-title"><RefreshCw size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Process Flow</h2>
            <ArchVizFrame>
              <div className="arch-topology-wide">
                <div className="arch-topo-col arch-topo-col--snowflake">
                  <div className="arch-topo-col__header arch-topo-col__header--sf">Snowflake AI Data Cloud</div>
                  <div className="arch-topo-layers">
                    <div className="arch-topo-layer">
                      <div className="arch-topo-layer__label">Ingestion</div>
                      <div className="arch-topo-layer__items">
                        <div className="arch-topo-chip">Snowpipe Batch</div>
                        <div className="arch-topo-chip">Snowpipe Streaming</div>
                        <div className="arch-topo-chip">Stages</div>
                        <div className="arch-topo-chip">Openflow</div>
                        <div className="arch-topo-chip">Marketplace</div>
                      </div>
                    </div>
                    <div className="arch-topo-layer-arrow"><div className="arrow-cell">&darr;</div></div>
                    <div className="arch-topo-layer">
                      <div className="arch-topo-layer__label">Bronze <span className="arch-topo-layer__sublabel">(Source Tables)</span></div>
                      <div className="arch-topo-layer__items">
                        <div className="arch-topo-chip">SSP_BID_REQUEST_RAW</div>
                        <div className="arch-topo-chip">TAPAD_GRAPH_RAW</div>
                        <div className="arch-topo-chip">EXPERIAN_GRAPH_RAW</div>
                        <div className="arch-topo-chip">IMPRESSION_LOG_RAW</div>
                      </div>
                    </div>
                    <div className="arch-topo-layer-arrow"><div className="arrow-cell">&darr;</div></div>
                    <div className="arch-topo-cdc-row">
                      <div className="arch-topo-cdc-chip"><GitBranch size={14} /> Streams</div>
                    </div>
                    <div className="arch-topo-layer-arrow"><div className="arrow-cell">&darr;</div></div>
                    <div className="arch-topo-task-box">
                      <div className="arch-topo-task-box__label">Task: IDR_INCREMENTAL_TASK</div>
                      <div className="arch-topo-task-box__call">CALL SP_RUN_IDR_PIPELINE()</div>
                      <div className="arch-topo-task-box__sp">
                        <div className="arch-topo-task-box__sp-label">SP_RUN_IDR_PIPELINE</div>
                        <div className="arch-topo-stage-row">
                          {[1, 2, 3, 4, 5, 6].map((n) => (
                            <div
                              key={n}
                              className={`arch-topo-stage arch-topo-stage--clickable${expandedStage === n ? ' arch-topo-stage--active' : ''}`}
                              onClick={() => { setPanelOpen(true); setExpandedStage(n); }}
                            >
                              <div className="arch-topo-stage__icon">{n}</div>
                              <div className="arch-topo-stage__name">{stageDetails[n].title.split(' — ')[0].replace(/^\d+\.\s*/, '')}</div>
                            </div>
                          ))}
                        </div>
                        <div className="arch-topo-hint">Click any stage for details</div>
                      </div>
                    </div>
                    <div className="arch-topo-layer-arrow"><div className="arrow-cell">&darr;</div></div>
                    <div className="arch-topo-layer arch-topo-layer--silver">
                      <div className="arch-topo-layer__label">Silver <span className="arch-topo-layer__sublabel">(Identity Graph Tables)</span></div>
                      <div className="arch-topo-layer__items" style={{justifyContent: 'center'}}>
                        <div className="arch-topo-chip">IDR_CORE_CLUSTER</div>
                        <div className="arch-topo-chip">IDR_CORE_CLUSTER_MEMBERSHIP</div>
                        <div className="arch-topo-chip">IDR_CORE_MATCH_RESULTS</div>
                      </div>
                      <div className="arch-topo-layer__items" style={{marginTop: 5, justifyContent: 'center'}}>
                        <div className="arch-topo-chip">IDR_CORE_IDENTIFIER_LINK</div>
                        <div className="arch-topo-chip">IDR_CORE_ENTITY_IDENTIFIERS</div>
                        <div className="arch-topo-chip">IDR_CORE_PROFILE_INDEX</div>
                      </div>
                      <div className="arch-topo-layer__items" style={{marginTop: 5, justifyContent: 'center'}}>
                        <div className="arch-topo-chip">IDR_ML_CANDIDATE_PAIRS</div>
                        <div className="arch-topo-chip">IDR_ML_FINGERPRINT_OBSERVATIONS</div>
                        <div className="arch-topo-chip">IDR_CORE_EVENT_LOG</div>
                      </div>
                    </div>
                    <div className="arch-topo-layer-arrow"><div className="arrow-cell">&darr;</div></div>
                    <div className="arch-topo-layer">
                      <div className="arch-topo-layer__label">Gold</div>
                      <div className="arch-topo-layer__items">
                        <div className="arch-topo-chip arch-topo-chip--clickable arch-topo-chip--with-tag" onClick={() => openTableDdl('GOLD', 'IDR_CORE_IDENTITY_PROFILE')}>IDR_CORE_IDENTITY_PROFILE <br/><span className="arch-topo-tag">Dynamic Table</span></div>
                        <div className="arch-topo-chip arch-topo-chip--clickable" onClick={() => openTableDdl('GOLD', 'IMPRESSION_LOG')}>IMPRESSION_LOG</div>
                      </div>
                      <div className="arch-topo-hint">Click tables for DDL</div>
                    </div>
                    <div className="arch-topo-supporting">
                      <div className="arch-topo-supporting__title">Key Supporting Features</div>
                      <div className="arch-topo-layer__items">
                        <div className="arch-topo-chip">Streams</div>
                        <div className="arch-topo-chip">Object Tagging</div>
                        <div className="arch-topo-chip">Data Masking</div>
                        <div className="arch-topo-chip">Metadata</div>
                        <div className="arch-topo-chip">Secure Jobs</div>
                        <div className="arch-topo-chip">Network Rules</div>
                        <div className="arch-topo-chip">Zero-Copy Clone</div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </ArchVizFrame>
          </>
        );

      case 'native-patterns':
        return (
          <>
            <h2 className="stg-section-title"><Sparkles size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Snowflake-Native Patterns</h2>
            <div className="arch-features">
              <div className="arch-feature-card">
                <h4>Streams + Tasks (CDC)</h4>
                <p>Streams track DML changes on Bronze tables. The Task fires every 60s but only runs when SYSTEM$STREAM_HAS_DATA() is true — zero idle cost.</p>
              </div>
              <div className="arch-feature-card">
                <h4>JavaScript Stored Procedures</h4>
                <p>Pipeline orchestrator is a JS SP that sequences stages, captures metrics, handles errors, and writes audit logs — all within a single transaction context.</p>
              </div>
              <div className="arch-feature-card">
                <h4>SQL Stored Procedures</h4>
                <p>Individual stages (match, cluster) are SQL SPs using LET/RESULTSET/CURSOR patterns, EXECUTE IMMEDIATE for dynamic SQL, and temp tables for intermediate state.</p>
              </div>
              <div className="arch-feature-card">
                <h4>VARIANT + LATERAL FLATTEN</h4>
                <p>Cluster membership stored as VARIANT arrays for flexible graph traversal. Denormalized into CLUSTER_MEMBERSHIP for O(1) lookups at scale.</p>
              </div>
              <div className="arch-feature-card">
                <h4>VECTOR_COSINE_SIMILARITY</h4>
                <p>Native vector operations for embedding-based similarity scoring in the ML layer — no external ML infrastructure needed.</p>
              </div>
              <div className="arch-feature-card">
                <h4>Zero-Copy Cloning</h4>
                <p>Instant database backups before major changes. Used for regression protection during optimization cycles.</p>
              </div>
              <div className="arch-feature-card">
                <h4>SPCS (Container Services)</h4>
                <p>Frontend + API deployed as a single container with OAuth-based ingress. No VPN or network config — just a public HTTPS endpoint.</p>
              </div>
              <div className="arch-feature-card">
                <h4>Incremental Processing</h4>
                <p>Every stage is scoped to "new since last run" via process-state timestamps. Steady-state cost is proportional to wave size, not graph size.</p>
              </div>
            </div>
          </>
        );

      case 'data-model':
        return (
          <>
            <h2 className="stg-section-title"><Grid3x3 size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Data Model</h2>
            <ArchVizFrame onExportPng={() => erExportRef.current?.()}>
              <div className="arch-er-diagram">
                <MermaidDiagram chart={erDiagram} registerExport={(fn) => { erExportRef.current = fn; }} />
              </div>
            </ArchVizFrame>
          </>
        );

      case 'deployment':
        return (
          <>
            <h2 className="stg-section-title"><Cloud size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Deployment Architecture</h2>
            <ArchVizFrame onExportPng={exportDeploymentPng} innerRef={deployFrameRef}>
              <div className="arch-whiteboard">
                <svg width="100%" viewBox="0 0 1100 700" xmlns="http://www.w3.org/2000/svg">
                  <defs>
                    <pattern id="wb-dots" x="0" y="0" width="20" height="20" patternUnits="userSpaceOnUse">
                      <circle cx="2" cy="2" r="1" fill="#e2e8f0" />
                    </pattern>
                    <marker id="wb-arrow" markerWidth="10" markerHeight="10" refX="9" refY="4" orient="auto">
                      <path d="M 0 0 L 10 4 L 0 8 z" fill="#475569" />
                    </marker>
                    <filter id="wb-shadow" x="-10%" y="-10%" width="120%" height="130%">
                      <feDropShadow dx="0" dy="4" stdDeviation="6" floodColor="#64748b" floodOpacity="0.12" />
                    </filter>
                  </defs>
                  <rect width="1100" height="700" fill="#ffffff" rx="16" />
                  <g filter="url(#wb-shadow)">
                    <path d="M 42 108 C 60 104, 240 105, 258 110 C 266 118, 264 172, 258 180 C 242 186, 62 185, 46 178 C 38 170, 36 118, 42 108 Z" fill="#f8fafc" stroke="#94a3b8" strokeWidth="2" />
                    <circle cx="78" cy="144" r="18" fill="#e0f2fe" stroke="#29b5e8" strokeWidth="1.5" />
                    <path d="M 70 150 C 70 142, 86 142, 86 150" fill="none" stroke="#29b5e8" strokeWidth="1.5" />
                    <circle cx="78" cy="138" r="5" fill="#29b5e8" />
                    <text x="106" y="140" fontSize="14" fontWeight="700" fill="#0f172a">SSP Ops / Analyst</text>
                    <text x="106" y="158" fontSize="11" fill="#64748b">Identity resolution operator</text>
                  </g>
                  <path d="M 328 58 C 370 52, 980 54, 1020 60 C 1030 70, 1028 650, 1020 662 C 1000 672, 360 670, 332 660 C 322 648, 320 72, 328 58 Z" fill="none" stroke="#29b5e8" strokeWidth="2.5" strokeDasharray="8 4" opacity="0.7" />
                  <text x="346" y="82" fontSize="12" fontWeight="700" fill="#29b5e8" letterSpacing="1">SNOWFLAKE ACCOUNT</text>
                  <g filter="url(#wb-shadow)">
                    <path d="M 400 108 C 420 104, 700 105, 720 110 C 728 118, 726 192, 720 200 C 700 206, 422 205, 404 198 C 396 190, 394 118, 400 108 Z" fill="#f8fafc" stroke="#94a3b8" strokeWidth="2" />
                    <text x="424" y="134" fontSize="13" fontWeight="700" fill="#0f172a">SPCS: Compute Pool</text>
                    <text x="424" y="152" fontSize="11" fill="#64748b">Runs containerized web application</text>
                    <rect x="424" y="162" width="272" height="30" rx="8" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <text x="440" y="182" fontSize="12" fontWeight="600" fill="#0f172a">SSP 360 App</text>
                    <text x="560" y="182" fontSize="11" fill="#64748b">React + FastAPI</text>
                  </g>
                  <g filter="url(#wb-shadow)">
                    <path d="M 380 240 C 400 236, 740 237, 760 242 C 768 250, 766 380, 760 388 C 740 394, 402 393, 384 386 C 376 378, 374 250, 380 240 Z" fill="#f8fafc" stroke="#94a3b8" strokeWidth="2" />
                    <text x="404" y="266" fontSize="13" fontWeight="700" fill="#0f172a">WAREHOUSE: IDR_DEMO_WH</text>
                    <text x="404" y="284" fontSize="11" fill="#64748b">SQL execution engine — pipelines, queries, Dynamic Tables</text>
                    <rect x="404" y="298" width="160" height="68" rx="8" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <text x="420" y="320" fontSize="11" fontWeight="600" fill="#0f172a">Stored Procedures</text>
                    <text x="420" y="338" fontSize="10" fill="#64748b">IDR pipeline</text>
                    <text x="420" y="352" fontSize="10" fill="#64748b">orchestration</text>
                    <rect x="580" y="298" width="160" height="68" rx="8" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <text x="596" y="320" fontSize="11" fontWeight="600" fill="#0f172a">Snowpark ML</text>
                    <text x="596" y="338" fontSize="10" fill="#64748b">Python UDFs</text>
                    <text x="596" y="352" fontSize="10" fill="#64748b">Scoring & matching</text>
                  </g>
                  <g filter="url(#wb-shadow)">
                    <path d="M 380 430 C 400 426, 970 427, 990 432 C 998 440, 996 614, 990 622 C 970 628, 402 627, 384 620 C 376 612, 374 440, 380 430 Z" fill="#f8fafc" stroke="#94a3b8" strokeWidth="2" />
                    <text x="404" y="456" fontSize="13" fontWeight="700" fill="#0f172a">DATABASE / STORAGE LAYER</text>
                    <text x="404" y="474" fontSize="11" fill="#64748b">Medallion architecture — Bronze → Silver → Gold</text>
                    <rect x="404" y="488" width="170" height="62" rx="10" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <rect x="404" y="488" width="170" height="22" rx="10" fill="#f1f5f9" />
                    <text x="458" y="504" fontSize="11" fontWeight="700" fill="#0f172a">BRONZE</text>
                    <text x="416" y="524" fontSize="10" fill="#64748b">Raw events</text>
                    <text x="416" y="538" fontSize="10" fill="#64748b">Source IDs &amp; payloads</text>
                    <rect x="594" y="488" width="170" height="62" rx="10" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <rect x="594" y="488" width="170" height="22" rx="10" fill="#f1f5f9" />
                    <text x="650" y="504" fontSize="11" fontWeight="700" fill="#0f172a">SILVER</text>
                    <text x="606" y="524" fontSize="10" fill="#64748b">Identity graph edges</text>
                    <text x="606" y="538" fontSize="10" fill="#64748b">Normalized features</text>
                    <rect x="784" y="488" width="170" height="62" rx="10" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <rect x="784" y="488" width="170" height="22" rx="10" fill="#f1f5f9" />
                    <text x="842" y="504" fontSize="11" fontWeight="700" fill="#0f172a">GOLD</text>
                    <text x="796" y="524" fontSize="10" fill="#64748b">Resolved profiles</text>
                    <text x="796" y="538" fontSize="10" fill="#64748b">Identity clusters</text>
                    <rect x="404" y="566" width="170" height="46" rx="10" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <rect x="404" y="566" width="170" height="20" rx="10" fill="#f1f5f9" />
                    <text x="460" y="581" fontSize="11" fontWeight="700" fill="#0f172a">CONFIG</text>
                    <text x="416" y="600" fontSize="10" fill="#64748b">Pipeline settings</text>
                    <rect x="594" y="566" width="170" height="46" rx="10" fill="white" stroke="#cbd5e1" strokeWidth="1.5" />
                    <rect x="594" y="566" width="170" height="20" rx="10" fill="#f1f5f9" />
                    <text x="658" y="581" fontSize="11" fontWeight="700" fill="#0f172a">APP</text>
                    <text x="606" y="600" fontSize="10" fill="#64748b">SPCS service &amp; image repo</text>
                  </g>
                  <path d="M 260 144 Q 340 144 398 154" fill="none" stroke="#475569" strokeWidth="2" markerEnd="url(#wb-arrow)" strokeLinecap="round" />
                  <rect x="306" y="130" width="52" height="18" rx="9" fill="white" stroke="#e2e8f0" strokeWidth="1" />
                  <text x="332" y="143" fontSize="9" fontWeight="600" fill="#475569" textAnchor="middle">HTTPS</text>
                  <path d="M 560 202 Q 560 220 560 238" fill="none" stroke="#475569" strokeWidth="2" markerEnd="url(#wb-arrow)" strokeLinecap="round" />
                  <path d="M 570 390 Q 570 410 570 428" fill="none" stroke="#475569" strokeWidth="2" markerEnd="url(#wb-arrow)" strokeLinecap="round" />
                  <path d="M 576 518 Q 584 518 592 518" fill="none" stroke="#475569" strokeWidth="1.5" markerEnd="url(#wb-arrow)" strokeLinecap="round" opacity="0.6" />
                  <path d="M 766 518 Q 774 518 782 518" fill="none" stroke="#475569" strokeWidth="1.5" markerEnd="url(#wb-arrow)" strokeLinecap="round" opacity="0.6" />
                </svg>
              </div>
            </ArchVizFrame>
          </>
        );

      case 'performance':
        return (
          <>
            <h2 className="stg-section-title"><Gauge size={16} style={{ marginRight: 8, verticalAlign: '-2px' }} /> Performance Characteristics</h2>
            <div className="arch-cards">
              <details className="arch-card" open>
                <summary>Scaling behavior</summary>
                <div className="arch-card__body">
                  <ul>
                    <li><strong>Match stage:</strong> Parallelizes well with WH size (5.5x speedup X-Small → Large). JOIN-heavy workload benefits from more compute nodes.</li>
                    <li><strong>Cluster stage:</strong> Limited parallelism due to iterative label propagation. Benefits from faster single-thread execution on larger WHs but doesn't scale linearly.</li>
                    <li><strong>ML scoring:</strong> Moderate parallelism (2.3x). UDF execution + feature materialization can be distributed.</li>
                    <li><strong>Recommendation:</strong> X-Small for batch ($0.57–$1.22/M); Medium for sub-10min SLA.</li>
                  </ul>
                </div>
              </details>
              <details className="arch-card" open>
                <summary>Optimizations applied</summary>
                <div className="arch-card__body">
                  <ul>
                    <li>Denormalized CLUSTER_MEMBERSHIP table — eliminates LATERAL FLATTEN on 350K+ clusters</li>
                    <li>Scoped match pivot — only processes identifiers from new + candidate sources (not full 19M table)</li>
                    <li>Seeded label propagation — starts from current cluster labels, converges in fewer iterations</li>
                    <li>Incremental ML — only scores PENDING pairs, caps embedding cross-product at 5 samples</li>
                    <li>Key-based evidence MERGE — replaces 100M+ row anti-join with primary-key dedup</li>
                    <li>Delta-only SCORE_SNAPSHOT — cuts evidence growth by ~95%</li>
                  </ul>
                </div>
              </details>
            </div>
          </>
        );

      default:
        return null;
    }
  }

  return (
    <div className="architecture-page">
      <h1>Architecture</h1>
      <p className="page-subtitle">
        End-to-end identity graph processing and resolution workflows powered entirely by Snowflake-native services.
      </p>

      <div className="stg-layout">
        <nav className="stg-nav">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.id}
              className={`stg-nav-item${activeTab === item.id ? ' stg-nav-item--active' : ''}`}
              onClick={() => setActiveTab(item.id)}
            >
              <item.icon size={16} className="stg-nav-icon" />
              <span className="stg-nav-text">
                <span className="stg-nav-label">{item.label}</span>
                <span className="stg-nav-summary">{item.summary}</span>
              </span>
            </button>
          ))}
        </nav>

        <div className="stg-content">
          {renderSection()}
        </div>
      </div>

      {/* Slide-in panel for stage details */}
      <div className={`arch-stage-panel${panelOpen ? ' arch-stage-panel--open' : ''}`}>
        <div className="arch-stage-panel__header">
          <h3>{tableDdl || tableDdlLoading ? 'Table Definition' : 'Pipeline Stages'}</h3>
          <button className="arch-stage-panel__close" onClick={() => { setPanelOpen(false); setExpandedStage(null); setTableDdl(null); }}>&times;</button>
        </div>
        <div className="arch-stage-panel__body">
          {tableDdlLoading && <div style={{ padding: 20, color: '#64748b' }}>Loading DDL...</div>}
          {tableDdl && !tableDdlLoading && (
            <div className="arch-ddl-view">
              <div className="arch-ddl-view__header">
                <span className="arch-ddl-view__fqn">{tableDdl.fqn}</span>
                <button className="arch-ddl-view__copy" onClick={() => { navigator.clipboard.writeText(tableDdl.ddl); }} title="Copy to clipboard"><svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg></button>
              </div>
              <pre className="arch-ddl-view__code">{tableDdl.ddl}</pre>
            </div>
          )}
          {!tableDdl && !tableDdlLoading && [1, 2, 3, 4, 5, 6].map((n) => (
            <div key={n} className={`arch-stage-accordion${expandedStage === n ? ' arch-stage-accordion--open' : ''}`}>
              <button
                className="arch-stage-accordion__trigger"
                onClick={() => setExpandedStage(expandedStage === n ? null : n)}
              >
                <span className="arch-stage-accordion__icon">{n}</span>
                <span className="arch-stage-accordion__title">{stageDetails[n].title.split('. ').slice(1).join('. ')}</span>
                <span className="arch-stage-accordion__chevron">{expandedStage === n ? '−' : '+'}</span>
              </button>
              {expandedStage === n && (
                <div className="arch-stage-accordion__content">
                  {stageDetails[n].body}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>
      {panelOpen && <div className="arch-stage-panel__backdrop" onClick={() => { setPanelOpen(false); setExpandedStage(null); setTableDdl(null); }} />}
    </div>
  );
}
