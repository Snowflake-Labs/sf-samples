import { useState, useEffect, useCallback } from 'react';
import { Play, ChevronLeft, ChevronRight, X } from 'lucide-react';

type Page = 'dashboard' | 'identities' | 'identity-detail' | 'generate-data' | 'run-history' | 'data-to-identity' | 'how-it-works' | 'cost-analysis' | 'architecture';

interface TourStep {
  page: Page;
  title: string;
  talkingPoints: string[];
  identityId?: string;
  tab?: string;
}

const TOUR_STEPS: TourStep[] = [
  {
    page: 'how-it-works',
    title: 'How IDR Works',
    talkingPoints: [
      'Single-engine design: deterministic, fuzzy, canonical, ML, and LLM matching combined into one resolution unit.',
      'Everything runs natively in Snowflake — SQL stored procedures, streams, tasks. No external compute.',
      'Click "View Details" to show the full narrative with stage-by-stage explanation.',
    ],
  },
  {
    page: 'data-to-identity',
    title: 'Data to Identity Journey',
    talkingPoints: [
      'End-to-end journey: raw POS / Loyalty / Web / Shopify rows → extracted identifiers → matched → clustered.',
      'Shows how a single event connects to the broader identity graph through shared identifiers.',
      'Demonstrates the deterministic rules engine in action.',
    ],
  },
  {
    page: 'dashboard',
    title: 'Dashboard Overview',
    talkingPoints: [
      'Live state of the identity graph: active clusters, total identifiers, match rates across sources.',
      'All metrics computed from the Silver-layer tables — no pre-aggregation needed.',
      'The graph grows incrementally with each pipeline run (triggered by stream data).',
    ],
  },
  {
    page: 'identities',
    title: 'Identity Clusters',
    talkingPoints: [
      'Each cluster represents a resolved person or household.',
      'Click into any cluster to see all source records, identifiers, and how they linked together.',
      'Show the cluster graph visualization — each node is a source record, edges are match rules.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    title: 'Cluster Deep Dive',
    talkingPoints: [
      'This is a single resolved identity — all source records, identifiers, and match edges that link them.',
      'The overview graph shows how different data sources connect via shared identifiers.',
      'Let\'s walk through each tab to see the details.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    tab: 'sources',
    title: 'Sources',
    talkingPoints: [
      'Click the "Sources" tab above.',
      'Every raw source record that contributed to this cluster — POS transactions, Loyalty member records, Web clickstream events, Shopify orders.',
      'You can click any row to see the full raw payload and extracted identifiers.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    tab: 'idr',
    title: 'IDR Explanation',
    talkingPoints: [
      'Click the "IDR Explanation" tab.',
      'Shows exactly which deterministic rules fired to link these records together.',
      'Each edge in the graph is a match decision — email match, phone match, loyalty number, device ID, nickname-canonical first name, fuzzy last name, ML score, LLM verdict, etc.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    tab: 'lineage',
    title: 'Resolution History',
    talkingPoints: [
      'Click the "Resolution History" tab.',
      'Full audit trail: every merge decision logged with timestamp, rule, and confidence.',
      'This is how we provide explainability — every identity link can be traced back to its evidence.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    tab: 'connected',
    title: 'Connected Clusters',
    talkingPoints: [
      'Click the "Connected Clusters" tab.',
      'ML-identified candidate pairs — clusters that share weak signals (device fingerprints) but no deterministic anchor yet.',
      'Each pair has a score, tier (HOUSEHOLD_LINK / CANDIDATE_ONLY), and evidence trail.',
    ],
  },
  {
    page: 'identity-detail',
    identityId: 'd26949f5-7413-4d83-b8a4-f494074b9613',
    tab: 'ai',
    title: 'AI Summary',
    talkingPoints: [
      'Click the "AI Summary" tab.',
      'LLM-generated natural language summary of the cluster — who this identity likely represents.',
      'Combines all deterministic and probabilistic signals into a human-readable narrative.',
    ],
  },
  {
    page: 'run-history',
    title: 'Task Runs',
    tab: 'tasks',
    talkingPoints: [
      'Pipeline runs automatically via Snowflake Tasks, triggered when new data arrives via streams.',
      'Each run shows: records processed, new clusters, merges, ML pair upgrades.',
      'On X-Small warehouse: ~10 min for 1M records.',
    ],
  },
  {
    page: 'run-history',
    tab: 'pipeline',
    title: 'Pipeline Events',
    talkingPoints: [
      'Scroll down to see individual pipeline stage events with timing breakdown.',
      'Each stage (extract, standardize, match, cluster, ML) is logged independently.',
      'Use this to identify bottlenecks and monitor pipeline health over time.',
    ],
  },
];

interface DemoTourProps {
  onNavigate: (page: Page, identityId?: string, tab?: string) => void;
  collapsed?: boolean;
}

export default function DemoTour({ onNavigate, collapsed }: DemoTourProps) {
  const [active, setActive] = useState(false);
  const [step, setStep] = useState(0);

  const activate = useCallback(() => {
    setActive(true);
    setStep(0);
    onNavigate(TOUR_STEPS[0].page, TOUR_STEPS[0].identityId, TOUR_STEPS[0].tab);
  }, [onNavigate]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === 'd') {
        e.preventDefault();
        activate();
      }
      if (active && e.key === 'Escape') {
        setActive(false);
      }
      if (active && e.key === 'ArrowRight') {
        goNext();
      }
      if (active && e.key === 'ArrowLeft') {
        goPrev();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  });

  useEffect(() => {
    if (window.location.hash === '#demo-mode' || window.location.hash === '#/demo-mode') {
      activate();
    }
  }, [activate]);

  const goNext = () => {
    if (step < TOUR_STEPS.length - 1) {
      const next = step + 1;
      setStep(next);
      onNavigate(TOUR_STEPS[next].page, TOUR_STEPS[next].identityId, TOUR_STEPS[next].tab);
    }
  };

  const goPrev = () => {
    if (step > 0) {
      const prev = step - 1;
      setStep(prev);
      onNavigate(TOUR_STEPS[prev].page, TOUR_STEPS[prev].identityId, TOUR_STEPS[prev].tab);
    }
  };

  if (!active) {
    return (
      <button className={`demo-tour-start-btn${collapsed ? ' demo-tour-start-btn--collapsed' : ''}`} onClick={activate} title="Start guided demo (Ctrl+D)">
        <Play size={16} />
        {!collapsed && <span>Start Demo</span>}
      </button>
    );
  }

  const current = TOUR_STEPS[step];

  return (
    <div className="demo-tour-bar">
      <div className="demo-tour-bar__header">
        <span className="demo-tour-bar__step-badge">
          {step + 1} / {TOUR_STEPS.length}
        </span>
        <span className="demo-tour-bar__title">{current.title}</span>
        <button className="demo-tour-bar__close" onClick={() => setActive(false)} aria-label="Exit tour">
          <X size={16} />
        </button>
      </div>
      <ul className="demo-tour-bar__points">
        {current.talkingPoints.map((point, i) => (
          <li key={i}>{point}</li>
        ))}
      </ul>
      <div className="demo-tour-bar__nav">
        <button onClick={() => { setStep(0); onNavigate(TOUR_STEPS[0].page, TOUR_STEPS[0].identityId, TOUR_STEPS[0].tab); }} className="btn-secondary btn-sm demo-tour-bar__reset" title="Restart demo">
          Reset
        </button>
        <button onClick={goPrev} disabled={step === 0} className="btn-secondary btn-sm">
          <ChevronLeft size={14} /> Back
        </button>
        <button onClick={goNext} disabled={step === TOUR_STEPS.length - 1} className="btn-primary btn-sm" style={step === TOUR_STEPS.length - 1 ? { display: 'none' } : undefined}>
          Next <ChevronRight size={14} />
        </button>
        {step === TOUR_STEPS.length - 1 && (
          <button onClick={() => setActive(false)} className="btn-primary btn-sm">
            Done
          </button>
        )}
      </div>
    </div>
  );
}

export { TOUR_STEPS };
export type { TourStep };
