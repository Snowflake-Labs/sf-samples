import { lazy, Suspense, useState, useEffect, useCallback } from 'react';
import type { LucideIcon } from 'lucide-react';
import {
  BookOpen,
  DatabaseZap,
  Fingerprint,
  History,
  LayoutDashboard,
  PlayCircle,
  Sparkles,
} from 'lucide-react';
import Dashboard from './pages/Dashboard';
import IdentityList from './pages/IdentityList';
import IdentityDetailPage from './pages/IdentityDetail';
import GenerateData from './pages/GenerateData';
import RunHistory from './pages/RunHistory';
import AIReview from './pages/AIReview';
import Settings from './pages/Settings';
import TestRunner from './pages/TestRunner';
import { ThemeToggle } from './components/ThemeToggle';
import DemoTour from './components/DemoTour';

const HowItWorksV2 = lazy(() => import('./pages/HowItWorksV2'));
const IdrFlowAnimationPage = lazy(() => import('./pages/IdrFlowAnimationPage'));

type Page =
  | 'dashboard'
  | 'identities'
  | 'identity-detail'
  | 'generate-data'
  | 'run-history'
  | 'data-to-identity'
  | 'how-it-works'
  | 'idr-flow-animation'
  | 'cost-analysis'
  | 'architecture'
  | 'ai-review'
  | 'test-runner'
  | 'settings';

type NavItem = { key: Page; label: string; icon: LucideIcon; match?: Page[] };

const SIDEBAR_ICON_SIZE = 18;
const SIDEBAR_ICON_STROKE = 1.5;

const NAV_SECTIONS: { title: string; items: NavItem[] }[] = [
  {
    title: 'Workspace',
    items: [
      { key: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
      { key: 'identities', label: 'Identities', icon: Fingerprint, match: ['identities', 'identity-detail'] },
      { key: 'ai-review', label: 'AI Review', icon: Sparkles },
    ],
  },
  {
    title: 'Pipeline',
    items: [
      { key: 'generate-data', label: 'Generate Data', icon: DatabaseZap },
      { key: 'run-history', label: 'Run History', icon: History },
    ],
  },
  {
    title: 'How it Works',
    items: [
      { key: 'how-it-works', label: 'High-level View', icon: BookOpen },
      { key: 'idr-flow-animation', label: 'Understand IDR Engine', icon: PlayCircle },
    ],
  },
];

const VALID_PAGES: Page[] = [
  'dashboard', 'identities', 'identity-detail',
  'generate-data', 'run-history', 'data-to-identity', 'how-it-works', 'idr-flow-animation', 'cost-analysis', 'architecture',
  'ai-review', 'test-runner', 'settings',
];

function parseHash(): { page: Page; identityId: string | null } {
  const raw = window.location.hash.replace(/^#\/?/, '');
  const [segment, ...rest] = raw.split('/');
  const identityId = rest.join('/') || null;

  if (segment === 'identity' && identityId) {
    return { page: 'identity-detail', identityId };
  }
  if (VALID_PAGES.includes(segment as Page)) {
    return { page: segment as Page, identityId: null };
  }
  return { page: 'dashboard', identityId: null };
}

function buildHash(page: Page, identityId?: string | null): string {
  if (page === 'identity-detail' && identityId) {
    return `#/identity/${identityId}`;
  }
  if (page === 'dashboard') return '#/';
  return `#/${page}`;
}

export default function App() {
  const initial = parseHash();
  const [page, setPageState] = useState<Page>(initial.page);
  const [selectedIdentityId, setSelectedIdentityId] = useState<string | null>(initial.identityId);
  const [activeTab, setActiveTab] = useState<string | undefined>(undefined);
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const navigate = useCallback((p: Page, id?: string | null, tab?: string) => {
    setPageState(p);
    setSelectedIdentityId(id ?? null);
    setActiveTab(tab);
    const hash = buildHash(p, id);
    if (window.location.hash !== hash) {
      window.location.hash = hash;
    }
  }, []);

  useEffect(() => {
    const onHashChange = () => {
      const { page: p, identityId } = parseHash();
      setPageState(p);
      setSelectedIdentityId(identityId);
    };
    window.addEventListener('hashchange', onHashChange);
    window.addEventListener('popstate', onHashChange);
    return () => {
      window.removeEventListener('hashchange', onHashChange);
      window.removeEventListener('popstate', onHashChange);
    };
  }, []);

  const navigateToDetail = (id: string) => navigate('identity-detail', id);
  const navigateBack = () => navigate('identities');

  const isActive = (item: NavItem) =>
    item.key === page || (item.match && item.match.includes(page));

  return (
    <div className="app">
      <a href="#main-content" className="sr-only">Skip to main content</a>
      <aside className={`sidebar ${sidebarOpen ? 'expanded' : 'collapsed'}`}>
        <div className="sidebar-header">
          <span className="sidebar-logo">
            <img src="/snowflake-logo.svg" alt="Snowflake" className="sidebar-logo-mark" width={26} height={26} />
            {sidebarOpen && <span className="sidebar-logo-text">Martech 360</span>}
          </span>
          <button
            className={`sidebar-toggle ${sidebarOpen ? '' : 'collapsed'}`}
            onClick={() => setSidebarOpen(!sidebarOpen)}
            title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
            aria-expanded={sidebarOpen}
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d={sidebarOpen ? "M10 12L6 8L10 4" : "M6 12L10 8L6 4"} stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </button>
        </div>

        <nav className="sidebar-nav" aria-label="Primary">
          {NAV_SECTIONS.map((section) => (
            <div key={section.title} className="sidebar-section" aria-labelledby={sidebarOpen ? `nav-section-${section.title.toLowerCase()}` : undefined}>
              {sidebarOpen && (
                <div className="sidebar-section-title" id={`nav-section-${section.title.toLowerCase()}`} role="presentation">{section.title}</div>
              )}
              {section.items.map((item) => {
                const Icon = item.icon;
                const active = isActive(item);
                return (
                  <button
                    key={item.key}
                    type="button"
                    className={`sidebar-btn ${active ? 'active' : ''}`}
                    onClick={() => navigate(item.key)}
                    title={sidebarOpen ? undefined : `${section.title}: ${item.label}`}
                    aria-current={active ? 'page' : undefined}
                  >
                    <span className="sidebar-btn-icon" aria-hidden>
                      <Icon size={SIDEBAR_ICON_SIZE} strokeWidth={SIDEBAR_ICON_STROKE} />
                    </span>
                    {sidebarOpen && <span className="sidebar-btn-label">{item.label}</span>}
                  </button>
                );
              })}
            </div>
          ))}
        </nav>

        <div className={`sidebar-footer${sidebarOpen ? ' sidebar-footer--expanded' : ''}`}>
          <DemoTour onNavigate={(p, id, tab) => navigate(p as Page, id, tab)} collapsed={!sidebarOpen} />
          <ThemeToggle collapsed={!sidebarOpen} />
          {sidebarOpen ? <span className="sidebar-footer-tagline">Martech Identity Resolution</span> : null}
        </div>
      </aside>

      <div className="app-body">
        <main className="app-main" id="main-content" tabIndex={-1}>
          {page === 'dashboard' && (
            <Dashboard onViewIdentities={() => navigate('identities')} />
          )}
          {page === 'data-to-identity' && (
            <Dashboard onViewIdentities={() => navigate('identities')} />
          )}
          {page === 'how-it-works' && (
            <Suspense
              fallback={
                <div className="hiw2-route-fallback">
                  <div className="loading">Loading flow diagram…</div>
                </div>
              }
            >
              <HowItWorksV2 />
            </Suspense>
          )}
          {page === 'idr-flow-animation' && (
            <Suspense
              fallback={
                <div className="hiw2-route-fallback">
                  <div className="loading">Loading IDR flow animation…</div>
                </div>
              }
            >
              <IdrFlowAnimationPage />
            </Suspense>
          )}
          {page === 'generate-data' && (
            <GenerateData
              onOpenRunHistory={() => navigate('run-history')}
              onOpenIdentities={() => navigate('identities')}
            />
          )}
          {page === 'identities' && <IdentityList onSelectIdentity={navigateToDetail} />}
          {page === 'identity-detail' && selectedIdentityId && (
            <IdentityDetailPage identityId={selectedIdentityId} onBack={navigateBack} activeTab={activeTab} />
          )}
          {page === 'run-history' && (
            <RunHistory
              onOpenGenerate={() => navigate('generate-data')}
              onOpenIdentities={() => navigate('identities')}
              onSelectIdentity={navigateToDetail}
              activeTab={activeTab}
            />
          )}
          {page === 'ai-review' && <AIReview onNavigateToIdentity={navigateToDetail} />}
          {page === 'cost-analysis' && <Dashboard onViewIdentities={() => navigate('identities')} />}
          {page === 'architecture' && <Dashboard onViewIdentities={() => navigate('identities')} />}
          {page === 'settings' && <Settings />}
          {page === 'test-runner' && <TestRunner />}
        </main>
        <footer className="app-footer" role="contentinfo">
          <div className="app-footer-inner">
            <span className="app-footer-brand">
              <img src="/snowflake-logo.svg" alt="" aria-hidden width={14} height={14} />
              <span>Martech 360 &middot; Identity Resolution on Snowflake</span>
            </span>
            <span className="app-footer-meta">
              <span>&copy; {new Date().getFullYear()} Snowflake Inc.</span>
              <span className="app-footer-sep" aria-hidden>·</span>
              <span>Demo build</span>
            </span>
          </div>
        </footer>
      </div>
    </div>
  );
}
