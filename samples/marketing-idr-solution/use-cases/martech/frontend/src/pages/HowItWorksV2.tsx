import { useEffect, useState } from 'react';
import { HowItWorksNarrativeSections } from '../components/HowItWorksNarrative';
import { IdrOverviewFlow } from '../components/IdrOverviewFlow';
import { IdrClusterDiagram } from '../components/IdrClusterDiagram';

export default function HowItWorksV2() {
  const [detailsOpen, setDetailsOpen] = useState(false);

  useEffect(() => {
    if (!detailsOpen) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setDetailsOpen(false);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [detailsOpen]);

  return (
    <div className="how-it-works-v2-page">
      <header className="hiw2-page-header">
        <div className="hiw2-page-header__top">
          <div className="hiw2-page-header__titles">
            <div>
              <h1>How IDR works</h1>
              <p className="hiw2-page-lead">
                Martech 360 Identity Resolution links retail/DTC POS, Loyalty, Shopify, and Web Clickstream
                records into clusters using a <strong>single resolution engine</strong> that combines{' '}
                <strong>deterministic</strong> rules (email, loyalty #, phone, device IDs),{' '}
                <strong>fuzzy</strong> name &amp; address matching, <strong>canonical</strong> nickname forms,
                an <strong>ML scorer</strong> for the long tail, and an <strong>LLM adjudicator</strong> on the
                borderline band where the model isn't sure.
              </p>
            </div>
          </div>
          <button type="button" className="btn-secondary hiw2-view-details-btn" onClick={() => setDetailsOpen(true)}>
            View Details
          </button>
        </div>
      </header>

      <section className="hiw2-flow-section" aria-label="IDR pipeline overview diagram">
        <IdrOverviewFlow />
      </section>

      <section className="hiw2-cluster-section" aria-label="IDR cluster formation diagram">
        <div className="hiw2-section-heading">
          <h2>Inside identity resolution: clusters &amp; rules</h2>
        </div>
        <IdrClusterDiagram />
      </section>

      {detailsOpen ? (
        <div className="modal-overlay" onClick={() => setDetailsOpen(false)} role="presentation">
          <div
            className="modal modal-hiw-details"
            role="dialog"
            aria-modal="true"
            aria-labelledby="hiw-details-title"
            onClick={e => e.stopPropagation()}
          >
            <div className="modal-header">
              <h2 id="hiw-details-title">How IDR works — details</h2>
              <button type="button" className="modal-close" aria-label="Close" onClick={() => setDetailsOpen(false)}>
                &times;
              </button>
            </div>
            <div className="modal-body modal-body-hiw-details">
              <div className="how-it-works-page hiw-modal-embed">
                <p className="hiw-lead">
                  Martech 360 Identity Resolution links retail/DTC POS, Loyalty, Shopify, and Web records into
                  clusters using a <strong>single resolution engine</strong>. The engine combines{' '}
                  <strong>deterministic</strong> rules (email, loyalty #, device IDs),{' '}
                  <strong>fuzzy</strong> name &amp; address matching, <strong>canonical</strong> nicknames, a
                  learned <strong>ML scorer</strong> for the long tail, and an <strong>LLM adjudicator</strong>{' '}
                  on the borderline band <code className="mono">[0.55, 0.85)</code> where Cortex AI returns a
                  MATCH/NO_MATCH verdict with reasoning.
                </p>
                <HowItWorksNarrativeSections />
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
