import { IdrFlowAnimation } from '../components/idr-flow/IdrFlowAnimation';

export default function IdrFlowAnimationPage() {
  return (
    <div className="how-it-works-v2-page">
      <header className="hiw2-page-header">
        <div className="hiw2-page-header__top">
          <div className="hiw2-page-header__titles">
            <div>
              <h1>Understand the IDR engine</h1>
              <p className="hiw2-page-lead">
                Watch records arrive and see how DET, FUZZY, CANON, ML, and LLM techniques
                cooperate to create, grow, and split clusters. Press Play to start.
              </p>
            </div>
          </div>
        </div>
      </header>
      <section aria-label="IDR record flow animation">
        <IdrFlowAnimation />
      </section>
    </div>
  );
}
