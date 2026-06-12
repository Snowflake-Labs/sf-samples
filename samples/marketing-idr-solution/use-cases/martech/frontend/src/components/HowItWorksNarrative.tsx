import { Cpu, GitBranch, Layers, Sparkles } from 'lucide-react';

type RuleTier = 'deterministic' | 'ml' | 'llm';

export const MARTECH_RULES: { id: string; name: string; summary: string; tier: RuleTier }[] = [
  { id: 'R01', name: 'Email Exact', summary: 'Same lowercased+trimmed email across records.', tier: 'deterministic' },
  { id: 'R02', name: 'HEM Exact', summary: 'Same SHA-256 hashed email — privacy-safe deterministic.', tier: 'deterministic' },
  { id: 'R03', name: 'Loyalty Member # Exact', summary: 'Same loyalty number — first-party deterministic anchor.', tier: 'deterministic' },
  { id: 'R04', name: 'Device ID Exact', summary: 'Same device ID (cookie / IDFA / GAID).', tier: 'deterministic' },
  { id: 'R05', name: 'UID2 Exact', summary: 'Same UID2 token — addressable deterministic.', tier: 'deterministic' },
  { id: 'R06', name: 'RampID Exact', summary: 'Same LiveRamp RampID across sources.', tier: 'deterministic' },
  { id: 'R07', name: 'Full Name + Phone', summary: 'Anchor on phone, exact match on first + last name.', tier: 'deterministic' },
  { id: 'R08', name: 'Nickname First + Email', summary: 'Email anchor; nickname-canonical first (Bob↔Robert) + exact last.', tier: 'deterministic' },
  { id: 'R09', name: 'Nickname First + Phone', summary: 'Phone anchor; nickname-canonical first + exact last.', tier: 'deterministic' },
  { id: 'R10', name: 'Fuzzy First + Email', summary: 'Email anchor; Jaro-Winkler first ≥ 0.92, exact last.', tier: 'deterministic' },
  { id: 'R11', name: 'First + Fuzzy Last + Email', summary: 'Email anchor; exact first, Jaro-Winkler last ≥ 0.92.', tier: 'deterministic' },
  { id: 'R12', name: 'Fuzzy First + Phone', summary: 'Phone anchor; Jaro-Winkler first ≥ 0.92, exact last.', tier: 'deterministic' },
  { id: 'R13', name: 'First + Fuzzy Last + Phone', summary: 'Phone anchor; exact first, Jaro-Winkler last ≥ 0.92.', tier: 'deterministic' },
  { id: 'R14', name: 'Last Name + Full Address', summary: 'Postal anchor; exact last name + full address — household-level deterministic.', tier: 'deterministic' },
  { id: 'R15', name: 'Fuzzy Last Name + Full Address', summary: 'Postal anchor; Jaro-Winkler last ≥ 0.92 — household with name typos.', tier: 'deterministic' },
  { id: 'R16', name: 'ML Candidate Score ≥ 0.85', summary: 'LightGBM-style scorer over name/address/phone fuzzy features.', tier: 'ml' },
  { id: 'R17', name: 'LLM Adjudicated MATCH', summary: 'Cortex AI verdict = MATCH on candidate pairs in band [0.55, 0.85).', tier: 'llm' },
];

const TIER_LABEL: Record<RuleTier, string> = {
  deterministic: 'DET',
  ml: 'ML',
  llm: 'LLM',
};

/** Narrative sections from the classic "How it works" page (no page header). */
export function HowItWorksNarrativeSections() {
  return (
    <>
      <section className="hiw-section hiw-callout">
        <h2>
          <Layers size={20} strokeWidth={2} aria-hidden />
          Single resolution engine
        </h2>
        <p>
          The IDR engine is one logical unit for individual resolution. It evaluates identifier pairs and
          composes match edges using five techniques that work together rather than as ordered passes:{' '}
          <strong>deterministic</strong> anchors (email, HEM, loyalty #, phone, device IDs, UID2, RampID),{' '}
          <strong>fuzzy</strong> name &amp; address matching (Jaro-Winkler ≥ 0.92 with email/phone/postal
          anchors), <strong>canonical</strong> nicknames (Bob ↔ Robert), an <strong>ML scorer</strong>{' '}
          (LightGBM-style classifier, score ≥ 0.85), and an <strong>LLM adjudicator</strong> (Cortex AI
          MATCH/NO_MATCH on the borderline band <code className="mono">[0.55, 0.85)</code>). Every link names
          the rule that fired (e.g. "Loyalty Member # Exact").
        </p>
      </section>

      <section className="hiw-section">
        <h2>
          <GitBranch size={20} strokeWidth={2} aria-hidden />
          Deterministic rules
        </h2>
        <p>
          Rules are stored in Snowflake as rows in <code className="mono">IDR_MATCHING_RULES</code> (seeded in{' '}
          <code className="mono">sql/config/matching_rules_seed.sql</code>). Each rule has priority, anchor field(s),
          a match type (<code className="mono">DETERMINISTIC</code>, <code className="mono">ML</code>, or{' '}
          <code className="mono">LLM</code>), and a base score. The matching job joins identifiers on those anchors
          (exact, nickname-canonical, or fuzzy patterns) and writes edges the cluster builder consumes.
        </p>
        <p className="hiw-muted">All 17 rules in the demo (mirrors the config seed):</p>
        <ul className="hiw-rule-list">
          {MARTECH_RULES.map(r => (
            <li key={r.id}>
              <span className={`hiw2-pill hiw2-pill--rule-${r.tier}`}>{TIER_LABEL[r.tier]}</span>
              <span className="hiw-rule-name">{r.id} · {r.name}</span>
              <span className="hiw-rule-desc">{r.summary}</span>
            </li>
          ))}
        </ul>
      </section>

      <section className="hiw-section">
        <h2>
          <Cpu size={20} strokeWidth={2} aria-hidden />
          ML candidate scoring
        </h2>
        <p>
          The ML layer targets <strong>candidate pairs</strong> that don't share a strong deterministic anchor but
          look like they might be the same person — typos in names, abbreviated street addresses, a phone digit
          off, etc. It does <strong>not</strong> override deterministic merges that fired on email / loyalty / device.
        </p>
        <ul className="hiw-bullet-list">
          <li>
            <strong>Candidate generation</strong> (<code className="mono">SP_GENERATE_ML_CANDIDATES</code>): name +
            postal-code blocking groups records that aren't already linked deterministically. Each unmerged pair
            inside a block becomes a candidate.
          </li>
          <li>
            <strong>Features</strong> (<code className="mono">v_IDR_ML_PAIR_FEATURES</code>): Jaro-Winkler on first
            and last name, address-token overlap, phone-digit edit distance, channel overlap (POS / Web / Shopify
            / Loyalty), geo cohabitation, transaction recency, identifier-type richness.
          </li>
          <li>
            <strong>Scoring</strong> (<code className="mono">SP_ML_SCORE_CANDIDATES</code>): a LightGBM-style
            classifier produces a probability. R16 emits an edge when the score is ≥ 0.85.{' '}
            <code className="mono">SCORE_DETAILS</code> stores the feature contributions for explainability.
          </li>
        </ul>
      </section>

      <section className="hiw-section">
        <h2>
          <Sparkles size={20} strokeWidth={2} aria-hidden />
          LLM adjudication
        </h2>
        <p>
          R17 picks up candidate pairs whose ML score lands in the borderline band{' '}
          <code className="mono">[0.55, 0.85)</code> — strong enough that a human reviewer would want to look,
          weak enough that auto-merging is risky. The pair plus its identifiers and key features are sent to
          Cortex AI; the verdict (<code className="mono">MATCH</code> / <code className="mono">NO_MATCH</code>),
          confidence, and short rationale are persisted on the match record. MATCH verdicts produce an edge at
          the LLM tier; NO_MATCH verdicts are kept as audit trail and prevent re-evaluation.
        </p>
      </section>
    </>
  );
}
