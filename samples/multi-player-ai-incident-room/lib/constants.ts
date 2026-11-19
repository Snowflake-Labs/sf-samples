/** App title — displayed in the nav header and browser tab */
export const APP_TITLE = 'Multi-player AI Incident Room';

/** Path to the logo in /public (used in the header and as favicon) */
export const LOGO_SRC = '/icon.svg';

/** Pages listed under "Go to" in the ⌘K command palette. */
export const NAV_ITEMS: { label: string; href: string }[] = [{ label: 'Incidents', href: '/' }];

/** Suggested asks shown in an empty room. */
export const EXAMPLE_PROMPTS = [
  '@triage what broke, and what is affected?',
  '@fixer propose a fix and prove it on a clone',
  '@scribe give us a five-line status',
];
