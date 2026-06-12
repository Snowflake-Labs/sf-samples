export type ThemePreference = 'light' | 'dark';

export const THEME_STORAGE_KEY = 'martech-theme-preference';

export function readStoredPreference(): ThemePreference {
  if (typeof window === 'undefined') return 'light';
  const v = localStorage.getItem(THEME_STORAGE_KEY);
  if (v === 'light' || v === 'dark') return v;
  return 'light';
}

export function writeStoredPreference(p: ThemePreference) {
  localStorage.setItem(THEME_STORAGE_KEY, p);
}

export function systemPrefersDark(): boolean {
  return typeof window !== 'undefined' && window.matchMedia('(prefers-color-scheme: dark)').matches;
}

export function resolveTheme(preference: ThemePreference): 'light' | 'dark' {
  return preference;
}

export function applyDomTheme(resolved: 'light' | 'dark') {
  document.documentElement.setAttribute('data-theme', resolved);
}
