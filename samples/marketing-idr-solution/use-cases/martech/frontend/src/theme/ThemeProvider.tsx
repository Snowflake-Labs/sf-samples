import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from 'react';
import {
  applyDomTheme,
  readStoredPreference,
  type ThemePreference,
  writeStoredPreference,
} from './storage';

type ThemeContextValue = {
  preference: ThemePreference;
  resolved: 'light' | 'dark';
  setPreference: (p: ThemePreference) => void;
  cyclePreference: () => void;
};

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [preference, setPreferenceState] = useState<ThemePreference>(() => readStoredPreference());

  const resolved = preference;

  useEffect(() => {
    applyDomTheme(resolved);
  }, [resolved]);

  const setPreference = useCallback((p: ThemePreference) => {
    writeStoredPreference(p);
    setPreferenceState(p);
  }, []);

  const cyclePreference = useCallback(() => {
    setPreference(preference === 'light' ? 'dark' : 'light');
  }, [preference, setPreference]);

  const value = useMemo(
    () => ({ preference, resolved, setPreference, cyclePreference }),
    [preference, resolved, setPreference, cyclePreference],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error('useTheme must be used within ThemeProvider');
  return ctx;
}

/** Resolved appearance for charts / listeners (light | dark). */
export function useResolvedTheme() {
  return useTheme().resolved;
}
