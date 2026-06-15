import { Moon, Sun } from 'lucide-react';
import { useTheme } from '../theme';

const BTN = 'theme-toggle__btn';
const ICON = 16;
const STROKE = 2;

export function ThemeToggle({ collapsed }: { collapsed: boolean }) {
  const { preference, setPreference, cyclePreference } = useTheme();

  if (collapsed) {
    const title = preference === 'light' ? 'Theme: Light' : 'Theme: Dark';
    return (
      <button
        type="button"
        className={`${BTN} theme-toggle__btn--solo`}
        onClick={cyclePreference}
        title={title}
        aria-label={title}
      >
        {preference === 'dark' ? (
          <Moon size={ICON} strokeWidth={STROKE} />
        ) : (
          <Sun size={ICON} strokeWidth={STROKE} />
        )}
      </button>
    );
  }

  return (
    <div className="theme-toggle" role="group" aria-label="Color theme">
      <span className="theme-toggle__label">Theme</span>
      <div className="theme-toggle__row">
        <button
          type="button"
          className={`${BTN}${preference === 'light' ? ' is-active' : ''}`}
          onClick={() => setPreference('light')}
          title="Light"
          aria-pressed={preference === 'light'}
        >
          <Sun size={ICON} strokeWidth={STROKE} />
        </button>
        <button
          type="button"
          className={`${BTN}${preference === 'dark' ? ' is-active' : ''}`}
          onClick={() => setPreference('dark')}
          title="Dark"
          aria-pressed={preference === 'dark'}
        >
          <Moon size={ICON} strokeWidth={STROKE} />
        </button>
      </div>
    </div>
  );
}
