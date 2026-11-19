import { afterEach, describe, expect, it, vi } from 'vitest';
import { callersRightsActive } from '../../lib/run-as';

afterEach(() => vi.unstubAllEnvs());

describe('callersRightsActive', () => {
  it('is off in local dev, where there is no caller token', () => {
    vi.stubEnv('NODE_ENV', 'development');
    expect(callersRightsActive()).toBe(false);
  });

  it('is on when deployed', () => {
    vi.stubEnv('NODE_ENV', 'production');
    expect(callersRightsActive()).toBe(true);
  });
});
