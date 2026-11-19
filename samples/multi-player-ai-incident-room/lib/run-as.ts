/** Caller's rights only applies to a deployed app. The SDK ignores it in dev mode. */
export function callersRightsActive(): boolean {
  return process.env.NODE_ENV !== 'development';
}
