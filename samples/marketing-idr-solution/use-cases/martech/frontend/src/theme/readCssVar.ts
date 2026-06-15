/** Read a custom property from :root (trimmed). Empty string if unset. */
export function readCssVar(name: string, el: HTMLElement = document.documentElement): string {
  return getComputedStyle(el).getPropertyValue(name).trim();
}
