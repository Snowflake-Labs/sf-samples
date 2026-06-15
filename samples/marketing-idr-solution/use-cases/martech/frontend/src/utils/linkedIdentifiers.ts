import type { LinkedIdentifier } from '../api';

/** One row per distinct identifier value; types and sources aggregated. */
export interface AggregatedLinkedIdentifier {
  identifier_value: string;
  identifier_types: string[];
  source_systems: string[];
}

export function aggregateLinkedIdentifiersByValue(ids: LinkedIdentifier[]): AggregatedLinkedIdentifier[] {
  const map = new Map<string, { types: Set<string>; sources: Set<string> }>();
  for (const id of ids) {
    const key = id.identifier_value;
    if (!map.has(key)) {
      map.set(key, { types: new Set(), sources: new Set() });
    }
    const g = map.get(key)!;
    g.types.add(id.identifier_type);
    g.sources.add(id.source_system);
  }
  return Array.from(map.entries())
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([value, { types, sources }]) => ({
      identifier_value: value,
      identifier_types: Array.from(types).sort(),
      source_systems: Array.from(sources).sort(),
    }));
}
