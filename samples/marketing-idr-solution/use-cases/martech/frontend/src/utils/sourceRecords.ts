import type { SourceRecordDetail, SourceRecordIdentifier } from '../api';

/** Human-readable labels for common SSP / graph identifier types */
const TYPE_LABELS: Record<string, string> = {
  HEM: 'Hashed email (HEM)',
  UID2: 'Unified ID 2.0',
  RAMPID: 'RampID',
  PPID: 'Publisher person ID',
  DEVICE_ID: 'Device ID',
  COOKIE: 'Cookie / device ID',
  GEO_COUNTRY: 'Country',
  GEO_METRO: 'Metro',
  GEO_ZIP: 'Postal code',
  GEO_LAT: 'Latitude',
  GEO_LON: 'Longitude',
  TAPAD_PERSON: 'Tapad person',
  TAPAD_ID: 'Tapad ID',
  EXPERIAN_ID: 'Experian ID',
  EXPERIAN_PERSON: 'Experian person',
  ID_VALUE: 'Graph ID value',
};

const TYPE_PRIORITY = [
  'HEM',
  'UID2',
  'RAMPID',
  'PPID',
  'DEVICE_ID',
  'COOKIE',
  'GEO_COUNTRY',
  'GEO_METRO',
  'GEO_ZIP',
  'TAPAD_PERSON',
  'EXPERIAN_PERSON',
];

export function humanizeIdentifierType(type: string): string {
  return TYPE_LABELS[type] ?? type.replace(/_/g, ' ');
}

export function friendlySourceTitle(sourceType: string): string {
  const m: Record<string, string> = {
    SSP_BID_REQUEST: 'Bid request',
    TAPAD_GRAPH: 'Tapad graph',
    EXPERIAN_GRAPH: 'Experian graph',
  };
  return m[sourceType] ?? sourceType.replace(/_/g, ' ');
}

/** Sort types: important IDs first, then alphabetical */
export function sortIdentifierTypes(types: string[]): string[] {
  const rank = (t: string) => {
    const i = TYPE_PRIORITY.indexOf(t);
    return i === -1 ? 1000 + t.charCodeAt(0) : i;
  };
  return [...types].sort((a, b) => rank(a) - rank(b) || a.localeCompare(b));
}

export function getIdentifierValues(record: SourceRecordDetail, type: string): string[] {
  return record.identifiers.filter((id) => id.type === type).map((id) => id.value);
}

export type FieldCompareState = 'aligned' | 'overlap' | 'split' | 'sparse' | 'only_one';

export function fieldCompareState(type: string, records: SourceRecordDetail[]): FieldCompareState {
  const valueSets = records.map((r) => getIdentifierValues(r, type).filter(Boolean));
  const withData = valueSets.filter((vs) => vs.length > 0);
  if (withData.length === 0) return 'sparse';
  const normalized = withData.map((vs) => [...vs].sort().join(' · '));
  if (withData.length === 1) return 'only_one';
  if (new Set(normalized).size === 1) return 'aligned';

  for (let i = 0; i < records.length; i++) {
    const a = new Set(getIdentifierValues(records[i], type));
    if (a.size === 0) continue;
    for (let j = i + 1; j < records.length; j++) {
      const b = new Set(getIdentifierValues(records[j], type));
      for (const v of a) {
        if (b.has(v)) return 'overlap';
      }
    }
  }
  return 'split';
}

export function compareSummary(records: SourceRecordDetail[]) {
  const types = new Set<string>();
  records.forEach((r) => r.identifiers.forEach((id) => types.add(id.type)));
  const sorted = sortIdentifierTypes(Array.from(types));
  let aligned = 0;
  let overlap = 0;
  let split = 0;
  let sparse = 0;
  let onlyOne = 0;
  for (const t of sorted) {
    const st = fieldCompareState(t, records);
    if (st === 'aligned') aligned++;
    else if (st === 'overlap') overlap++;
    else if (st === 'split') split++;
    else if (st === 'sparse') sparse++;
    else if (st === 'only_one') onlyOne++;
  }
  return { aligned, overlap, split, sparse, onlyOne, totalTypes: sorted.length };
}

export function formatRawPayload(raw: string | null | undefined): string {
  if (raw == null || raw === '') return '';
  try {
    const parsed = JSON.parse(raw) as unknown;
    return JSON.stringify(parsed, null, 2);
  } catch {
    return raw;
  }
}

export function summaryRowsFromIdentifiers(ids: SourceRecordIdentifier[]): { label: string; value: string; type: string }[] {
  const order = sortIdentifierTypes([...new Set(ids.map((i) => i.type))]);
  const out: { label: string; value: string; type: string }[] = [];
  for (const t of order) {
    const vals = ids.filter((i) => i.type === t).map((i) => i.value);
    if (vals.length === 0) continue;
    out.push({
      type: t,
      label: humanizeIdentifierType(t),
      value: vals.join(', '),
    });
  }
  return out;
}
