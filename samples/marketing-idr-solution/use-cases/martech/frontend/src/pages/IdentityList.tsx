import { useState, useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Filter, X } from 'lucide-react';
import {
  fetchIdentities,
  fetchIdentityFilterSources,
  type IdentitySortField,
  type IdentityConfidenceBand,
  type IdentityConnectedFilter,
} from '../api';
import CopyButton from '../components/CopyButton';

const PAGE_SIZE_OPTIONS = [10, 25, 50, 100] as const;

export default function IdentityList({ onSelectIdentity }: { onSelectIdentity: (id: string) => void }) {
  const [searchInput, setSearchInput] = useState('');
  const [debouncedQ, setDebouncedQ] = useState('');
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);
  const [sortField, setSortField] = useState<IdentitySortField>('confidence');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [confidenceBand, setConfidenceBand] = useState<IdentityConfidenceBand | ''>('');
  const [connectedFilter, setConnectedFilter] = useState<IdentityConnectedFilter | ''>('');
  /** source_type values (OR) from /api/identity-filter-sources */
  const [selectedSourceTypes, setSelectedSourceTypes] = useState<string[]>([]);
  const [minDevices, setMinDevices] = useState<string>('');
  const [filtersOpen, setFiltersOpen] = useState(false);

  useEffect(() => {
    const id = window.setTimeout(() => setDebouncedQ(searchInput.trim()), 350);
    return () => clearTimeout(id);
  }, [searchInput]);

  const sourcesFilterKey = [...selectedSourceTypes].sort().join('|');

  useEffect(() => {
    setPage(1);
  }, [debouncedQ, confidenceBand, connectedFilter, sourcesFilterKey, minDevices]);

  const { data: sourceFilterMeta, isLoading: sourceOptionsLoading } = useQuery({
    queryKey: ['identity-filter-sources'],
    queryFn: fetchIdentityFilterSources,
    staleTime: 5 * 60_000,
  });

  const filterActive =
    confidenceBand !== '' || connectedFilter !== '' || selectedSourceTypes.length > 0 || minDevices !== '';

  const identityListQueryKey = [
    'identities',
    debouncedQ,
    page,
    pageSize,
    sortField,
    sortDir,
    confidenceBand,
    connectedFilter,
    sourcesFilterKey,
    minDevices,
  ] as const;

  const { data, isLoading, isFetching } = useQuery({
    queryKey: identityListQueryKey,
    queryFn: ({ signal }) =>
      fetchIdentities(
        {
          q: debouncedQ || undefined,
          page,
          pageSize,
          sortBy: sortField,
          sortDir,
          confidenceBand: confidenceBand || undefined,
          connectedClusters: connectedFilter || undefined,
          filterSourceTypes: selectedSourceTypes.length > 0 ? selectedSourceTypes : undefined,
          minDevices: minDevices === '' ? '' : Number(minDevices),
        },
        { signal },
      ),
    // keepPreviousData for the whole key made filtered views show the unfiltered result until Snowflake returned.
    placeholderData: (previousData, previousQuery) => {
      if (!previousData || previousQuery == null) return undefined;
      const prevKey = previousQuery.queryKey;
      if (!Array.isArray(prevKey) || prevKey[0] !== 'identities') return undefined;
      const cur = identityListQueryKey as unknown as unknown[];
      if (prevKey.length !== cur.length) return undefined;
      for (let i = 0; i < cur.length; i++) {
        if (i === 2) continue; // page only — smooth pagination
        if (prevKey[i] !== cur[i]) return undefined;
      }
      return previousData;
    },
  });

  useEffect(() => {
    if (!data) return;
    const maxPage = Math.max(1, Math.ceil(data.total / data.page_size));
    if (page > maxPage) setPage(maxPage);
  }, [data, page]);

  const handleSort = (field: IdentitySortField) => {
    setPage(1);
    if (sortField === field) {
      setSortDir(d => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortField(field);
      setSortDir(['display_name', 'identity_id'].includes(field) ? 'asc' : 'desc');
    }
  };

  const totalPages = data ? Math.max(1, Math.ceil(data.total / data.page_size)) : 1;
  const rangeFrom =
    data && data.total > 0 ? (data.page - 1) * data.page_size + 1 : 0;
  const rangeTo = data && data.total > 0 ? rangeFrom + data.identities.length - 1 : 0;

  return (
    <div className="identity-list-page">
      <div className="list-header">
        <h1>Identities</h1>
        {data && <span className="count-badge">{data.total.toLocaleString()} total</span>}
      </div>

      <div className="list-controls identity-list-toolbar">
        <input
          type="text"
          className="search-input"
          placeholder="Search by ID, Email, Phone, Loyalty#, Device ID, UID2, RampID..."
          value={searchInput}
          onChange={e => setSearchInput(e.target.value)}
        />
        <button
          type="button"
          className={`identity-filter-toggle ${filtersOpen ? 'is-open' : ''} ${filterActive ? 'has-active' : ''}`}
          aria-expanded={filtersOpen}
          aria-controls="identity-filters-drawer"
          onClick={() => setFiltersOpen(o => !o)}
        >
          <Filter size={16} strokeWidth={1.5} />
          <span>Filters</span>
          {filterActive && <span className="identity-filter-toggle-dot" aria-hidden />}
        </button>
      </div>

      {filtersOpen && (
      <div
        id="identity-filters-drawer"
        className="identity-filters identity-filters--drawer"
        role="group"
        aria-label="Filter identities"
      >
        <div className="identity-filters-drawer-header">
          <span className="identity-filters-label">Filters</span>
          <button
            type="button"
            className="identity-filters-close"
            aria-label="Close filters"
            onClick={() => setFiltersOpen(false)}
          >
            <X size={16} strokeWidth={1.5} />
          </button>
        </div>
        <label className="identity-filter-field">
          <span className="identity-filter-field-label">Confidence</span>
          <select
            className="identity-filter-select"
            value={confidenceBand}
            onChange={e => setConfidenceBand(e.target.value as IdentityConfidenceBand | '')}
          >
            <option value="">All</option>
            <option value="high">High (≥90%)</option>
            <option value="medium">Medium (60–90%)</option>
            <option value="low">Low (&lt;60%)</option>
          </select>
        </label>
        <label className="identity-filter-field">
          <span className="identity-filter-field-label">Connected clusters</span>
          <select
            className="identity-filter-select"
            value={connectedFilter}
            onChange={e => setConnectedFilter(e.target.value as IdentityConnectedFilter | '')}
          >
            <option value="">All</option>
            <option value="yes">Has connections</option>
            <option value="no">None</option>
          </select>
        </label>
        <div className="identity-filter-field identity-filter-field--sources">
          <span className="identity-filter-field-label" id="identity-sources-filter-label">
            Sources
          </span>
          <p className="identity-filter-sources-hint">
            Ingested data sources. Identity must have links from <strong>all</strong> selected sources (AND).
          </p>
          {sourceOptionsLoading && <span className="identity-filter-sources-loading">Loading sources…</span>}
          {!sourceOptionsLoading &&
            (sourceFilterMeta?.sources.length ?? 0) === 0 && (
              <span className="identity-filter-sources-empty">
                No source tables are configured yet. Refresh your catalog metadata if you expect sources here.
              </span>
            )}
          {!sourceOptionsLoading &&
            sourceFilterMeta &&
            sourceFilterMeta.sources.length > 0 &&
            sourceFilterMeta.ui_mode === 'checkbox' && (
              <div
                className="identity-source-checkboxes"
                role="group"
                aria-labelledby="identity-sources-filter-label"
              >
                {sourceFilterMeta.sources.map(opt => (
                  <label key={opt.value} className="identity-source-checkbox">
                    <input
                      type="checkbox"
                      checked={selectedSourceTypes.includes(opt.value)}
                      onChange={e => {
                        setPage(1);
                        setSelectedSourceTypes(prev => {
                          if (e.target.checked) {
                            if (prev.includes(opt.value)) return prev;
                            return [...prev, opt.value];
                          }
                          return prev.filter(v => v !== opt.value);
                        });
                      }}
                    />
                    <span title={opt.value}>{opt.label}</span>
                  </label>
                ))}
              </div>
            )}
          {!sourceOptionsLoading &&
            sourceFilterMeta &&
            sourceFilterMeta.sources.length > 0 &&
            sourceFilterMeta.ui_mode === 'multiselect' && (
              <select
                multiple
                className="identity-filter-select identity-filter-multiselect"
                aria-labelledby="identity-sources-filter-label"
                size={Math.min(10, Math.max(4, sourceFilterMeta.sources.length))}
                value={selectedSourceTypes}
                onChange={e => {
                  setPage(1);
                  setSelectedSourceTypes(Array.from(e.target.selectedOptions, o => o.value));
                }}
              >
                {sourceFilterMeta.sources.map(opt => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            )}
        </div>
        <label className="identity-filter-field">
          <span className="identity-filter-field-label">Min devices</span>
          <select className="identity-filter-select" value={minDevices} onChange={e => setMinDevices(e.target.value)}>
            <option value="">Any</option>
            <option value="1">1+</option>
            <option value="2">2+</option>
            <option value="3">3+</option>
          </select>
        </label>
        {filterActive && (
          <button
            type="button"
            className="identity-filters-clear"
            onClick={() => {
              setConfidenceBand('');
              setConnectedFilter('');
              setSelectedSourceTypes([]);
              setMinDevices('');
              setPage(1);
            }}
          >
            Clear filters
          </button>
        )}
      </div>
      )}

      {isLoading && !data && <div className="loading loading-page">Loading...</div>}

      {data && (
        <>
          <div className="identity-list-meta">
            <div className="identity-list-meta-left">
              <span className="showing-label">
                {data.total === 0
                  ? 'No matches'
                  : `Showing ${rangeFrom.toLocaleString()}–${rangeTo.toLocaleString()} of ${data.total.toLocaleString()}`}
              </span>
              {isFetching && data && <span className="identity-list-refreshing">Updating…</span>}
            </div>
            <label className="page-size-label">
              <span className="page-size-label-text">Rows per page</span>
              <select
                className="page-size-select"
                value={pageSize}
                onChange={e => {
                  setPage(1);
                  setPageSize(Number(e.target.value));
                }}
              >
                {PAGE_SIZE_OPTIONS.map(n => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className={`identity-table-wrap ${isFetching ? 'identity-table-wrap--dim' : ''}`}>
            <table className="identity-table full">
              <thead>
                <tr>
                  <SortableHeader
                    label="Display name"
                    field="display_name"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                  />
                  <th>Primary IDs</th>
                  <SortableHeader
                    label="Sources"
                    field="source_count"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                  />
                  <SortableHeader
                    label="Household"
                    field="household_id"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                    align="center"
                  />
                  <SortableHeader
                    label="Connected clusters"
                    field="connected_clusters"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                    align="center"
                  />
                  <SortableHeader
                    label="Confidence"
                    field="confidence"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                    align="center"
                  />
                  <SortableHeader
                    label="Last seen"
                    field="last_seen"
                    activeField={sortField}
                    sortDir={sortDir}
                    onSort={handleSort}
                  />
                </tr>
              </thead>
              <tbody>
                {data.identities.map(ip => (
                  <tr key={ip.identity_id} onClick={() => onSelectIdentity(ip.identity_id)} className="clickable-row">
                    <td className="identity-name-cell">
                      <div className="display-name" title={ip.display_name || ip.identity_id}>
                        {ip.display_name || ip.identity_id.slice(0, 12)}
                      </div>
                      <div className="identity-id-row">
                        <span className="identity-id-trunc mono" title={ip.identity_id}>
                          {ip.identity_id}
                        </span>
                        <CopyButton value={ip.identity_id} />
                      </div>
                    </td>
                    <td>
                      <div className="id-pills">
                        {ip.primary_hem && <span className="pill hem">EMAIL</span>}
                        {ip.primary_uid2 && <span className="pill uid2">UID2</span>}
                        {ip.primary_rampid && <span className="pill rampid">RampID</span>}
                        {ip.primary_ppid && <span className="pill ppid">LOYALTY</span>}
                        {ip.primary_device_id && <span className="pill device">Device</span>}
                        {ip.primary_cookie && <span className="pill cookie">Cookie</span>}
                      </div>
                    </td>
                    <td className="sources-cell">
                      <div
                        className="sources-stack"
                        title={
                          [
                            ip.source_types?.length ? ip.source_types.join(', ') : null,
                            ip.source_count != null
                              ? `${ip.source_count} source record${ip.source_count === 1 ? '' : 's'}`
                              : null,
                          ]
                            .filter(Boolean)
                            .join(' · ') || 'Sources'
                        }
                      >
                        {(ip.source_types ?? []).length > 0 && (
                          <div className="sources-icons-row">
                            {(ip.source_types ?? []).map(st => (
                              <span
                                key={st}
                                className={`source-initial ${sourceTypeCssClass(st)}`}
                                title={st}
                              >
                                {sourceTypeInitial(st)}
                              </span>
                            ))}
                          </div>
                        )}
                        <div className="sources-meta-line">
                          {ip.source_count != null ? (
                            <>
                              <span className="sources-meta-count">{ip.source_count}</span>
                              <span className="sources-meta-suffix">
                                {' '}
                                linked record{ip.source_count === 1 ? '' : 's'}
                              </span>
                            </>
                          ) : (
                            <span className="sources-meta-muted">—</span>
                          )}
                        </div>
                      </div>
                    </td>
                    <td style={{ textAlign: 'center' }}>
                      {ip.household_id ? (
                        <span title={`Household ${ip.household_id}`}>Y</span>
                      ) : (
                        <span className="muted">—</span>
                      )}
                    </td>
                    <td style={{ textAlign: 'center' }} title="ML weak-link candidate pairs touching this cluster">
                      {ip.connected_cluster_count != null ? ip.connected_cluster_count : '—'}
                    </td>
                    <td style={{ textAlign: 'center' }}>
                      <span className={`confidence ${getConfidenceClass(ip.confidence_score)}`}>
                        {ip.confidence_score != null ? `${(ip.confidence_score * 100).toFixed(0)}%` : '—'}
                      </span>
                    </td>
                    <td>{ip.last_seen ? new Date(ip.last_seen).toLocaleDateString() : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {data.total > 0 && (
            <nav className="identity-list-pagination" aria-label="Identity list pages">
              <button
                type="button"
                className="btn-secondary pagination-btn"
                disabled={page <= 1 || isFetching}
                onClick={() => setPage(p => Math.max(1, p - 1))}
              >
                Previous
              </button>
              <span className="pagination-status">
                Page <strong>{data.page}</strong> of {totalPages.toLocaleString()}
              </span>
              <button
                type="button"
                className="btn-secondary pagination-btn"
                disabled={page >= totalPages || isFetching}
                onClick={() => setPage(p => Math.min(totalPages, p + 1))}
              >
                Next
              </button>
            </nav>
          )}
        </>
      )}
    </div>
  );
}

function SortableHeader({
  label,
  field,
  activeField,
  sortDir,
  onSort,
  align,
}: {
  label: string;
  field: IdentitySortField;
  activeField: IdentitySortField;
  sortDir: 'asc' | 'desc';
  onSort: (f: IdentitySortField) => void;
  align?: 'left' | 'right' | 'center';
}) {
  const active = activeField === field;
  const alignClass = align === 'right' ? 'numeric-cell' : align === 'center' ? 'center-cell' : '';
  return (
    <th
      className={`sortable-header ${alignClass}`}
      style={align === 'center' ? { textAlign: 'center' } : undefined}
      aria-sort={active ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'}
    >
      <button
        type="button"
        className="sortable-header-btn"
        style={align === 'center' ? { display: 'flex', justifyContent: 'center', width: '100%' } : undefined}
        aria-label={`Sort by ${label}`}
        onClick={() => onSort(field)}
      >
        <span>{label}</span>
        <span className="sort-indicator" aria-hidden>
          {active ? (sortDir === 'asc' ? '↑' : '↓') : ''}
        </span>
      </button>
    </th>
  );
}

function getConfidenceClass(score: number | null): string {
  if (score == null) return '';
  if (score >= 0.95) return 'high';
  if (score >= 0.85) return 'medium';
  return 'low';
}

function sourceTypeCssClass(sourceType: string): string {
  return sourceType.toLowerCase().replace(/_/g, '-');
}

function sourceTypeInitial(sourceType: string): string {
  const m = sourceType.match(/[A-Za-z0-9]/);
  return m ? m[0].toUpperCase() : '?';
}
