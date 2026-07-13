const numberFormatter = new Intl.NumberFormat('en-US');
const compactFormatter = new Intl.NumberFormat('en-US', {
  notation: 'compact',
  maximumFractionDigits: 1,
});
const currencyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
});
const preciseCurrencyFormatter = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const driftValueFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 3,
});
const dateFormatter = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit',
  month: 'short',
  year: 'numeric',
  timeZone: 'UTC',
});
const dateTimeFormatter = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  hour12: false,
});

export function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

export function buildQuery(filters = {}) {
  const params = new URLSearchParams();
  const allowed = ['start_date', 'end_date', 'zone_id', 'rider_type'];

  for (const key of allowed) {
    const value = filters[key];
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      params.set(key, String(value).trim());
    }
  }

  const query = params.toString();
  return query ? `?${query}` : '';
}

export function createRequestGate() {
  const generations = new Map();

  return {
    begin(scope) {
      const generation = (generations.get(scope) || 0) + 1;
      generations.set(scope, generation);
      return generation;
    },
    isCurrent(scope, generation) {
      return generations.get(scope) === generation;
    },
  };
}

export function reconcileActiveFilters(metadata = {}, activeFilters = {}) {
  const nextFilters = { ...activeFilters };
  const zoneIds = new Set((metadata.zones || []).map((zone) => String(zone.zone_id)));
  const riderTypes = new Set((metadata.rider_types || []).map(String));

  if (nextFilters.zone_id && !zoneIds.has(String(nextFilters.zone_id))) delete nextFilters.zone_id;
  if (nextFilters.rider_type && !riderTypes.has(String(nextFilters.rider_type))) {
    delete nextFilters.rider_type;
  }
  for (const key of ['start_date', 'end_date']) {
    const value = nextFilters[key];
    if (
      value &&
      ((metadata.start_date && value < metadata.start_date) ||
        (metadata.end_date && value > metadata.end_date))
    ) {
      delete nextFilters[key];
    }
  }

  return nextFilters;
}

export function chartGeometry(
  points,
  valueKey,
  { width = 760, height = 184, padding = { top: 12, right: 18, bottom: 30, left: 58 } } = {},
) {
  if (!Array.isArray(points) || points.length === 0) {
    return { path: '', maxValue: 0, xTicks: [], yTicks: [] };
  }

  const parsed = points.map((point, index) => ({
    timestamp: Number.isFinite(Date.parse(point.trip_hour)) ? Date.parse(point.trip_hour) : index,
    value: Math.max(0, Number(point[valueKey]) || 0),
    point,
  }));
  const maxValue = Math.max(...parsed.map((entry) => entry.value), 0);
  const scaleMax = Math.max(maxValue, 1);
  const start = parsed[0].timestamp;
  const end = parsed.at(-1).timestamp;
  const xSpan = Math.max(end - start, 1);
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;

  const coordinates = parsed.map((entry) => ({
    x: padding.left + ((entry.timestamp - start) / xSpan) * plotWidth,
    y: padding.top + (1 - entry.value / scaleMax) * plotHeight,
  }));
  const path = coordinates
    .map((coordinate, index) => {
      const operation = index === 0 ? 'M' : 'L';
      return `${operation} ${coordinate.x.toFixed(2)} ${coordinate.y.toFixed(2)}`;
    })
    .join(' ');

  const tickIndexes = [...new Set([0, Math.floor((parsed.length - 1) / 2), parsed.length - 1])];
  const xTicks = tickIndexes.map((index) => ({
    x: coordinates[index].x,
    value: parsed[index].point.trip_hour,
  }));
  const yTicks = [scaleMax, scaleMax / 2, 0].map((value) => ({
    value,
    y: padding.top + (1 - value / scaleMax) * plotHeight,
  }));

  return { path, maxValue, xTicks, yTicks, width, height, padding };
}

export function statusDescriptor(run) {
  if (!run) return { label: 'No run', tone: 'offline', symbol: '—' };
  if (run.status === 'failed_quality') {
    return {
      label: run.published_at ? 'Published with failed gates' : 'Quality failed',
      tone: 'failed',
      symbol: '!',
    };
  }
  if (run.status === 'failed') return { label: 'Failed', tone: 'failed', symbol: '!' };
  if (run.status === 'running') return { label: 'Running', tone: 'running', symbol: '…' };
  if (run.published_at) return { label: 'Published', tone: 'success', symbol: '✓' };
  if (run.status === 'success') return { label: 'Succeeded', tone: 'success', symbol: '✓' };
  return { label: prettyName(run.status || 'unknown'), tone: 'offline', symbol: '?' };
}

const safeFailureSummaries = new Set([
  'Quality gate failure',
  'Replay source or input integrity failure',
  'Replay output equivalence failure',
  'Run failed; review operator logs',
]);

export function failureSummary(run = {}) {
  const summary = String(run.failure_summary ?? '').trim();
  return safeFailureSummaries.has(summary) ? summary : null;
}

export function formatNumber(value) {
  return value !== null && value !== undefined && Number.isFinite(Number(value))
    ? numberFormatter.format(Number(value))
    : '—';
}

export function formatCompactNumber(value) {
  return value !== null && value !== undefined && Number.isFinite(Number(value))
    ? compactFormatter.format(Number(value))
    : '—';
}

export function formatCurrency(value, precise = false) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return (precise ? preciseCurrencyFormatter : currencyFormatter).format(Number(value));
}

export function formatPercent(value, digits = 0) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

export function formatDate(value) {
  const dateOnly = String(value ?? '').match(/^(\d{4})-(\d{2})-(\d{2})$/);
  const parsed = dateOnly
    ? new Date(Date.UTC(Number(dateOnly[1]), Number(dateOnly[2]) - 1, Number(dateOnly[3])))
    : new Date(value);
  return Number.isNaN(parsed.getTime()) ? '—' : dateFormatter.format(parsed);
}

export function formatDateTime(value) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? '—' : dateTimeFormatter.format(parsed);
}

export function formatDuration(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return '—';
  const total = Math.max(0, Number(seconds));
  if (total < 60) return `${total.toFixed(total < 10 ? 1 : 0)}s`;
  return `${Math.floor(total / 60)}m ${Math.round(total % 60)}s`;
}

export function formatBytes(value) {
  if (!Number.isFinite(Number(value))) return '—';
  const bytes = Number(value);
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 ** 2) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
}

export function prettyName(value) {
  return String(value ?? '')
    .replaceAll('_', ' ')
    .replace(/\b\w/g, (character) => character.toUpperCase());
}

export function formatObserved(value) {
  if (value === null || value === undefined || value === '') return 'No value';
  const numeric = Number(value);
  return Number.isFinite(numeric) ? numberFormatter.format(numeric) : String(value);
}

export function formatDriftValue(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return driftValueFormatter.format(Number(value));
}

export function formatDelta(value, kind = 'absolute') {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  const numeric = Object.is(Number(value), -0) ? 0 : Number(value);
  const prefix = numeric > 0 ? '+' : '';
  if (kind === 'relative') return `${prefix}${(numeric * 100).toFixed(1)}%`;
  return `${prefix}${driftValueFormatter.format(numeric)}`;
}

export function formatDriftThreshold(value, kind = 'absolute') {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  return kind === 'relative'
    ? `±${(Number(value) * 100).toFixed(1)}%`
    : `±${driftValueFormatter.format(Number(value))}`;
}

export function runIntervalLabel(run = {}) {
  if (!run.data_interval_start && !run.data_interval_end) return 'Data interval —';
  const start = formatDate(run.data_interval_start);
  const end = formatDate(run.data_interval_end);
  return start === end ? start : `${start} – ${end}`;
}

export function shortHash(value, length = 10) {
  const hash = String(value ?? '');
  return hash ? hash.slice(0, Math.max(4, length)) : '—';
}

export function shortRunId(value) {
  const runId = String(value ?? '');
  return runId ? runId.slice(-8) : '—';
}
