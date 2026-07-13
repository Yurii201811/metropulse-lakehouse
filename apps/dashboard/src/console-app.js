import {
  buildQuery,
  chartGeometry,
  createRequestGate,
  escapeHtml,
  failureSummary,
  formatBytes,
  formatCompactNumber,
  formatCurrency,
  formatDate,
  formatDateTime,
  formatDelta,
  formatDriftThreshold,
  formatDriftValue,
  formatDuration,
  formatNumber,
  formatObserved,
  formatPercent,
  prettyName,
  reconcileActiveFilters,
  runIntervalLabel,
  shortHash,
  shortRunId,
  statusDescriptor,
} from './dashboard-core.js';

const API_BASE = String(window.METROPULSE_API_BASE_URL || 'http://127.0.0.1:8000').replace(
  /\/$/,
  '',
);

const endpoints = {
  health: '/health',
  ready: '/ready',
  summary: '/api/summary',
  timeseries: '/api/timeseries',
  stations: '/api/stations',
  zones: '/api/zones',
  quality: '/api/quality',
  runs: '/api/pipeline-runs',
  lineage: '/api/lineage',
  filters: '/api/filters',
  manifests: '/api/ingest-files',
  drift: '/api/drift',
};

const analyticsKeys = ['summary', 'timeseries', 'stations', 'zones'];
const allKeys = [...Object.keys(endpoints)];
const systemKeys = allKeys.filter((key) => !analyticsKeys.includes(key));
const requestGate = createRequestGate();
const state = {
  data: {},
  errors: new Map(),
  activeFilters: {},
  chartTableVisible: false,
  chartWidth: null,
  commandIndex: 0,
  commandTrigger: null,
  selectedRunId: null,
  runDetail: null,
  runDetailError: null,
  runDetailLoading: false,
};

const chartDateFormatter = new Intl.DateTimeFormat('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  hour12: false,
});

function element(selector) {
  return document.querySelector(selector);
}

async function getJson(key, query = '') {
  const params = new URLSearchParams(query.replace(/^\?/, ''));
  if (key === 'stations') params.set('limit', '12');
  const queryString = params.toString();
  const url = `${API_BASE}${endpoints[key]}${queryString ? `?${queryString}` : ''}`;
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(url, {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (key === 'ready' && response.status === 503 && payload) return payload;
    if (!response.ok) {
      const detail = typeof payload?.detail === 'string' ? payload.detail : '';
      throw new Error(detail || `${endpoints[key]} returned HTTP ${response.status}`);
    }
    return payload;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`${endpoints[key]} timed out after 10 seconds.`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function getRunDetail(runId) {
  const url = `${API_BASE}${endpoints.runs}/${encodeURIComponent(runId)}`;
  const controller = new AbortController();
  const timeout = window.setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(url, {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });
    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }
    if (!response.ok) {
      const detail = typeof payload?.detail === 'string' ? payload.detail : '';
      throw new Error(detail || `${endpoints.runs}/{run_id} returned HTTP ${response.status}`);
    }
    return payload;
  } catch (error) {
    if (error.name === 'AbortError') {
      throw new Error(`${endpoints.runs}/{run_id} timed out after 10 seconds.`);
    }
    throw error;
  } finally {
    window.clearTimeout(timeout);
  }
}

async function loadKeys(keys, query = '', request = null) {
  const results = await Promise.allSettled(keys.map((key) => getJson(key, query)));
  if (request && !requestGate.isCurrent(request.scope, request.generation)) return false;

  results.forEach((result, index) => {
    const key = keys[index];
    if (result.status === 'fulfilled') {
      state.data[key] = result.value;
      state.errors.delete(key);
    } else {
      delete state.data[key];
      state.errors.set(key, result.reason instanceof Error ? result.reason : new Error(String(result.reason)));
    }
  });
  return true;
}

function beginRequest(scope) {
  return { scope, generation: requestGate.begin(scope) };
}

function renderAll() {
  renderHeader();
  renderFilters();
  renderKpis();
  renderTimeseries();
  renderRuns();
  renderQuality();
  renderDrift();
  renderRunInvestigation();
  renderStations();
  renderZones();
  renderManifests();
  renderLineage();
  renderNotice();
}

function renderOperationalData() {
  renderHeader();
  renderFilters();
  renderRuns();
  renderQuality();
  renderDrift();
  renderRunInvestigation();
  renderManifests();
  renderLineage();
  renderNotice();
}

function renderHeader() {
  const summary = state.data.summary;
  const readiness = state.data.ready;
  const health = state.data.health;
  const publishedRun = (state.data.runs || []).find((run) => run.published_at);
  const ready = readiness?.status === 'ready';
  const offline = state.errors.has('health');
  const readinessUnknown = !offline && !readiness;
  const descriptor = offline
    ? { label: 'Offline', tone: 'offline' }
    : ready
      ? publishedRun
        ? statusDescriptor(publishedRun)
        : { label: 'Ready', tone: 'success' }
      : { label: readinessUnknown ? 'Readiness unknown' : 'Not ready', tone: 'failed' };

  const sidebarStatus = element('#sidebar-status');
  sidebarStatus.className = `rail-status status-${
    offline ? 'offline' : ready ? 'ready' : readinessUnknown ? 'warning' : 'failed'
  }`;
  sidebarStatus.innerHTML = `
    <span class="status-indicator" aria-hidden="true"></span>
    <span>
      <strong>${
        offline
          ? 'API unavailable'
          : ready
            ? 'Snapshot ready'
            : readinessUnknown
              ? 'Readiness unknown'
              : 'Snapshot not ready'
      }</strong>
      <small>${
        offline
          ? 'Start the API and retry'
          : readiness?.snapshot_run_id
            ? `Run ${escapeHtml(shortRunId(readiness.snapshot_run_id))}`
            : readinessUnknown
              ? 'Retry the readiness check'
              : readiness?.missing_tables?.length
                ? `${readiness.missing_tables.length} required tables missing`
                : 'Run the pipeline first'
      }</small>
    </span>
  `;

  const runStatus = element('#run-status');
  runStatus.className = `run-chip status-${descriptor.tone}`;
  runStatus.innerHTML = `
    <span class="status-indicator" aria-hidden="true"></span>
    <span>${escapeHtml(descriptor.label)}</span>
  `;

  element('#rail-version').textContent = health?.version ? `API version ${health.version}` : 'API version —';
  element('#api-docs-link').href = `${API_BASE}/docs`;

  if (summary?.first_trip_date && summary?.last_trip_date) {
    element('#date-range').textContent = `${formatDate(summary.first_trip_date)} – ${formatDate(
      summary.last_trip_date,
    )}`;
  } else if (state.errors.has('summary')) {
    element('#date-range').textContent = 'Data window unavailable';
  } else {
    element('#date-range').textContent = 'No trips in this query window';
  }

  element('#snapshot-meta').textContent = summary?.snapshot_published_at
    ? `Run ${shortRunId(summary.snapshot_run_id)} · published ${formatDateTime(summary.snapshot_published_at)}`
    : 'No published snapshot metadata';
  element('#lineage-run').textContent = summary?.snapshot_run_id
    ? `Run ${shortRunId(summary.snapshot_run_id)}`
    : 'Latest published run';
}

function renderFilters() {
  const filters = state.data.filters;
  if (!filters) {
    updateFilterCount();
    return;
  }

  state.activeFilters = reconcileActiveFilters(filters, state.activeFilters);

  const zoneSelect = element('#zone-filter');
  zoneSelect.innerHTML = [
    '<option value="">All zones</option>',
    ...(filters.zones || []).map(
      (zone) =>
        `<option value="${escapeHtml(zone.zone_id)}">${escapeHtml(zone.zone_name)} · ${escapeHtml(
          zone.zone_id,
        )}</option>`,
    ),
  ].join('');
  const riderSelect = element('#rider-filter');
  riderSelect.innerHTML = [
    '<option value="">All riders</option>',
    ...(filters.rider_types || []).map(
      (rider) => `<option value="${escapeHtml(rider)}">${escapeHtml(prettyName(rider))}</option>`,
    ),
  ].join('');

  for (const selector of ['#start-date', '#end-date']) {
    const input = element(selector);
    input.min = filters.start_date || '';
    input.max = filters.end_date || '';
  }
  element('#start-date').value = state.activeFilters.start_date || filters.start_date || '';
  element('#end-date').value = state.activeFilters.end_date || filters.end_date || '';
  zoneSelect.value = state.activeFilters.zone_id || '';
  riderSelect.value = state.activeFilters.rider_type || '';
  updateFilterCount();
}

function updateFilterCount() {
  const filterCount = Object.keys(state.activeFilters).length;
  const total = state.data.summary?.total_trips;
  const tripLabel = Number.isFinite(Number(total)) ? `${formatNumber(total)} trips` : 'Trips unavailable';
  element('#filter-count').textContent = filterCount
    ? `${tripLabel} · ${filterCount} active ${filterCount === 1 ? 'filter' : 'filters'}`
    : `${tripLabel} · full dataset`;
}

function renderKpis() {
  const container = element('#kpis');
  const summary = state.data.summary;
  if (!summary) {
    container.innerHTML = panelError('Metrics unavailable', state.errors.get('summary'));
    return;
  }

  const rejected = Number(summary.rejected_trips || 0) + Number(summary.rejected_payments || 0);
  const cards = [
    {
      label: 'Accepted trips',
      value: formatNumber(summary.total_trips),
      detail: `${formatNumber(summary.active_stations)} active start stations`,
      signal: true,
    },
    {
      label: 'Matched revenue',
      value: formatCurrency(summary.total_revenue),
      detail: `${formatPercent(summary.payment_match_rate, 1)} payment match rate`,
    },
    {
      label: 'Quality pass rate',
      value: formatPercent(summary.validation_rate, 0),
      detail: `${formatNumber(rejected)} contract rows quarantined`,
    },
    {
      label: 'Pipeline runtime',
      value: formatDuration(summary.latest_runtime_seconds),
      detail: `${formatNumber(summary.rejected_trips)} trip · ${formatNumber(
        summary.rejected_payments,
      )} payment rejects`,
    },
  ];

  container.innerHTML = cards
    .map(
      (card) => `
        <article class="metric">
          <span>${escapeHtml(card.label)}</span>
          <strong>${escapeHtml(card.value)}</strong>
          <small>${escapeHtml(card.detail)}</small>
        </article>
      `,
    )
    .join('');
}

function renderTimeseries() {
  const container = element('#timeseries-charts');
  const points = state.data.timeseries;
  if (!Array.isArray(points)) {
    container.innerHTML = panelError('Time series unavailable', state.errors.get('timeseries'));
    resetTimeseriesTable();
    return;
  }
  if (points.length === 0) {
    container.innerHTML = emptyState('No hourly observations', 'Adjust or reset the query window.');
    resetTimeseriesTable();
    return;
  }

  renderTimeseriesCharts(points, { force: true });
  renderTimeseriesTable(points);
  setChartTableAvailability(true);
}

function renderTimeseriesCharts(points, { force = false } = {}) {
  const container = element('#timeseries-charts');
  const chartWidth = Math.max(240, Math.min(760, Math.floor(container.clientWidth || 760)));
  if (!force && state.chartWidth === chartWidth) return;
  state.chartWidth = chartWidth;
  container.innerHTML = [
    renderSmallMultiple(points, 'trips', 'Trips', 'Accepted trips', false, chartWidth),
    renderSmallMultiple(points, 'revenue', 'Revenue', 'Matched USD', true, chartWidth),
  ].join('');
}

function renderTimeseriesTable(points) {
  const tableContainer = element('#timeseries-table');
  tableContainer.innerHTML = `
    <table>
      <caption class="visually-hidden">Hourly accepted trips and matched revenue</caption>
      <thead>
        <tr><th>Hour</th><th class="numeric">Trips</th><th class="numeric">Revenue</th></tr>
      </thead>
      <tbody>
        ${points
          .map(
            (point) => `
              <tr>
                <td><time datetime="${escapeHtml(point.trip_hour)}">${escapeHtml(
                  formatDateTime(point.trip_hour),
                )}</time></td>
                <td class="numeric">${escapeHtml(formatNumber(point.trips))}</td>
                <td class="numeric">${escapeHtml(formatCurrency(point.revenue, true))}</td>
              </tr>
            `,
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function setChartTableAvailability(available) {
  const button = element('#chart-table-toggle');
  const tableContainer = element('#timeseries-table');
  if (!available) state.chartTableVisible = false;
  button.disabled = !available;
  button.setAttribute('aria-expanded', String(available && state.chartTableVisible));
  button.textContent = available && state.chartTableVisible ? 'Hide data table' : 'View data table';
  tableContainer.classList.toggle('hidden', !available || !state.chartTableVisible);
}

function resetTimeseriesTable() {
  state.chartWidth = null;
  element('#timeseries-table').innerHTML = '';
  setChartTableAvailability(false);
}

function renderSmallMultiple(points, valueKey, label, unit, currency = false, width = 760) {
  const geometry = chartGeometry(points, valueKey, {
    width,
    height: 184,
    padding: { top: 12, right: 12, bottom: 30, left: 54 },
  });
  const chartId = `chart-${valueKey}-title`;
  const yLines = geometry.yTicks
    .map(
      (tick) => `
        <line class="chart-grid" x1="${geometry.padding.left}" x2="${
          geometry.width - geometry.padding.right
        }" y1="${tick.y.toFixed(2)}" y2="${tick.y.toFixed(2)}"></line>
        <text class="chart-axis-label" x="${geometry.padding.left - 8}" y="${(
          tick.y + 4
        ).toFixed(2)}" text-anchor="end">${escapeHtml(axisValue(tick.value, currency))}</text>
      `,
    )
    .join('');
  const xLabels = geometry.xTicks
    .map((tick, index) => {
      const anchor = index === 0 ? 'start' : index === geometry.xTicks.length - 1 ? 'end' : 'middle';
      return `<text class="chart-axis-label" x="${tick.x.toFixed(2)}" y="${
        geometry.height - 8
      }" text-anchor="${anchor}">${escapeHtml(chartTimestamp(tick.value))}</text>`;
    })
    .join('');

  return `
    <figure class="chart-figure">
      <figcaption id="${chartId}">
        <span>${escapeHtml(label)}</span>
        <span>${escapeHtml(unit)} · hourly · ${escapeHtml(formatNumber(points.length))} points · peak ${escapeHtml(
          axisValue(geometry.maxValue, currency),
        )}</span>
      </figcaption>
      <svg class="chart-svg" viewBox="0 0 ${geometry.width} ${geometry.height}" role="img" aria-labelledby="${chartId}">
        ${yLines}
        ${xLabels}
        <path class="chart-line${valueKey === 'revenue' ? ' revenue' : ''}" d="${geometry.path}"></path>
      </svg>
    </figure>
  `;
}

function axisValue(value, currency = false) {
  const formatted = formatCompactNumber(value);
  return currency ? `$${formatted}` : formatted;
}

function chartTimestamp(value) {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : chartDateFormatter.format(date);
}

function renderRuns() {
  const container = element('#runs');
  const runs = state.data.runs;
  if (!runs) {
    container.innerHTML = panelError('Run history unavailable', state.errors.get('runs'));
    return;
  }
  if (runs.length === 0) {
    container.innerHTML = emptyState('No pipeline runs', 'Run `metropulse run` to publish a snapshot.');
    return;
  }

  container.className = 'run-list';
  container.innerHTML = runs
    .map((run) => {
      const descriptor = statusDescriptor(run);
      const duration = run.ended_at
        ? Math.max(0, (new Date(run.ended_at) - new Date(run.started_at)) / 1000)
        : null;
      const selected = state.selectedRunId === run.run_id;
      const sourceMode = prettyName(run.source_mode || 'generated');
      const failure = failureSummary(run);
      return `
        <article class="run-row ${escapeHtml(descriptor.tone)}${selected ? ' selected' : ''}">
          <button
            class="run-control"
            type="button"
            data-run-id="${escapeHtml(run.run_id)}"
            aria-expanded="${selected}"
            aria-controls="run-investigation"
          >
            <span class="run-state" aria-hidden="true">${escapeHtml(descriptor.symbol)}</span>
            <span class="run-main">
              <span class="run-title">
                <strong>${escapeHtml(descriptor.label)} · ${escapeHtml(shortRunId(run.run_id))}</strong>
                <time datetime="${escapeHtml(run.started_at)}">${escapeHtml(
                  formatDateTime(run.started_at),
                )}</time>
              </span>
              <span class="run-details">
                <span>${escapeHtml(formatNumber(run.silver_trips))} accepted</span>
                <span>${escapeHtml(formatNumber(run.rejected_trips))} rejected</span>
                <span>${escapeHtml(formatNumber(run.quality_passed))}/${escapeHtml(
                  formatNumber(Number(run.quality_passed || 0) + Number(run.quality_failed || 0)),
                )} checks</span>
                <span>${escapeHtml(formatDuration(duration))}</span>
              </span>
              <span class="run-provenance">
                <span>${escapeHtml(sourceMode)}</span>
                <span>${escapeHtml(runIntervalLabel(run))}</span>
                ${
                  run.replay_of_run_id
                    ? `<span>Replay of ${escapeHtml(shortRunId(run.replay_of_run_id))}</span>`
                    : ''
                }
                <span>Input <code title="${escapeHtml(run.input_set_sha256 || '')}">${escapeHtml(
                  shortHash(run.input_set_sha256),
                )}</code></span>
                <span>Output <code title="${escapeHtml(run.output_set_sha256 || '')}">${escapeHtml(
                  shortHash(run.output_set_sha256),
                )}</code></span>
              </span>
              ${
                failure ? `<span class="run-error">${escapeHtml(failure)}</span>` : ''
              }
            </span>
            <span class="run-disclosure" aria-hidden="true">${selected ? 'Close' : 'Inspect'}</span>
          </button>
        </article>
      `;
    })
    .join('');

  container.querySelectorAll('[data-run-id]').forEach((button) => {
    button.addEventListener('click', () => selectRun(button.dataset.runId));
  });
}

function renderQuality() {
  const container = element('#quality');
  const checks = state.data.quality;
  if (!checks) {
    element('#quality-summary').textContent = 'Quality evidence unavailable';
    container.innerHTML = panelError('Quality evidence unavailable', state.errors.get('quality'));
    return;
  }
  if (checks.length === 0) {
    element('#quality-summary').textContent = 'No checks recorded';
    container.innerHTML = emptyState('No quality results', 'Publish a pipeline run to create checks.');
    return;
  }

  const passed = checks.filter((check) => check.status === 'pass').length;
  element('#quality-summary').textContent = `${passed} of ${checks.length} checks passing`;
  container.className = 'quality-list';
  container.innerHTML = checks
    .map(
      (check) => `
        <article class="quality-row ${check.status === 'pass' ? 'pass' : 'fail'}">
          <span class="quality-state" aria-hidden="true">${check.status === 'pass' ? '✓' : '!'}</span>
          <div class="quality-main">
            <div class="quality-title">
              <strong>${escapeHtml(prettyName(check.check_name))}</strong>
              <span>${escapeHtml(prettyName(check.status))}</span>
            </div>
            <div class="quality-details">
              <span>Observed ${escapeHtml(formatObserved(check.observed_value))}</span>
              <span>Gate ${escapeHtml(check.threshold)}</span>
              ${check.details ? `<span>${escapeHtml(check.details)}</span>` : ''}
            </div>
          </div>
        </article>
      `,
    )
    .join('');
}

function renderDrift() {
  const container = element('#drift');
  const summary = element('#drift-summary');
  const status = element('#drift-status');
  const drift = state.data.drift;

  if (!drift) {
    summary.textContent = 'Comparison evidence unavailable';
    status.className = 'comparison-status unavailable';
    status.textContent = 'Unavailable';
    container.innerHTML = panelError('Snapshot drift unavailable', state.errors.get('drift'));
    return;
  }

  const results = Array.isArray(drift.results) ? drift.results : [];
  const failed = Number(drift.failed_metrics || results.filter((result) => result.status !== 'pass').length);
  const checked = Number(drift.checked_metrics || results.length);
  const passed = Math.max(0, checked - failed);
  const stable = drift.status === 'pass' || (checked > 0 && failed === 0);

  status.className = `comparison-status ${stable ? 'pass' : checked ? 'fail' : 'unavailable'}`;
  status.textContent = checked ? (stable ? 'Stable' : `${failed} breached`) : 'No baseline';
  summary.textContent = drift.baseline_run_id
    ? `${passed} of ${checked} metrics stable · ${shortRunId(drift.run_id)} vs ${shortRunId(
        drift.baseline_run_id,
      )}`
    : 'No compatible published baseline yet';

  if (!checked || results.length === 0) {
    container.innerHTML = emptyState(
      'No drift comparison yet',
      'A compatible prior snapshot is required before drift can be measured.',
    );
    return;
  }

  container.innerHTML = driftTable(results, 'Latest snapshot drift metrics');
}

function driftTable(results, caption) {
  return `
    <div class="table-wrap drift-table-wrap" role="region" aria-label="${escapeHtml(caption)}" tabindex="0">
      <table class="drift-table">
        <caption class="visually-hidden">${escapeHtml(caption)}</caption>
        <thead>
          <tr>
            <th>Metric</th>
            <th class="numeric">Current</th>
            <th class="numeric">Baseline</th>
            <th class="numeric">Change</th>
            <th class="numeric">Gate</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          ${results
            .map((result) => {
              const passed = result.status === 'pass';
              return `
                <tr>
                  <td><strong>${escapeHtml(prettyName(result.metric_name))}</strong></td>
                  <td class="numeric">${escapeHtml(formatDriftValue(result.current_value))}</td>
                  <td class="numeric">${escapeHtml(formatDriftValue(result.baseline_value))}</td>
                  <td class="numeric data-code">${escapeHtml(
                    formatDelta(result.delta, result.delta_kind),
                  )}</td>
                  <td class="numeric data-code">${escapeHtml(
                    formatDriftThreshold(result.threshold, result.delta_kind),
                  )}</td>
                  <td><span class="evidence-status ${passed ? 'pass' : 'fail'}">${
                    passed ? 'Stable' : 'Breached'
                  }</span></td>
                </tr>
              `;
            })
            .join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderRunInvestigation() {
  const panel = element('#run-investigation');
  const content = element('#run-investigation-content');
  const summary = element('#run-investigation-summary');

  panel.classList.toggle('hidden', !state.selectedRunId);
  panel.toggleAttribute('aria-busy', Boolean(state.selectedRunId && state.runDetailLoading));
  if (!state.selectedRunId) {
    summary.textContent = 'Select a run to inspect its evidence';
    content.innerHTML = '';
    return;
  }

  if (state.runDetailLoading) {
    summary.textContent = `Loading run ${shortRunId(state.selectedRunId)}`;
    content.innerHTML = '<div class="skeleton detail-skeleton"></div>';
    return;
  }

  if (state.runDetailError) {
    summary.textContent = `Run ${shortRunId(state.selectedRunId)} could not be loaded`;
    content.innerHTML = `${panelError('Run evidence unavailable', state.runDetailError)}
      <div class="investigation-actions">
        <button class="button-secondary compact-button" type="button" data-run-detail-retry>Retry</button>
      </div>`;
    content.querySelector('[data-run-detail-retry]')?.addEventListener('click', () => {
      selectRun(state.selectedRunId, { force: true });
    });
    return;
  }

  const detail = state.runDetail;
  if (!detail?.run) return;
  const run = detail.run;
  const quality = Array.isArray(detail.quality) ? detail.quality : [];
  const manifests = Array.isArray(detail.manifests) ? detail.manifests : [];
  const profiles = Array.isArray(detail.profiles) ? detail.profiles : [];
  const fingerprints = Array.isArray(detail.fingerprints) ? detail.fingerprints : [];
  const drift = Array.isArray(detail.drift) ? detail.drift : [];
  const baselineRunId = drift[0]?.baseline_run_id || run.previous_published_run_id;
  const descriptor = statusDescriptor(run);
  const failure = failureSummary(run);

  summary.textContent = `${descriptor.label} · ${prettyName(run.source_mode || 'generated')} · ${runIntervalLabel(
    run,
  )}`;
  content.innerHTML = `
    <div class="investigation-ledger">
      ${investigationFact('Run', shortRunId(run.run_id), run.run_id)}
      ${investigationFact('Runtime', formatDuration(run.runtime_seconds))}
      ${investigationFact('Snapshot', run.is_current_snapshot ? 'Current published' : 'Historical')}
      ${investigationFact('Source files', formatNumber(manifests.length))}
      ${investigationFact('Contract', run.contract_version || '—')}
      ${investigationFact('Code', run.code_version || '—')}
    </div>
    ${failure ? `<p class="run-error">${escapeHtml(failure)}</p>` : ''}

    <div class="investigation-grid">
      <section class="evidence-block" aria-labelledby="provenance-title">
        <h4 id="provenance-title">Provenance</h4>
        <dl class="provenance-list">
          ${provenanceItem('Source mode', prettyName(run.source_mode || 'generated'))}
          ${provenanceItem('Data interval', runIntervalLabel(run))}
          ${provenanceItem('Replay of', shortRunId(run.replay_of_run_id), run.replay_of_run_id)}
          ${provenanceItem(
            'Previous published',
            shortRunId(run.previous_published_run_id),
            run.previous_published_run_id,
          )}
          ${provenanceItem('Input set', shortHash(run.input_set_sha256, 16), run.input_set_sha256)}
          ${provenanceItem('Output set', shortHash(run.output_set_sha256, 16), run.output_set_sha256)}
          ${provenanceItem('Config', shortHash(run.config_sha256, 16), run.config_sha256)}
        </dl>
      </section>

      <section class="evidence-block" aria-labelledby="investigation-quality-title">
        <h4 id="investigation-quality-title">Quality · ${escapeHtml(
          `${quality.filter((check) => check.status === 'pass').length}/${quality.length}`,
        )}</h4>
        ${
          quality.length
            ? `<ul class="evidence-list">${quality
                .map(
                  (check) => `<li>
                    <span
                      class="evidence-status ${check.status === 'pass' ? 'pass' : 'fail'}"
                      aria-label="${check.status === 'pass' ? 'Passed' : 'Failed'}"
                    >${check.status === 'pass' ? '✓' : '!'}</span>
                    <span><strong>${escapeHtml(prettyName(check.check_name))}</strong><small>Observed ${escapeHtml(
                      formatObserved(check.observed_value),
                    )} · gate ${escapeHtml(check.threshold)}</small></span>
                  </li>`,
                )
                .join('')}</ul>`
            : emptyState('No quality evidence', 'This run has no recorded quality checks.')
        }
      </section>

      <section class="evidence-block" aria-labelledby="fingerprints-title">
        <h4 id="fingerprints-title">Relation fingerprints</h4>
        ${
          fingerprints.length
            ? `<ul class="fingerprint-list">${fingerprints
                .map(
                  (fingerprint) => `<li>
                    <span><strong>${escapeHtml(fingerprint.relation_name)}</strong><small>${escapeHtml(
                      formatNumber(fingerprint.row_count),
                    )} rows</small></span>
                    <code title="${escapeHtml(fingerprint.fingerprint_sha256)}">${escapeHtml(
                      shortHash(fingerprint.fingerprint_sha256, 16),
                    )}</code>
                  </li>`,
                )
                .join('')}</ul>`
            : emptyState('No fingerprints', 'This run has no recorded relation fingerprints.')
        }
      </section>

      <section class="evidence-block" aria-labelledby="profiles-title">
        <h4 id="profiles-title">Snapshot profile</h4>
        ${
          profiles.length
            ? `<dl class="profile-list">${profiles
                .map(
                  (profile) => `<div><dt>${escapeHtml(prettyName(profile.metric_name))}</dt><dd>${escapeHtml(
                    formatDriftValue(profile.metric_value),
                  )}${profile.unit ? ` <small>${escapeHtml(profile.unit)}</small>` : ''}</dd></div>`,
                )
                .join('')}</dl>`
            : emptyState('No profile metrics', 'This run has no recorded snapshot profile.')
        }
      </section>
    </div>

    <section class="prior-comparison" aria-labelledby="prior-comparison-title">
      <div class="evidence-heading">
        <h4 id="prior-comparison-title">Prior comparison</h4>
        <span>${escapeHtml(
          baselineRunId
            ? `Against ${shortRunId(baselineRunId)}`
            : 'No compatible prior run',
        )}</span>
      </div>
      ${
        drift.length
          ? driftTable(drift, `Drift comparison for run ${shortRunId(run.run_id)}`)
          : emptyState('No prior comparison', 'A compatible published run is required for drift evidence.')
      }
    </section>
  `;
}

function investigationFact(label, value, title = '') {
  return `<div><span>${escapeHtml(label)}</span><strong${title ? ` title="${escapeHtml(title)}"` : ''}>${escapeHtml(
    value,
  )}</strong></div>`;
}

function provenanceItem(label, value, title = '') {
  return `<div><dt>${escapeHtml(label)}</dt><dd${title ? ` title="${escapeHtml(title)}"` : ''}>${escapeHtml(
    value,
  )}</dd></div>`;
}

function syncRunSelection() {
  document.querySelectorAll('[data-run-id]').forEach((button) => {
    const selected = button.dataset.runId === state.selectedRunId;
    button.setAttribute('aria-expanded', String(selected));
    button.toggleAttribute('aria-busy', selected && state.runDetailLoading);
    button.closest('.run-row')?.classList.toggle('selected', selected);
    const disclosure = button.querySelector('.run-disclosure');
    if (disclosure) disclosure.textContent = selected ? 'Close' : 'Inspect';
  });
}

async function selectRun(runId, { force = false } = {}) {
  if (!force && state.selectedRunId === runId) {
    closeRunInvestigation();
    return;
  }

  state.selectedRunId = runId;
  state.runDetail = null;
  state.runDetailError = null;
  state.runDetailLoading = true;
  syncRunSelection();
  renderRunInvestigation();

  const request = beginRequest('run-detail');
  try {
    const detail = await getRunDetail(runId);
    if (!requestGate.isCurrent(request.scope, request.generation)) return;
    state.runDetail = detail;
  } catch (error) {
    if (!requestGate.isCurrent(request.scope, request.generation)) return;
    state.runDetailError = error instanceof Error ? error : new Error(String(error));
  } finally {
    if (requestGate.isCurrent(request.scope, request.generation)) {
      state.runDetailLoading = false;
      syncRunSelection();
      renderRunInvestigation();
      element('#run-investigation-title').focus();
    }
  }
}

function closeRunInvestigation() {
  const runId = state.selectedRunId;
  requestGate.begin('run-detail');
  state.selectedRunId = null;
  state.runDetail = null;
  state.runDetailError = null;
  state.runDetailLoading = false;
  syncRunSelection();
  renderRunInvestigation();
  if (runId) {
    [...document.querySelectorAll('[data-run-id]')]
      .find((button) => button.dataset.runId === runId)
      ?.focus();
  }
}

function renderStations() {
  const container = element('#stations');
  const stations = state.data.stations;
  if (!stations) {
    container.innerHTML = panelError('Station model unavailable', state.errors.get('stations'));
    return;
  }
  if (stations.length === 0) {
    container.innerHTML = emptyState('No station rows', 'Adjust or reset the query window.');
    return;
  }

  container.innerHTML = `
    <table class="station-table">
      <caption class="visually-hidden">Top departure stations for the active query window</caption>
      <thead>
        <tr>
          <th>Station</th>
          <th>Zone</th>
          <th class="numeric">Departures</th>
          <th class="numeric">Revenue</th>
          <th class="numeric">Member share</th>
        </tr>
      </thead>
      <tbody>
        ${stations
          .map(
            (station) => `
              <tr>
                <td class="station-name" data-label="Station">
                  <strong>${escapeHtml(station.station_name)}</strong>
                  <span>${escapeHtml(station.station_id)}</span>
                </td>
                <td data-label="Zone">${escapeHtml(station.zone_name)}</td>
                <td class="numeric" data-label="Departures">${escapeHtml(
                  formatNumber(station.departures),
                )}</td>
                <td class="numeric" data-label="Revenue">${escapeHtml(
                  formatCurrency(station.revenue),
                )}</td>
                <td class="numeric" data-label="Member share">${escapeHtml(
                  formatPercent(station.member_trip_share, 0),
                )}</td>
              </tr>
            `,
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function renderZones() {
  const container = element('#zones');
  const zones = state.data.zones;
  if (!zones) {
    container.innerHTML = panelError('Zone model unavailable', state.errors.get('zones'));
    return;
  }
  if (zones.length === 0) {
    container.innerHTML = emptyState('No zone rows', 'Adjust or reset the query window.');
    return;
  }

  const totalRevenue = zones.reduce((total, zone) => total + Number(zone.revenue || 0), 0);
  container.className = 'zone-list';
  container.innerHTML = zones
    .map((zone) => {
      const share = totalRevenue > 0 ? Number(zone.revenue || 0) / totalRevenue : 0;
      return `
        <article class="zone-row">
          <div class="zone-copy">
            <strong>${escapeHtml(zone.zone_name)}</strong>
            <span>${escapeHtml(formatNumber(zone.trips))} trips · ${escapeHtml(
              formatCurrency(zone.revenue_per_trip, true),
            )} / trip</span>
          </div>
          <div class="zone-value">
            <strong>${escapeHtml(formatCurrency(zone.revenue))}</strong>
            <span>${escapeHtml(formatPercent(share, 1))} of revenue</span>
          </div>
          <div class="zone-bar" role="img" aria-label="${escapeHtml(
            `${zone.zone_name}: ${formatPercent(share, 1)} of filtered revenue`,
          )}">
            <span style="--bar-size: ${Math.max(0, Math.min(100, share * 100)).toFixed(2)}%"></span>
          </div>
        </article>
      `;
    })
    .join('');
}

function renderManifests() {
  const container = element('#manifests');
  const manifests = state.data.manifests;
  if (!manifests) {
    container.innerHTML = panelError('Source manifest unavailable', state.errors.get('manifests'));
    return;
  }
  if (manifests.length === 0) {
    container.innerHTML = emptyState('No source manifests', 'Publish a new pipeline run to record file evidence.');
    return;
  }

  container.innerHTML = `
    <table>
      <caption class="visually-hidden">Source files recorded for the latest published run</caption>
      <thead>
        <tr>
          <th>Dataset</th>
          <th>Source file</th>
          <th>SHA-256</th>
          <th class="numeric">Rows</th>
          <th class="numeric">Size</th>
          <th>Loaded</th>
        </tr>
      </thead>
      <tbody>
        ${manifests
          .map(
            (manifest) => `
              <tr>
                <td><code>${escapeHtml(manifest.dataset_name)}</code></td>
                <td>${escapeHtml(manifest.source_file)}</td>
                <td><code class="hash-value" title="${escapeHtml(manifest.file_sha256)}">${escapeHtml(
                  manifest.file_sha256,
                )}</code></td>
                <td class="numeric">${escapeHtml(formatNumber(manifest.row_count))}</td>
                <td class="numeric">${escapeHtml(formatBytes(manifest.file_bytes))}</td>
                <td><time datetime="${escapeHtml(manifest.loaded_at)}">${escapeHtml(
                  formatDateTime(manifest.loaded_at),
                )}</time></td>
              </tr>
            `,
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function renderLineage() {
  const container = element('#lineage');
  const edges = state.data.lineage;
  if (!edges) {
    container.innerHTML = panelError('Lineage graph unavailable', state.errors.get('lineage'));
    return;
  }
  if (edges.length === 0) {
    container.innerHTML = emptyState('No lineage edges', 'Publish a pipeline run to build the graph.');
    return;
  }

  container.innerHTML = `
    <div class="table-wrap" role="region" aria-label="Lineage edge table" tabindex="0">
      <table class="lineage-table">
        <caption class="visually-hidden">Source-to-target lineage edges and transform types</caption>
        <thead><tr><th>Source</th><th>Transform</th><th>Target</th></tr></thead>
        <tbody>
          ${edges
            .map(
              (edge) => `
                <tr>
                  <td><code>${escapeHtml(edge.source_node)}</code></td>
                  <td><span class="transform-label">${escapeHtml(
                    prettyName(edge.transform_type),
                  )}</span></td>
                  <td><code>${escapeHtml(edge.target_node)}</code></td>
                </tr>
              `,
            )
            .join('')}
        </tbody>
      </table>
    </div>
  `;
}

function renderNotice() {
  const notice = element('#notice-region');
  if (state.errors.size === 0) {
    notice.className = 'notice hidden';
    notice.innerHTML = '';
    return;
  }

  const unavailable = [...state.errors.keys()].map((key) => prettyName(key)).join(', ');
  const allUnavailable = allKeys.every((key) => state.errors.has(key));
  notice.className = `notice ${allUnavailable ? 'error' : 'warning'}`;
  notice.innerHTML = `
    <strong>${allUnavailable ? 'API unavailable.' : 'Partial data available.'}</strong>
    ${escapeHtml(unavailable)} ${state.errors.size === 1 ? 'endpoint is' : 'endpoints are'} unavailable.
    <button class="button-secondary compact-button" type="button" data-retry>Retry</button>
  `;
  notice.querySelector('[data-retry]').addEventListener('click', retryAll);
}

function panelError(title, error) {
  return `
    <div class="panel-error">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(error?.message || 'The API did not return this data product.')}</p>
    </div>
  `;
}

function emptyState(title, detail) {
  return `
    <div class="empty-state">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(detail)}</p>
    </div>
  `;
}

function selectedFilters() {
  const metadata = state.data.filters || {};
  const values = Object.fromEntries(new FormData(element('#filter-form')).entries());
  const filters = {};
  if (values.start_date && values.start_date !== metadata.start_date) filters.start_date = values.start_date;
  if (values.end_date && values.end_date !== metadata.end_date) filters.end_date = values.end_date;
  if (values.zone_id) filters.zone_id = values.zone_id;
  if (values.rider_type) filters.rider_type = values.rider_type;
  return filters;
}

function setAnalyticsBusy(busy, label = 'Querying…') {
  const applyButton = element('#apply-filters');
  applyButton.disabled = busy;
  applyButton.textContent = busy ? label : 'Apply filters';
  element('#reset-filters').disabled = busy;
  element('#overview').toggleAttribute('aria-busy', busy);
  element('#data-products').toggleAttribute('aria-busy', busy);
}

async function applyFilters(event) {
  event?.preventDefault();
  const startInput = element('#start-date');
  const endInput = element('#end-date');
  startInput.setCustomValidity('');
  endInput.setCustomValidity('');
  if (startInput.value && endInput.value && startInput.value > endInput.value) {
    endInput.setCustomValidity('End date must be on or after the start date.');
    endInput.reportValidity();
    return;
  }

  state.activeFilters = selectedFilters();
  const request = beginRequest('analytics');
  setAnalyticsBusy(true);
  try {
    const committed = await loadKeys(analyticsKeys, buildQuery(state.activeFilters), request);
    if (committed) renderAll();
  } finally {
    if (requestGate.isCurrent(request.scope, request.generation)) setAnalyticsBusy(false);
  }
}

async function resetFilters() {
  const filters = state.data.filters || {};
  element('#start-date').value = filters.start_date || '';
  element('#end-date').value = filters.end_date || '';
  element('#zone-filter').value = '';
  element('#rider-filter').value = '';
  state.activeFilters = {};
  await applyFilters();
}

async function retryAll() {
  const systemRequest = beginRequest('system');
  const analyticsRequest = beginRequest('analytics');
  const retryButton = element('[data-retry]');
  if (retryButton) {
    retryButton.disabled = true;
    retryButton.textContent = 'Retrying…';
  }
  setAnalyticsBusy(true, 'Refreshing…');
  try {
    const systemCommitted = await loadKeys(systemKeys, '', systemRequest);
    if (
      systemCommitted &&
      requestGate.isCurrent(analyticsRequest.scope, analyticsRequest.generation)
    ) {
      renderOperationalData();
    }
    const analyticsCommitted = await loadKeys(
      analyticsKeys,
      buildQuery(state.activeFilters),
      analyticsRequest,
    );
    if (analyticsCommitted) renderAll();
  } finally {
    if (requestGate.isCurrent(analyticsRequest.scope, analyticsRequest.generation)) {
      setAnalyticsBusy(false);
    }
  }
}

function toggleChartTable() {
  state.chartTableVisible = !state.chartTableVisible;
  const button = element('#chart-table-toggle');
  button.setAttribute('aria-expanded', String(state.chartTableVisible));
  button.textContent = state.chartTableVisible ? 'Hide data table' : 'View data table';
  element('#timeseries-table').classList.toggle('hidden', !state.chartTableVisible);
}

function setMenu(open, { restoreFocus = true } = {}) {
  const desktop = window.matchMedia('(min-width: 60rem)').matches;
  const menuOpen = !desktop && open;
  const wasOpen = document.body.classList.contains('menu-open');
  const sidebar = element('#primary-navigation');
  const workspace = element('#main-content');
  const menuButton = element('#mobile-menu');

  document.body.classList.toggle('menu-open', menuOpen);
  menuButton.setAttribute('aria-expanded', String(menuOpen));
  element('#nav-backdrop').tabIndex = menuOpen ? 0 : -1;

  if (desktop) {
    sidebar.removeAttribute('inert');
    sidebar.removeAttribute('aria-hidden');
    workspace.removeAttribute('inert');
    return;
  }

  if (menuOpen) {
    sidebar.removeAttribute('inert');
    sidebar.removeAttribute('aria-hidden');
    workspace.setAttribute('inert', '');
    window.requestAnimationFrame(() => sidebar.focus());
  } else {
    workspace.removeAttribute('inert');
    sidebar.setAttribute('inert', '');
    sidebar.setAttribute('aria-hidden', 'true');
    if (restoreFocus && wasOpen) window.requestAnimationFrame(() => menuButton.focus());
  }
}

function setupNavigation() {
  const desktopLayout = window.matchMedia('(min-width: 60rem)');
  setMenu(false, { restoreFocus: false });
  element('#mobile-menu').addEventListener('click', () => {
    setMenu(!document.body.classList.contains('menu-open'));
  });
  element('#nav-backdrop').addEventListener('click', () => setMenu(false));
  document.querySelectorAll('.rail-link, .brand').forEach((link) => {
    link.addEventListener('click', () => setMenu(false));
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && document.body.classList.contains('menu-open')) setMenu(false);
  });
  desktopLayout.addEventListener('change', () => setMenu(false, { restoreFocus: false }));

  const links = [...document.querySelectorAll('.rail-link[data-section]')];
  const sections = links.map((link) => document.getElementById(link.dataset.section)).filter(Boolean);
  const observer = new IntersectionObserver(
    (entries) => {
      const visible = entries
        .filter((entry) => entry.isIntersecting)
        .sort((left, right) => right.intersectionRatio - left.intersectionRatio)[0];
      if (!visible) return;
      links.forEach((link) => {
        const active = link.dataset.section === visible.target.id;
        link.classList.toggle('active', active);
        if (active) link.setAttribute('aria-current', 'page');
        else link.removeAttribute('aria-current');
      });
    },
    { rootMargin: '-18% 0px -64% 0px', threshold: [0, 0.15, 0.5] },
  );
  sections.forEach((section) => observer.observe(section));
}

function getCommands() {
  return [
    { label: 'Go to overview', meta: 'Navigation', action: () => navigateTo('overview') },
    { label: 'Go to runs and quality', meta: 'Navigation', action: () => navigateTo('operations') },
    { label: 'Go to data products', meta: 'Navigation', action: () => navigateTo('data-products') },
    { label: 'Go to lineage', meta: 'Navigation', action: () => navigateTo('lineage-section') },
    { label: 'Reset query filters', meta: 'Data', action: resetFilters },
    { label: 'Retry unavailable endpoints', meta: 'System', action: retryAll },
    {
      label: 'Open API documentation',
      meta: 'External',
      action: () => window.open(`${API_BASE}/docs`, '_blank', 'noopener'),
    },
    {
      label: 'Open GitHub repository',
      meta: 'External',
      action: () =>
        window.open('https://github.com/Yurii201811/metropulse-lakehouse', '_blank', 'noopener'),
    },
  ];
}

function navigateTo(id) {
  setMenu(false, { restoreFocus: false });
  closeCommands();
  document.getElementById(id)?.scrollIntoView({ block: 'start' });
}

function openCommands(trigger) {
  const dialog = element('#command-dialog');
  const menuWasOpen = document.body.classList.contains('menu-open');
  if (menuWasOpen) setMenu(false, { restoreFocus: false });
  state.commandTrigger = menuWasOpen ? element('#mobile-menu') : trigger;
  state.commandIndex = 0;
  element('#command-input').value = '';
  renderCommands();
  if (!dialog.open) dialog.showModal();
  window.requestAnimationFrame(() => element('#command-input').focus());
}

function closeCommands() {
  const dialog = element('#command-dialog');
  if (dialog.open) dialog.close();
}

function renderCommands() {
  const query = element('#command-input').value.trim().toLowerCase();
  const commands = getCommands().filter((command) => command.label.toLowerCase().includes(query));
  state.commandIndex = Math.max(0, Math.min(state.commandIndex, Math.max(commands.length - 1, 0)));
  const list = element('#command-list');
  const commandInput = element('#command-input');
  if (commands.length) commandInput.setAttribute('aria-activedescendant', `command-option-${state.commandIndex}`);
  else commandInput.removeAttribute('aria-activedescendant');
  list.innerHTML = commands.length
    ? commands
        .map(
          (command, index) => `
            <button
              id="command-option-${index}"
              class="command-item${index === state.commandIndex ? ' active' : ''}"
              type="button"
              role="option"
              tabindex="-1"
              aria-selected="${index === state.commandIndex}"
              data-command-index="${index}"
            >
              <span>${escapeHtml(command.label)}</span>
              <span>${escapeHtml(command.meta)}</span>
            </button>
          `,
        )
        .join('')
    : emptyState('No matching commands', 'Try a navigation or data action.');

  list.querySelectorAll('[data-command-index]').forEach((button) => {
    button.addEventListener('click', () => runCommand(commands[Number(button.dataset.commandIndex)]));
  });
  list.querySelector('.command-item.active')?.scrollIntoView({ block: 'nearest' });
}

async function runCommand(command) {
  if (!command) return;
  closeCommands();
  await command.action();
}

function setupCommands() {
  for (const selector of ['#command-trigger', '#desktop-command']) {
    element(selector).addEventListener('click', (event) => openCommands(event.currentTarget));
  }
  element('#command-close').addEventListener('click', closeCommands);
  element('#command-input').addEventListener('input', () => {
    state.commandIndex = 0;
    renderCommands();
  });
  element('#command-input').addEventListener('keydown', (event) => {
    const query = event.currentTarget.value.trim().toLowerCase();
    const commands = getCommands().filter((command) => command.label.toLowerCase().includes(query));
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      state.commandIndex = Math.min(state.commandIndex + 1, Math.max(commands.length - 1, 0));
      renderCommands();
    } else if (event.key === 'ArrowUp') {
      event.preventDefault();
      state.commandIndex = Math.max(state.commandIndex - 1, 0);
      renderCommands();
    } else if (event.key === 'Enter') {
      event.preventDefault();
      runCommand(commands[state.commandIndex]);
    }
  });
  element('#command-dialog').addEventListener('close', () => state.commandTrigger?.focus());
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && element('#command-dialog').open) {
      event.preventDefault();
      closeCommands();
    } else if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
      event.preventDefault();
      openCommands(document.activeElement || element('#desktop-command'));
    }
  });
}

function setupInteractions() {
  element('#filter-form').addEventListener('submit', applyFilters);
  element('#reset-filters').addEventListener('click', resetFilters);
  element('#chart-table-toggle').addEventListener('click', toggleChartTable);
  element('#run-investigation-close').addEventListener('click', closeRunInvestigation);
  setupNavigation();
  setupCommands();
  let chartResizeFrame = null;
  window.addEventListener('resize', () => {
    if (!Array.isArray(state.data.timeseries) || state.data.timeseries.length === 0 || chartResizeFrame) return;
    chartResizeFrame = window.requestAnimationFrame(() => {
      chartResizeFrame = null;
      renderTimeseriesCharts(state.data.timeseries);
    });
  });
}

async function initialize() {
  setupInteractions();
  const systemRequest = beginRequest('system');
  const analyticsRequest = beginRequest('analytics');
  setAnalyticsBusy(true, 'Loading…');
  try {
    const [systemCommitted, analyticsCommitted] = await Promise.all([
      loadKeys(systemKeys, '', systemRequest),
      loadKeys(analyticsKeys, '', analyticsRequest),
    ]);
    if (analyticsCommitted) renderAll();
    else if (
      systemCommitted &&
      requestGate.isCurrent(analyticsRequest.scope, analyticsRequest.generation)
    ) {
      renderOperationalData();
    }
  } finally {
    if (requestGate.isCurrent(analyticsRequest.scope, analyticsRequest.generation)) {
      setAnalyticsBusy(false);
    }
  }
}

initialize();
