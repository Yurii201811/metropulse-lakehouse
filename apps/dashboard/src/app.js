const API_BASE = window.METROPULSE_API_BASE_URL || 'http://127.0.0.1:8000';

const numberFormat = new Intl.NumberFormat('en-US');
const currencyFormat = new Intl.NumberFormat('en-US', {
  style: 'currency',
  currency: 'USD',
  maximumFractionDigits: 0,
});

const endpoints = {
  summary: '/api/summary',
  timeseries: '/api/timeseries',
  stations: '/api/stations',
  zones: '/api/zones',
  quality: '/api/quality',
  runs: '/api/pipeline-runs',
  lineage: '/api/lineage',
};

async function get(path) {
  const response = await fetch(`${API_BASE}${path}`);
  if (!response.ok) {
    throw new Error(`${path} returned ${response.status}`);
  }
  return response.json();
}

async function loadDashboard() {
  const entries = await Promise.all(
    Object.entries(endpoints).map(async ([key, path]) => [key, await get(path)]),
  );
  return Object.fromEntries(entries);
}

function renderDashboard(data) {
  const dashboard = document.querySelector('#dashboard');
  document.querySelector('#loading-state').classList.add('hidden');
  document.querySelector('#error-state').classList.add('hidden');
  dashboard.classList.remove('hidden');

  document.querySelector('#date-range').textContent = formatRange(
    data.summary.first_trip_date,
    data.summary.last_trip_date,
  );
  document.querySelector('#run-status').textContent = data.runs[0]?.status || 'ready';
  document.querySelector('#lineage-run').textContent = data.runs[0]
    ? `Run ${data.runs[0].run_id.slice(-8)}`
    : 'Latest run';

  renderKpis(data);
  renderChart(data.timeseries);
  renderRuns(data.runs);
  renderQuality(data.quality);
  renderStations(data.stations);
  renderZones(data.zones);
  renderLineage(data.lineage);
}

function renderKpis(data) {
  const validationRate = Math.round((data.summary.validation_rate || 0) * 100);
  const passingChecks = data.quality.filter((check) => check.status === 'pass').length;
  const kpis = [
    {
      label: 'Trips',
      value: numberFormat.format(data.summary.total_trips),
      detail: `${data.summary.active_stations} active stations`,
      tone: 'blue',
    },
    {
      label: 'Revenue',
      value: currencyFormat.format(data.summary.total_revenue),
      detail: `${data.summary.avg_duration_min} min avg ride`,
      tone: 'blue',
    },
    {
      label: 'Validation',
      value: `${validationRate}%`,
      detail: `${passingChecks} checks passing`,
      tone: 'green',
    },
    {
      label: 'Runtime',
      value: `${data.summary.latest_runtime_seconds || 0}s`,
      detail: 'Latest pipeline run',
      tone: 'amber',
    },
  ];
  document.querySelector('#kpis').innerHTML = kpis
    .map(
      (kpi) => `
        <article class="kpi ${kpi.tone}">
          <span>${escapeHtml(kpi.label)}</span>
          <strong>${escapeHtml(kpi.value)}</strong>
          <small>${escapeHtml(kpi.detail)}</small>
        </article>
      `,
    )
    .join('');
}

function renderChart(points) {
  const sampled = samplePoints(points, 64);
  const width = 720;
  const height = 250;
  const padding = 32;
  const maxTrips = Math.max(...sampled.map((point) => point.trips), 1);
  const maxRevenue = Math.max(...sampled.map((point) => point.revenue), 1);
  const tripPath = toPath(sampled, width, height, padding, (point) => point.trips / maxTrips);
  const revenuePath = toPath(
    sampled,
    width,
    height,
    padding,
    (point) => point.revenue / maxRevenue,
  );

  document.querySelector('#timeseries-chart').innerHTML = `
    <defs>
      <linearGradient id="areaFill" x1="0" x2="0" y1="0" y2="1">
        <stop offset="0%" stop-color="#0f766e" stop-opacity="0.22"></stop>
        <stop offset="100%" stop-color="#0f766e" stop-opacity="0.02"></stop>
      </linearGradient>
    </defs>
    ${[0, 1, 2, 3]
      .map((line) => {
        const y = padding + (line * (height - padding * 2)) / 3;
        return `<line class="gridline" x1="${padding}" x2="${width - padding}" y1="${y}" y2="${y}"></line>`;
      })
      .join('')}
    <path class="area" d="${tripPath} L ${width - padding} ${height - padding} L ${padding} ${height - padding} Z"></path>
    <path class="trip-line" d="${tripPath}"></path>
    <path class="revenue-line" d="${revenuePath}"></path>
  `;
}

function renderRuns(runs) {
  document.querySelector('#runs').innerHTML = runs
    .slice(0, 4)
    .map(
      (run) => `
        <div class="timeline-item">
          <span class="${run.status === 'success' ? 'status-dot success' : 'status-dot warning'}"></span>
          <div>
            <strong>${escapeHtml(run.status)}</strong>
            <small>${formatDateTime(run.started_at)} · ${numberFormat.format(run.silver_trips || 0)} rows</small>
          </div>
        </div>
      `,
    )
    .join('');
}

function renderQuality(checks) {
  document.querySelector('#quality').innerHTML = checks
    .map(
      (check) => `
        <div class="quality-item">
          <span class="${check.status === 'pass' ? 'check-pass' : 'check-fail'}">✓</span>
          <div>
            <strong>${prettyName(check.check_name)}</strong>
            <small>${formatObserved(check.observed_value)} · ${escapeHtml(check.threshold)}</small>
          </div>
        </div>
      `,
    )
    .join('');
}

function renderStations(stations) {
  document.querySelector('#stations').innerHTML = `
    <table>
      <thead>
        <tr>
          <th>Station</th>
          <th>Zone</th>
          <th>Departures</th>
          <th>Revenue</th>
          <th>Member share</th>
        </tr>
      </thead>
      <tbody>
        ${stations
          .map(
            (station) => `
              <tr>
                <td>
                  <strong>${escapeHtml(station.station_name)}</strong>
                  <span>${escapeHtml(station.station_id)}</span>
                </td>
                <td>${escapeHtml(station.zone_name)}</td>
                <td>${numberFormat.format(station.departures)}</td>
                <td>${currencyFormat.format(station.revenue)}</td>
                <td>${Math.round(station.member_trip_share * 100)}%</td>
              </tr>
            `,
          )
          .join('')}
      </tbody>
    </table>
  `;
}

function renderZones(zones) {
  const maxRevenue = Math.max(...zones.map((zone) => zone.revenue), 1);
  document.querySelector('#zones').innerHTML = zones
    .map(
      (zone) => `
        <div class="zone-row">
          <div>
            <strong>${escapeHtml(zone.zone_name)}</strong>
            <span>${numberFormat.format(zone.trips)} trips</span>
          </div>
          <div class="zone-bar" aria-hidden="true">
            <i style="width:${Math.max(12, (zone.revenue / maxRevenue) * 100)}%"></i>
          </div>
          <b>${currencyFormat.format(zone.revenue)}</b>
        </div>
      `,
    )
    .join('');
}

function renderLineage() {
  const groups = [
    {
      label: 'Raw CSV',
      nodes: ['raw.trips.csv', 'raw.payments.csv', 'raw.stations.csv', 'raw.weather.csv'],
    },
    {
      label: 'Bronze',
      nodes: ['bronze.trips', 'bronze.payments', 'bronze.stations', 'bronze.weather'],
    },
    { label: 'Silver', nodes: ['silver.trip_enriched'] },
    {
      label: 'Gold/API',
      nodes: [
        'gold.hourly_mobility',
        'gold.daily_station_performance',
        'gold.revenue_by_zone',
        'FastAPI + dashboard',
      ],
    },
  ];
  document.querySelector('#lineage').innerHTML = groups
    .map(
      (group, index) => `
        <div class="lineage-column">
          <span>${escapeHtml(group.label)}</span>
          ${group.nodes.map((node) => `<div class="lineage-node">${escapeHtml(node)}</div>`).join('')}
          ${index < groups.length - 1 ? '<span class="lineage-arrow">→</span>' : ''}
        </div>
      `,
    )
    .join('');
}

function showError(error) {
  document.querySelector('#loading-state').classList.add('hidden');
  const errorState = document.querySelector('#error-state');
  errorState.classList.remove('hidden');
  errorState.innerHTML = `<strong>API connection failed</strong><span>${escapeHtml(error.message)}</span>`;
}

function samplePoints(points, maxPoints) {
  if (points.length <= maxPoints) return points;
  const stride = Math.ceil(points.length / maxPoints);
  return points.filter((_, index) => index % stride === 0);
}

function toPath(points, width, height, padding, getRatio) {
  return points
    .map((point, index) => {
      const x = padding + (index * (width - padding * 2)) / Math.max(points.length - 1, 1);
      const y = height - padding - getRatio(point) * (height - padding * 2);
      return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
    })
    .join(' ');
}

function formatRange(start, end) {
  return `${new Date(start).toLocaleDateString()} to ${new Date(end).toLocaleDateString()}`;
}

function formatDateTime(value) {
  return new Date(value).toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatObserved(value) {
  return Number(value).toFixed(Number(value) < 1 ? 3 : 0);
}

function prettyName(value) {
  return escapeHtml(value.replaceAll('_', ' '));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

loadDashboard().then(renderDashboard).catch(showError);
