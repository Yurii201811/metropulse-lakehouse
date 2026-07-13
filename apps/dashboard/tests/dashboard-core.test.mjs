import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildQuery,
  chartGeometry,
  createRequestGate,
  escapeHtml,
  failureSummary,
  formatBytes,
  formatDate,
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
} from '../src/dashboard-core.js';

test('escapeHtml protects every HTML-significant character', () => {
  assert.equal(
    escapeHtml(`<script data-x="1">Tom & Jerry's</script>`),
    '&lt;script data-x=&quot;1&quot;&gt;Tom &amp; Jerry&#039;s&lt;/script&gt;',
  );
});

test('buildQuery emits only supported non-empty analytics filters', () => {
  assert.equal(
    buildQuery({
      rider_type: 'member',
      ignored: 'nope',
      zone_id: 'Z02',
      end_date: '',
      start_date: '2026-05-01',
    }),
    '?start_date=2026-05-01&zone_id=Z02&rider_type=member',
  );
});

test('request gate rejects stale work within a scope without affecting other scopes', () => {
  const gate = createRequestGate();
  const firstAnalytics = gate.begin('analytics');
  const system = gate.begin('system');
  const latestAnalytics = gate.begin('analytics');

  assert.equal(gate.isCurrent('analytics', firstAnalytics), false);
  assert.equal(gate.isCurrent('analytics', latestAnalytics), true);
  assert.equal(gate.isCurrent('system', system), true);

  const firstDetail = gate.begin('run-detail');
  const latestDetail = gate.begin('run-detail');
  assert.equal(gate.isCurrent('run-detail', firstDetail), false);
  assert.equal(gate.isCurrent('run-detail', latestDetail), true);
  assert.equal(gate.isCurrent('analytics', latestAnalytics), true);
});

test('refreshed filter metadata preserves valid selections and removes stale ones', () => {
  const metadata = {
    start_date: '2026-05-01',
    end_date: '2026-05-31',
    zones: [{ zone_id: 'Z02' }],
    rider_types: ['member'],
  };

  assert.deepEqual(
    reconcileActiveFilters(metadata, {
      start_date: '2026-05-05',
      end_date: '2026-06-01',
      zone_id: 'Z02',
      rider_type: 'casual',
    }),
    { start_date: '2026-05-05', zone_id: 'Z02' },
  );
});

test('chartGeometry uses the time domain and emits labeled zero-to-peak ticks', () => {
  const points = [
    { trip_hour: '2026-01-01T00:00:00', trips: 0 },
    { trip_hour: '2026-01-01T01:00:00', trips: 10 },
    { trip_hour: '2026-01-01T03:00:00', trips: 5 },
  ];
  const geometry = chartGeometry(points, 'trips');

  assert.match(geometry.path, /^M /);
  assert.equal(geometry.maxValue, 10);
  assert.equal(geometry.xTicks.length, 3);
  assert.deepEqual(
    geometry.yTicks.map((tick) => tick.value),
    [10, 5, 0],
  );

  assert.equal(
    chartGeometry(
      [
        { trip_hour: '2026-01-01T00:00:00', trips: 0 },
        { trip_hour: '2026-01-01T01:00:00', trips: 0 },
      ],
      'trips',
    ).maxValue,
    0,
  );
});

test('run status distinguishes a published snapshot from failure', () => {
  assert.deepEqual(statusDescriptor({ status: 'success', published_at: '2026-01-01' }), {
    label: 'Published',
    tone: 'success',
    symbol: '✓',
  });
  assert.equal(statusDescriptor({ status: 'failed' }).symbol, '!');
  assert.deepEqual(statusDescriptor({ status: 'failed_quality', published_at: null }), {
    label: 'Quality failed',
    tone: 'failed',
    symbol: '!',
  });
  assert.equal(
    statusDescriptor({ status: 'failed_quality', published_at: '2026-01-01' }).label,
    'Published with failed gates',
  );
  assert.equal(formatDuration(null), '—');
  assert.equal(formatNumber(null), '—');
});

test('failure summaries render only the API safe vocabulary and ignore raw operator errors', () => {
  assert.equal(
    failureSummary({
      failure_summary: 'Replay source or input integrity failure',
      error_message: '/private/raw/source.csv',
    }),
    'Replay source or input integrity failure',
  );
  assert.equal(failureSummary({ failure_summary: '<script>unsafe</script>' }), null);
  assert.equal(failureSummary({ error_message: '/private/raw/source.csv' }), null);
});

test('date-only values are formatted without local timezone drift', () => {
  assert.equal(formatDate('2026-05-29'), '29 May 2026');
});

test('formatters preserve units and explicit missing values', () => {
  assert.equal(formatBytes(1536), '1.5 KB');
  assert.equal(formatDuration(65), '1m 5s');
  assert.equal(formatPercent(0.987, 1), '98.7%');
  assert.equal(formatNumber(undefined), '—');
});

test('API identifiers remain readable without losing run traceability', () => {
  assert.equal(prettyName('failed_quality'), 'Failed Quality');
  assert.equal(formatObserved(null), 'No value');
  assert.equal(shortRunId('20260713093503-b57b5eeb'), 'b57b5eeb');
});

test('drift formatters preserve direction, comparison kind, and missing values', () => {
  assert.equal(formatDriftValue(12.34567), '12.346');
  assert.equal(formatDriftValue(null), '—');
  assert.equal(formatDelta(0.125, 'relative'), '+12.5%');
  assert.equal(formatDelta(-0.025, 'absolute'), '-0.025');
  assert.equal(formatDelta(null, 'relative'), '—');
  assert.equal(formatDriftThreshold(0.2, 'relative'), '±20.0%');
  assert.equal(formatDriftThreshold(0.02, 'absolute'), '±0.02');
});

test('run provenance helpers keep intervals and hashes compact', () => {
  assert.equal(
    runIntervalLabel({ data_interval_start: '2026-07-01', data_interval_end: '2026-07-12' }),
    '01 Jul 2026 – 12 Jul 2026',
  );
  assert.equal(runIntervalLabel({}), 'Data interval —');
  assert.equal(shortHash('abcdef0123456789'), 'abcdef0123');
  assert.equal(shortHash(null), '—');
});
