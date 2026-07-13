import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildQuery,
  chartGeometry,
  createRequestGate,
  escapeHtml,
  formatBytes,
  formatDate,
  formatDuration,
  formatNumber,
  formatObserved,
  formatPercent,
  prettyName,
  reconcileActiveFilters,
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
