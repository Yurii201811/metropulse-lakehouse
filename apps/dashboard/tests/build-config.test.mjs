import assert from 'node:assert/strict';
import test from 'node:test';

import { normalizeApiBaseUrl } from '../scripts/build-config.mjs';

test('API base URL normalization preserves safe paths and removes trailing slashes', () => {
  assert.equal(normalizeApiBaseUrl(' https://example.com/metropulse/// '), 'https://example.com/metropulse');
  assert.equal(normalizeApiBaseUrl('http://127.0.0.1:8000'), 'http://127.0.0.1:8000');
});

test('API base URL validation rejects unsafe or malformed public configuration', () => {
  assert.throws(() => normalizeApiBaseUrl('file:///tmp/api'), /http or https/);
  assert.throws(() => normalizeApiBaseUrl('https://user:secret@example.com'), /credentials/);
  assert.throws(() => normalizeApiBaseUrl('https://example.com?token=secret'), /query or fragment/);
  assert.throws(() => normalizeApiBaseUrl('https://example.com/#debug'), /query or fragment/);
  assert.throws(() => normalizeApiBaseUrl('https://example.com/?'), /query or fragment/);
  assert.throws(() => normalizeApiBaseUrl('https://example.com/#'), /query or fragment/);
  assert.throws(() => normalizeApiBaseUrl('not a URL'), /valid absolute URL/);
});
