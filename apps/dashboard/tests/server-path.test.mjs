import assert from 'node:assert/strict';
import { dirname, resolve } from 'node:path';
import { Readable } from 'node:stream';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { resolvePublicFile } from '../scripts/server-path.mjs';
import { createDashboardServer } from '../scripts/serve.mjs';

const dashboardRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');

test('static server resolves files but rejects directories and traversal', () => {
  assert.equal(resolvePublicFile(dashboardRoot, '/index.html'), resolve(dashboardRoot, 'index.html'));
  assert.equal(resolvePublicFile(dashboardRoot, '/src/'), null);
  assert.equal(resolvePublicFile(dashboardRoot, '/../../pyproject.toml'), null);
  assert.equal(resolvePublicFile(dashboardRoot, '/missing.txt'), null);
  assert.equal(resolvePublicFile('/missing/root', '/index.html'), null);
});

async function listen(server) {
  await new Promise((resolveListening) => server.listen(0, '127.0.0.1', resolveListening));
  const address = server.address();
  return `http://127.0.0.1:${address.port}`;
}

async function close(server) {
  await new Promise((resolveClosed, rejectClosed) => {
    server.close((error) => (error ? rejectClosed(error) : resolveClosed()));
  });
}

test('HTTP server waits for a readable asset before sending 200', async () => {
  const server = createDashboardServer({ publicRoot: dashboardRoot });
  const baseUrl = await listen(server);
  try {
    assert.equal((await fetch(`${baseUrl}/index.html`)).status, 200);
    assert.equal((await fetch(`${baseUrl}/index.html`, { method: 'HEAD' })).status, 200);
    assert.equal((await fetch(`${baseUrl}/src/`)).status, 404);
    assert.equal((await fetch(`${baseUrl}/index.html`, { method: 'POST' })).status, 405);
  } finally {
    await close(server);
  }
});

test('HTTP server returns 500 when an existing asset cannot be opened', async () => {
  const server = createDashboardServer({
    publicRoot: dashboardRoot,
    createFileStream() {
      const stream = new Readable({ read() {} });
      queueMicrotask(() => stream.destroy(new Error('simulated open failure')));
      return stream;
    },
  });
  const baseUrl = await listen(server);
  try {
    const response = await fetch(`${baseUrl}/index.html`);
    assert.equal(response.status, 500);
    assert.equal(await response.text(), 'Unable to read asset');
  } finally {
    await close(server);
  }
});
