import { cpSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { normalizeApiBaseUrl } from './build-config.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const projectRoot = resolve(root, '../..');
const dist = resolve(root, 'dist');
const sourceDist = resolve(dist, 'src');
const apiBaseUrl = normalizeApiBaseUrl(
  process.env.METROPULSE_API_BASE_URL || 'http://127.0.0.1:8000',
);

rmSync(dist, { force: true, recursive: true });
mkdirSync(dist, { recursive: true });
mkdirSync(sourceDist, { recursive: true });
cpSync(resolve(root, 'index.html'), resolve(dist, 'index.html'));
for (const file of ['console-app.js', 'console.css', 'dashboard-core.js']) {
  cpSync(resolve(root, 'src', file), resolve(sourceDist, file));
}
cpSync(resolve(projectRoot, 'tokens.css'), resolve(dist, 'tokens.css'));
writeFileSync(
  resolve(dist, 'config.js'),
  `window.METROPULSE_API_BASE_URL = ${JSON.stringify(apiBaseUrl).replaceAll('<', '\\u003c')};\n`,
  'utf8',
);

console.log(`Built dashboard to ${dist} with API ${apiBaseUrl}`);
