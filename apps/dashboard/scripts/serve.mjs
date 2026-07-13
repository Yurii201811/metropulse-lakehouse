import { createReadStream, existsSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, resolve } from 'node:path';
import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

import { resolvePublicFile } from './server-path.mjs';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const dist = resolve(root, 'dist');
const defaultPublicRoot = existsSync(dist) ? dist : root;
const host = process.env.HOST || '127.0.0.1';
const port = Number(process.env.PORT || 5173);

const contentTypes = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.png': 'image/png',
  '.svg': 'image/svg+xml',
};

export function createDashboardServer({
  publicRoot = defaultPublicRoot,
  createFileStream = createReadStream,
} = {}) {
  return createServer((request, response) => {
    if (!['GET', 'HEAD'].includes(request.method || 'GET')) {
      response.writeHead(405, { Allow: 'GET, HEAD' });
      response.end('Method not allowed');
      return;
    }

    let requestUrl;
    try {
      requestUrl = new URL(request.url || '/', 'http://localhost');
    } catch {
      response.writeHead(400);
      response.end('Bad request');
      return;
    }
    const pathname = requestUrl.pathname === '/' ? '/index.html' : requestUrl.pathname;
    const filePath = resolvePublicFile(publicRoot, pathname);
    if (!filePath) {
      response.writeHead(404);
      response.end('Not found');
      return;
    }

    const stream = createFileStream(filePath);
    stream.once('error', (error) => {
      if (!response.headersSent) {
        response.writeHead(500);
        response.end('Unable to read asset');
      } else {
        response.destroy(error);
      }
    });
    stream.once('open', () => {
      response.writeHead(200, {
        'Content-Type': contentTypes[extname(filePath)] || 'application/octet-stream',
        'X-Content-Type-Options': 'nosniff',
      });
      if (request.method === 'HEAD') {
        stream.destroy();
        response.end();
        return;
      }
      stream.pipe(response);
    });
  });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  createDashboardServer().listen(port, host, () => {
    console.log(`MetroPulse dashboard serving http://${host}:${port}`);
  });
}
