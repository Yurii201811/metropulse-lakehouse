import { realpathSync, statSync } from 'node:fs';
import { resolve, sep } from 'node:path';

export function resolvePublicFile(publicRoot, pathname) {
  try {
    const candidate = resolve(publicRoot, `.${decodeURIComponent(pathname)}`);
    if (!statSync(candidate).isFile()) return null;

    const canonicalRoot = realpathSync(publicRoot);
    const canonicalFile = realpathSync(candidate);
    const insideRoot = canonicalFile.startsWith(`${canonicalRoot}${sep}`);
    return insideRoot ? canonicalFile : null;
  } catch {
    return null;
  }
}
