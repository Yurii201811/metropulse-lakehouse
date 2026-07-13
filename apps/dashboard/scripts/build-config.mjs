export function normalizeApiBaseUrl(value) {
  const rawValue = String(value ?? '').trim();
  if (!rawValue) throw new Error('METROPULSE_API_BASE_URL must not be empty.');

  let parsed;
  try {
    parsed = new URL(rawValue);
  } catch {
    throw new Error('METROPULSE_API_BASE_URL must be a valid absolute URL.');
  }

  if (!['http:', 'https:'].includes(parsed.protocol)) {
    throw new Error('METROPULSE_API_BASE_URL must use http or https.');
  }
  if (parsed.username || parsed.password) {
    throw new Error('METROPULSE_API_BASE_URL must not contain credentials.');
  }
  if (rawValue.includes('?') || rawValue.includes('#')) {
    throw new Error('METROPULSE_API_BASE_URL must not contain a query or fragment.');
  }

  return parsed.toString().replace(/\/+$/, '');
}
