export const STANDARD_MARKETS = new Set(['SH', 'SZ', 'BJ', 'HK', 'US']);

export function mainlandMarketFromCode(code) {
  const value = String(code || '').trim();
  if (!/^\d{6}$/.test(value)) return null;
  if (value.startsWith('920')) return 'BJ';
  if (/^[69]/.test(value)) return 'SH';
  if (/^[023]/.test(value)) return 'SZ';
  return null;
}

export function canonicalMarket(market, code) {
  const value = String(market || '').trim().toUpperCase();
  if (value === 'A') return mainlandMarketFromCode(code) || value;
  return value;
}

export function securityKey(market, code) {
  return `${canonicalMarket(market, code)}:${String(code || '').trim()}`;
}
