import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const masterPath = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
const endpoint = 'https://www.sec.gov/files/company_tickers.json';
const response = await fetch(endpoint, { headers: { 'user-agent': 'broker-note-dashboard/1.0 local research', accept: 'application/json' } });
if (!response.ok) throw new Error(`SEC ticker master HTTP ${response.status}`);
const payload = await response.json();
const master = JSON.parse(readFileSync(masterPath, 'utf8'));
const byKey = new Map((master.securities || []).map((row) => [`${row.market}:${row.code}`, row]));
let merged = 0;
for (const row of Object.values(payload || {})) {
  const code = String(row.ticker || '').trim().toUpperCase();
  const name = String(row.title || '').trim();
  if (!code || !name) continue;
  byKey.set(`US:${code}`, { code, name, market: 'US' });
  merged += 1;
}
master.securities = [...byKey.values()];
master.count = master.securities.length;
master.fetched_at = new Date().toISOString();
master.sources = { ...(master.sources || {}), sec_company_tickers: endpoint };
writeFileSync(masterPath, `${JSON.stringify(master, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ source: endpoint, us_rows: merged, total: master.count, output: masterPath }, null, 2));
