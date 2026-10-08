import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { canonicalMarket, STANDARD_MARKETS } from './lib/security_market.mjs';

const output = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
const payload = JSON.parse(readFileSync(output, 'utf8'));
let changed = 0;
const normalized = (payload.securities || []).map((security) => {
  const market = canonicalMarket(security.market, security.code);
  if (!STANDARD_MARKETS.has(market)) throw new Error(`无法规范市场: ${security.code}/${security.market}`);
  if (market !== security.market) changed += 1;
  return { ...security, market };
});
const byKey = new Map();
for (const security of normalized) {
  const key = `${security.market}:${security.code}`;
  const existing = byKey.get(key);
  if (existing && existing.name !== security.name) throw new Error(`证券主表键冲突: ${key} = ${existing.name}/${security.name}`);
  byKey.set(key, security);
}
const securities = [...byKey.values()];
const deduplicated = normalized.length - securities.length;

const temporary = `${output}.tmp`;
writeFileSync(temporary, JSON.stringify({ ...payload, count: securities.length, securities }, null, 2), 'utf8');
renameSync(temporary, output);
console.log(JSON.stringify({ output, count: securities.length, normalized: changed, deduplicated }));
