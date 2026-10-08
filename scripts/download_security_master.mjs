import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const endpoint = 'https://82.push2.eastmoney.com/api/qt/clist/get';

async function fetchJson(url, market, page) {
  let lastError;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    try {
      const response = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return await response.json();
    } catch (error) {
      lastError = error;
      if (attempt < 3) await new Promise((resolvePromise) => setTimeout(resolvePromise, [1000, 3000, 8000][attempt]));
    }
  }
  throw new Error(`${market} page ${page}: ${lastError?.message || 'request failed'}`);
}

async function fetchMarket(market, fs) {
  const rows = [];
  const pageSize = 100;
  let page = 1;
  let total = Infinity;
  while (rows.length < total) {
    const url = new URL(endpoint);
    const params = {
      pn: String(page), pz: String(pageSize), po: '1', np: '1', fltt: '2', invt: '2', fid: 'f3',
      fs, fields: 'f12,f14',
    };
    for (const [key, value] of Object.entries(params)) url.searchParams.set(key, value);
    const payload = await fetchJson(url, market, page);
    const batch = payload?.data?.diff || [];
    total = Number(payload?.data?.total || 0);
    rows.push(...batch);
    if (!batch.length) break;
    page += 1;
  }
  return rows.map((item) => ({
      code: String(item.f12),
      name: String(item.f14 || '').trim(),
      market,
    })).filter((item) => item.code && item.name && item.name !== '-');
}

const groups = [
  ['SZ', 'm:0+t:6,m:0+t:80'],
  ['SH', 'm:1+t:2,m:1+t:23'],
  ['BJ', 'm:0+t:81+s:2048'],
  ['HK', 'm:116+t:3,m:116+t:4,m:116+t:1,m:116+t:2'],
];

const securities = [];
for (const [market, fs] of groups) securities.push(...await fetchMarket(market, fs));

const output = resolve('data/security_master.json');
writeFileSync(output, JSON.stringify({ fetched_at: new Date().toISOString(), count: securities.length, securities }, null, 2));
console.log(JSON.stringify({ output, count: securities.length }));
