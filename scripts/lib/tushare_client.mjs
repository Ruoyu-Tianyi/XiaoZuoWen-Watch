import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

function tokenFromText(text) {
  const patterns = [
    /(?:TUSHARE_)?TOKEN\s*=\s*["']([a-f0-9]{32,})["']/i,
    /```\s*([a-f0-9]{32,})\s*```/i,
  ];
  for (const pattern of patterns) {
    const token = String(text || '').match(pattern)?.[1];
    if (token) return token;
  }
  return '';
}

function tokenFromZip(path) {
  for (const entry of ['tushare_pro_skill/tushare_config.py', 'tushare_pro_skill/SKILL.md']) {
    const result = spawnSync('tar', ['-xOf', path, entry], { encoding: 'utf8', windowsHide: true, maxBuffer: 4 * 1024 * 1024 });
    if (result.status === 0) {
      const token = tokenFromText(result.stdout);
      if (token) return token;
    }
  }
  return '';
}

export function loadTushareToken(config = {}) {
  if (process.env.TUSHARE_TOKEN) return process.env.TUSHARE_TOKEN.trim();
  if (process.env.TUSHARE_TOKEN_FILE) {
    const token = tokenFromText(readFileSync(process.env.TUSHARE_TOKEN_FILE, 'utf8')) || readFileSync(process.env.TUSHARE_TOKEN_FILE, 'utf8').trim();
    if (token) return token;
  }
  const zipPath = process.env.TUSHARE_CREDENTIAL_ZIP || config.credential_zip;
  if (zipPath) {
    const token = tokenFromZip(zipPath);
    if (token) return token;
  }
  throw new Error('未找到 Tushare token，请设置 TUSHARE_TOKEN、TUSHARE_TOKEN_FILE 或 credential_zip');
}

export async function tushareCall({ token, apiName, params = {}, fields = '', timeoutMs = 60000 }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch('https://api.tushare.pro', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ api_name: apiName, token, params, fields }),
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`Tushare HTTP ${response.status}`);
    const payload = await response.json();
    if (Number(payload.code) !== 0) throw new Error(`Tushare ${payload.code}: ${payload.msg || '接口调用失败'}`);
    const names = payload.data?.fields || [];
    return (payload.data?.items || []).map((values) => Object.fromEntries(names.map((name, index) => [name, values[index]])));
  } finally {
    clearTimeout(timeout);
  }
}

export async function fetchAStockMaster(config = {}) {
  const token = loadTushareToken(config);
  const fields = 'ts_code,symbol,name,area,industry,full_name,market,exchange,list_status,list_date,delist_date';
  const rows = [];
  for (const listStatus of ['L', 'D', 'P']) {
    rows.push(...await tushareCall({ token, apiName: 'stock_basic', params: { list_status: listStatus }, fields }));
  }
  const allowed = new Set(config.markets || ['SH', 'SZ', 'BJ']);
  return rows.filter((row) => allowed.has(String(row.ts_code || '').split('.')[1]));
}

export async function fetchHKStockMaster(config = {}) {
  const token = loadTushareToken(config);
  const fields = 'ts_code,name,fullname,market,list_status,list_date,delist_date,curr_type';
  const rows = [];
  for (const listStatus of ['L', 'D', 'P']) {
    rows.push(...await tushareCall({ token, apiName: 'hk_basic', params: { list_status: listStatus }, fields }));
  }
  return rows.map((row) => ({
    ...row,
    full_name: row.fullname || null,
    area: null,
    industry: null,
    exchange: 'HKEX',
  }));
}

function aliasesForSecurity(row) {
  const values = new Set([row.name, row.full_name]);
  const name = String(row.name || '').trim();
  const fullName = String(row.full_name || '').trim();
  if (/^(?:ST|\*ST)/i.test(name)) values.add(name.replace(/^(?:ST|\*ST)/i, ''));
  if (name.endsWith('证券') && name.length > 2) values.add(name.slice(0, -2));
  if (name.endsWith('公司') && name.length > 2) values.add(name.slice(0, -2));
  const shortFullName = fullName.replace(/(?:股份)?有限公司$/u, '');
  if (shortFullName && shortFullName !== fullName) {
    values.add(shortFullName);
    if (shortFullName.endsWith('证券')) values.add(shortFullName.slice(0, -2));
  }
  const commonAliases = {
    '中国人寿': ['国寿'], '中国太保': ['太保'], '中国平安': ['平安'],
    '新华保险': ['新华'], '中国太平': ['太平'], '中信证券': ['中信'],
    '华泰证券': ['华泰'], '中金公司': ['中金'],
  };
  for (const [canonical, aliases] of Object.entries(commonAliases)) {
    if (name === canonical || shortFullName === canonical) for (const alias of aliases) values.add(alias);
  }
  return [...values].map((value) => String(value || '').trim()).filter(Boolean);
}

export function syncSecurityMaster(db, rows) {
  const now = new Date().toISOString();
  const upsert = db.prepare(`
    INSERT INTO securities(ts_code,symbol,market,name,full_name,area,industry,exchange,list_status,list_date,delist_date,source,synced_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,'tushare',?)
    ON CONFLICT(ts_code) DO UPDATE SET symbol=excluded.symbol,market=excluded.market,name=excluded.name,
      full_name=excluded.full_name,area=excluded.area,industry=excluded.industry,exchange=excluded.exchange,
      list_status=excluded.list_status,list_date=excluded.list_date,delist_date=excluded.delist_date,synced_at=excluded.synced_at
  `);
  const find = db.prepare('SELECT id FROM securities WHERE ts_code=?');
  const clearAliases = db.prepare("DELETE FROM security_aliases WHERE security_id=? AND source IN ('tushare','tushare_code')");
  const alias = db.prepare('INSERT OR REPLACE INTO security_aliases(alias,security_id,source,priority) VALUES(?,?,?,?)');
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const row of rows) {
      const [symbol, market] = String(row.ts_code).split('.');
      upsert.run(row.ts_code, symbol, market, row.name || '', row.full_name || null, row.area || null, row.industry || null,
        row.exchange || null, row.list_status || null, row.list_date || null, row.delist_date || null, now);
      const securityId = Number(find.get(row.ts_code).id);
      clearAliases.run(securityId);
      for (const value of aliasesForSecurity(row)) alias.run(value, securityId, 'tushare', value === row.name ? 10 : 20);
      alias.run(row.ts_code, securityId, 'tushare_code', 1);
      alias.run(symbol, securityId, 'tushare_code', 2);
    }
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
  return rows.length;
}
