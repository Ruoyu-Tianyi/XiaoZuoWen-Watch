import { readFileSync, renameSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const endpoint = 'https://www.bse.cn/nqxxController/nqxxCnzq.do';
const output = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
let accessCookie = '';

async function fetchPage(page) {
  const body = new URLSearchParams({
    page: String(page), typejb: 'T', xxzqdm: '', sortfield: 'xxzqdm', sorttype: 'asc',
  });
  body.append('xxfcbj[]', '2');
  const request = () => fetch(endpoint, {
    method: 'POST',
    redirect: 'manual',
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'User-Agent': 'Mozilla/5.0',
      ...(accessCookie ? { Cookie: accessCookie } : {}),
    },
    body,
  });
  let response = await request();
  if (response.status === 307) {
    accessCookie = String(response.headers.get('set-cookie') || '').split(';')[0];
    if (!accessCookie) throw new Error('北交所访问校验未返回 Cookie');
    response = await request();
  }
  if (!response.ok) throw new Error(`北交所接口 HTTP ${response.status}`);
  const jsonp = await response.text();
  const start = jsonp.indexOf('(');
  const end = jsonp.lastIndexOf(')');
  if (start < 0 || end <= start) throw new Error('北交所接口返回格式异常');
  return JSON.parse(jsonp.slice(start + 1, end))[0];
}

const first = await fetchPage(0);
const rows = [...(first.content || [])];
const totalPages = Number(first.totalPages || 1);
for (let page = 1; page < totalPages; page += 1) rows.push(...((await fetchPage(page)).content || []));
const bj = rows.map((row) => ({ code: String(row.xxzqdm), name: String(row.xxzqjc || '').trim(), market: 'BJ' }))
  .filter((security) => /^920\d{3}$/.test(security.code) && security.name);
const byKey = new Map(bj.map((security) => [`BJ:${security.code}`, security]));
if (byKey.size !== Number(first.totalElements)) throw new Error(`北交所清单不完整: expected=${first.totalElements}, actual=${byKey.size}`);

const existing = JSON.parse(readFileSync(output, 'utf8'));
const securities = [...(existing.securities || []).filter((security) => security.market !== 'BJ'), ...byKey.values()];
const payload = { ...existing, fetched_at: new Date().toISOString(), count: securities.length, securities };
const temporary = `${output}.tmp`;
writeFileSync(temporary, JSON.stringify(payload, null, 2), 'utf8');
renameSync(temporary, output);
console.log(JSON.stringify({ output, bj: byKey.size, total: securities.length }));
