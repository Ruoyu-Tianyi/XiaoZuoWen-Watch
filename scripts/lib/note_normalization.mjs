import { repairExactEvidence } from './model_client.mjs';

const safeDecode = text => { try { return decodeURIComponent(text); } catch { return text; } };
const decodeEntity = token => {
  const numeric = token.match(/^&#(x[\da-f]+|\d+);$/iu);
  if (numeric) {
    const value = numeric[1][0].toLowerCase() === 'x' ? parseInt(numeric[1].slice(1), 16) : Number(numeric[1]);
    return value > 0 && value <= 0x10ffff && !(value >= 0xd800 && value <= 0xdfff) ? String.fromCodePoint(value) : token;
  }
  return ({ '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'", '&nbsp;': ' ' }[token.toLowerCase()] || token);
};
const decodeLabel = value => safeDecode(value).replace(/&(?:amp|lt|gt|quot|apos|nbsp|#\d+|#x[\da-f]+);/giu, decodeEntity);
export function readableText(source) {
  const text = String(source || '');
  const offsets = [], ends = [];
  let plain = '', position = 0;
  for (const match of text.matchAll(/<e\b[^>]*\/?>(?:<\/e>)?|<\/e>|&(?:amp|lt|gt|quot|apos|nbsp|#\d+|#x[\da-f]+);/giu)) {
    for (; position < match.index; position++) { plain += text[position]; offsets.push(position); ends.push(position + 1); }
    const token = match[0];
    const title = token.match(/(?:title|text|name)\s*=\s*["']([^"']*)["']/iu)?.[1];
    const replacement = token.startsWith('<') ? (title ? decodeLabel(title) : '') : decodeEntity(token);
    for (let i = 0; i < replacement.length; i++) { plain += replacement[i]; offsets.push(position); ends.push(position + token.length); }
    position += token.length;
  }
  for (; position < text.length; position++) { plain += text[position]; offsets.push(position); ends.push(position + 1); }
  return { text: plain, offsets, ends };
}

export const sourceText = topic => `${topic.title || ''}\n${topic.content || ''}`;
export function originalEvidence(topic, value) {
  const repaired = repairExactEvidence(topic, value);
  if (repaired) return repaired;
  const raw = sourceText(topic), normalized = readableText(raw);
  const evidence = repairExactEvidence({ title: '', content: normalized.text }, value);
  const start = evidence ? normalized.text.indexOf(evidence) : -1;
  if (start < 0) return null;
  return raw.slice(normalized.offsets[start], normalized.ends[start + evidence.length - 1]);
}

export const normalizeName = name => String(name || '').trim().replace(/\s+/gu, '')
  .replace(/(?:[-－](?:WVR|SW|SS|B|W|P|S|R))+$/iu, '').replace(/^(?:\*ST|ST)(?=\p{Script=Han})/iu, '');
const foreignOrProduct = /^(?:黄金|白银|纸类|摩托车|家具及其零件|越南家具产品|.*凝胶贴膏|.*制铺地制品|.*金属框架坐具|.*制餐具及厨房用具|.*狗食或猫食饲料|.*北美温控入口)$/u;
const escapeRegex = value => value.replace(/[.*+?^${}()|[\]\\]/gu, '\\$&');
function splitScope(surface) {
  const raw = String(surface || '').trim();
  // A trailing Latin A/H can be part of a company name (e.g. ALPHA).
  const match = raw.match(/^(.*?)(?:\s+(AH|A|H)|[（(](AH|A|H)[）)]|(?<=\p{Script=Han})(AH|A|H))$/iu);
  return { base: match ? match[1].trim() : raw, scope: (match?.[2] || match?.[3] || match?.[4] || '').toUpperCase() };
}
function sameCompany(a, h) {
  const keys = security => [security.name, security.full_name].filter(Boolean).map(name => normalizeName(name)
    .replace(/(?:股份有限公司|有限责任公司|有限公司)$/u, ''));
  return keys(a).some(key => keys(h).includes(key));
}

export function securityResolver(db) {
  const securities = db.prepare('SELECT * FROM securities').all();
  const byId = new Map(securities.map(s => [s.id, s])), names = new Map();
  const add = (name, security) => {
    const key = normalizeName(name);
    if (!key || !security || !['SH','SZ','BJ','HK'].includes(security.market)) return;
    if (!names.has(key)) names.set(key, new Map());
    names.get(key).set(security.id, security);
  };
  for (const s of securities) { add(s.name, s); add(s.full_name, s); }
  for (const a of db.prepare("SELECT alias,security_id FROM security_aliases WHERE source<>'tushare_code'").all()) {
    if (!/^\d+(?:\.(?:SH|SZ|BJ|HK))?$/iu.test(a.alias)) add(a.alias, byId.get(a.security_id));
  }
  return (surface, hint, topic) => {
    const { base, scope: suppliedScope } = splitScope(surface);
    let scope = suppliedScope;
    const original = topic ? readableText(sourceText(topic)).text : null;
    const explicitScopes = new Set(original && base ? [...original.matchAll(new RegExp(`${escapeRegex(base)}(?:\\s*(AH|A|H)(?![A-Za-z])|[（(](AH|A|H)[）)])`, 'giu'))]
      .map(match => (match[1] || match[2]).toUpperCase()) : []);
    if (!scope && explicitScopes.size === 1) scope = [...explicitScopes][0];
    const code = String(hint || '').trim().toUpperCase();
    let candidates = [...(names.get(normalizeName(base))?.values() || [])].filter(Boolean);
    if (!candidates.length && /^\d{5,6}(?:\.(?:SH|SZ|BJ|HK))?$/iu.test(base)) candidates = securities.filter(s => s.ts_code === base.toUpperCase() || s.symbol === base);
    if (foreignOrProduct.test(base) && !candidates.length) return { status: 'out_of_scope', securities: [], reason: '明确为商品、药品或产品，并非证券对象' };
    if (/\.US$/u.test(code) && !candidates.length) return { status: 'out_of_scope', securities: [], reason: '明确美股代码不在范围内' };
    if (original && ((suppliedScope && !explicitScopes.has(suppliedScope)) || explicitScopes.size > 1)) {
      return { status: 'ambiguous', securities: [], candidates, reason: '市场范围标记缺少原文支持或存在冲突' };
    }
    const codeMatch = code.match(/^(\d{1,6})\.(SH|SZ|BJ|HK)$/u);
    const normalizedCode = codeMatch ? `${codeMatch[1].padStart(codeMatch[2] === 'HK' ? 5 : 6, '0')}.${codeMatch[2]}` : null;
    const a = candidates.filter(s => ['SH','SZ','BJ'].includes(s.market)), h = candidates.filter(s => s.market === 'HK');
    const selected = scope === 'AH' ? [...a, ...h] : scope === 'H' ? h : scope === 'A' ? a : a.length ? a : h;
    const matchesHint = security => normalizedCode ? security.ts_code === normalizedCode
      : /^\d{1,6}$/u.test(code) && security.symbol === code.padStart(security.market === 'HK' ? 5 : 6, '0');
    // Scope comes from the article, never from a model's code hint. A/H-only names
    // can resolve directly; when dual listed, an unmarked name defaults to A.
    if (code && candidates.length && !selected.some(matchesHint)) {
      return { status: 'ambiguous', securities: [], candidates: selected.length ? selected : candidates, reason: '证券名称或原文市场范围与代码提示不一致' };
    }
    const complete = scope === 'AH' ? a.length === 1 && h.length === 1 && sameCompany(a[0], h[0]) : selected.length === 1;
    if (complete) return { status: 'resolved', securities: selected, method: scope === 'AH' ? 'tushare_ah_exact' : 'tushare_name_exact', reason: null };
    return { status: candidates.length ? 'ambiguous' : 'unresolved', securities: [], candidates, reason: candidates.length ? '证券名称或AH关联不唯一/不完整' : '证券主表无唯一匹配，需带来源核验' };
  };
}

const BROKERS = {
  '长江证券': ['长江'], '开源证券': ['开源'], '中信证券': ['中信'], '中信建投': ['中信建投证券','建投'],
  '申万宏源': ['申万宏源证券','申万'], '中国银河': ['银河证券','中国银河证券','银河'],
  '华泰证券': ['华泰'], '中泰证券': ['中泰'], '天风证券': ['天风'], '中金公司': ['中金'],
  '国泰海通': ['国泰海通证券'], '国投证券': ['国投'], '浙商证券': ['浙商'], '广发证券': ['广发'],
};
export function brokerSignatureMatches(broker, signature) {
  const supplied = String(broker || '').trim();
  if (!supplied) return false;
  const text = readableText(signature).text;
  const entries = Object.entries(BROKERS).flatMap(([canonical, aliases]) => [canonical, ...aliases].map(alias => ({ canonical, alias })));
  const canonical = entries.find(row => row.alias === supplied)?.canonical || supplied;
  const aliases = BROKERS[canonical] ? [canonical, ...BROKERS[canonical]] : [canonical.replace(/(?:股份有限公司|有限责任公司|有限公司)$/u, '')];
  return aliases.some(alias => {
    if (alias.length < 2) return false;
    for (let start = text.indexOf(alias); start >= 0; start = text.indexOf(alias, start + 1)) {
      const shadowed = entries.some(other => {
        if (other.canonical === canonical || other.alias.length <= alias.length) return false;
        for (let at = text.indexOf(other.alias); at >= 0; at = text.indexOf(other.alias, at + 1)) {
          if (at <= start && at + other.alias.length >= start + alias.length) return true;
        }
        return false;
      });
      if (!shadowed) return true;
    }
    return false;
  });
}
export function normalizeSource(topic, recommendation = {}) {
  const evidence = originalEvidence(topic, recommendation.evidence);
  const issues = [];
  if (!evidence) return { broker: null, group: '其他', authors: [], evidence: null,
    issues: recommendation.broker || recommendation.research_group ? ['推荐方缺少原文署名证据'] : [] };
  const evidenceText = readableText(evidence).text;
  const supplied = String(recommendation.broker || '').trim();
  const matches = Object.entries(BROKERS).flatMap(([canonical, aliases]) => [canonical, ...aliases].map(alias => ({ canonical, alias, aliases: [canonical,...aliases] })))
    .sort((a,b) => b.alias.length - a.alias.length);
  const entry = matches.find(row => supplied.startsWith(row.alias) && brokerSignatureMatches(row.canonical, evidenceText));
  const aliases = entry?.aliases || [supplied];
  let broker = supplied && brokerSignatureMatches(entry?.canonical || supplied, evidenceText) ? entry?.canonical || supplied : null;
  const suppliedGroup = String(recommendation.research_group || '').trim();
  let group = (suppliedGroup === '其他' ? '' : suppliedGroup) || (entry ? supplied.slice(entry.alias.length).replace(/^(?:研究所|研究|团队)/u, '').trim() : '');
  if (group) for (const alias of aliases.sort((a,b) => b.length-a.length)) if (alias && group.startsWith(alias)) { group = group.slice(alias.length).trim(); break; }
  group = group.replace(/^(?:证券)?(?:研究所|研究团队|研究组)/u, '').trim();
  if (group && !evidenceText.includes(group)) { issues.push('研究组缺少原文署名支持'); group = ''; }
  // Bare occurrences of listed brokers in the article are not a signature. Check
  // the source line for brackets, source attribution, or an explicit research team.
  const lines = readableText(sourceText(topic)).text.split(/\r?\n/u);
  const sourceLines = lines.filter(line => line.includes(evidenceText) || evidenceText.includes(line) && line.trim());
  const signed = aliases.filter(Boolean).some(alias => sourceLines.some(line =>
    new RegExp(`(?:【|\\[|来源[：:]?\\s*|出品[：:]?\\s*|作者[：:]?\\s*)${escapeRegex(alias)}|${escapeRegex(alias)}(?:证券)?(?:研究所|研究组|研究团队)`, 'u').test(line)
    || group && line.includes(alias + group)));
  if (broker && !signed) { issues.push('券商名称出现但无法确认是署名'); broker = null; group = ''; }
  if (supplied && !broker && !issues.length) issues.push('券商缺少原文署名支持');
  return { broker, group: group || '其他', authors: (Array.isArray(recommendation.authors) ? recommendation.authors : []).map(String)
    .filter(author => author.trim() && evidenceText.includes(author)), evidence: broker || group ? evidence : null, issues };
}

const CURRENCIES = new Set(['CNY','HKD','USD','EUR','GBP','JPY','SGD','AUD','CHF','CAD']);
function normalizedTarget(raw, resolution, topic) {
  const issues = [], notes = [];
  let price = raw.target_price == null || raw.target_price === '' ? null : Number(raw.target_price);
  let evidence = originalEvidence(topic, raw.target_price_evidence);
  let verbatim = originalEvidence(topic, raw.target_price_raw);
  const evidenceText = readableText(evidence || '').text, rawText = readableText(verbatim || raw.target_price_raw || '').text;
  let currency = String(raw.target_currency || '').trim().toUpperCase();
  if (price != null && Number.isFinite(price)) {
    const amount = String(price).replace('.', '\\.');
    const capAmount = new RegExp(`(?:^|[^\\d.])${amount}(?:\\.0+)?\\s*(?:万亿|亿|万)(?:元|港元|港币|美元|人民币)?`, 'u');
    const shareAmounts = [...evidenceText.matchAll(/(?:目标价|目标价格|股价|每股(?:价值|估值)?|合理价)[^\d\n。；]{0,14}([\d,]+(?:\.\d+)?)(?:\s*[-~至]\s*([\d,]+(?:\.\d+)?))?/gu)]
      .flatMap(match => [match[1],match[2]].filter(Boolean)).map(number => Number(number.replaceAll(',', '')));
    if (capAmount.test(rawText) || capAmount.test(evidenceText) && !shareAmounts.includes(price) || /^(?:亿元|亿港元|亿美元|万亿元)$/u.test(currency)) {
      notes.push('模型提供的是总市值金额，已保留原始结果并清空每股目标价字段');
      return { price: null, currency: null, evidence: null, verbatim: null, issues, notes };
    }
  }
  if (price == null) {
    if (raw.target_currency || raw.target_price_raw || raw.target_price_evidence) issues.push('存在目标价附属字段但缺少价格数值');
    return { price: null, currency: null, evidence: null, verbatim: null, issues, notes };
  }
  const currencyAliases = { '人民币':'CNY','人民币元':'CNY','RMB':'CNY','港元':'HKD','港币':'HKD','HK$':'HKD','美元':'USD','US$':'USD' };
  currency = currencyAliases[currency] || currency;
  const explicitCurrencies = text => [['HKD', /港元|港币|HK\$/iu], ['CNY', /人民币|RMB|CNY/iu], ['USD', /美元|US\$/iu]]
    .filter(([,pattern]) => pattern.test(text)).map(([value]) => value);
  const rawCurrencies = explicitCurrencies(rawText);
  const explicit = rawCurrencies.length ? rawCurrencies : explicitCurrencies(evidenceText);
  if (currency === '元' || currency === '') {
    const markets = new Set((resolution.securities || []).map(security => security.market));
    if (explicit.length === 1) currency = explicit[0];
    else if (explicit.length > 1) currency = '';
    else if (markets.size && !markets.has('HK')) currency = 'CNY';
    else if (markets.size === 1 && markets.has('HK')) currency = 'HKD';
    else currency = '';
  }
  if (explicit.length === 1 && CURRENCIES.has(currency) && explicit[0] !== currency) issues.push('目标价币种与原文明确标注不一致');
  const numbers = evidenceText.match(/\d+(?:,\d{3})*(?:\.\d+)?/gu) || [];
  if (!(price > 0) || !Number.isFinite(price) || !evidence || !CURRENCIES.has(currency)
      || !numbers.some(number => Number(number.replaceAll(',', '')) === price)
      || raw.target_price_raw && !verbatim) issues.push('目标价/币种或精确证据待核实');
  return { price, currency: currency || null, evidence, verbatim, issues, notes };
}

export function normalizeView(topic, raw, resolver) {
  const surface = String(raw.security_surface || '').trim();
  const readable = readableText(sourceText(topic)).text;
  const baseSurface = splitScope(surface).base;
  const evidence = originalEvidence(topic, raw.evidence), securityEvidence = originalEvidence(topic, raw.security_evidence);
  const issues = [];
  if (!surface || !baseSurface || !readable.includes(baseSurface)) issues.push('证券名称不在原文');
  if (!evidence || !securityEvidence) issues.push('观点或标的证据不在原文');
  if (securityEvidence && baseSurface && !readableText(securityEvidence).text.includes(baseSurface)) issues.push('标的证据未包含当前证券名称');
  if (!['bullish','bearish'].includes(raw.direction)) issues.push('方向不明确');
  const resolution = resolver(surface, raw.security_code_hint, topic);
  if (resolution.status !== 'resolved') issues.push(resolution.reason);
  const target = normalizedTarget(raw, resolution, topic);
  issues.push(...target.issues);
  return { raw, surface, evidence: evidence || '', securityEvidence: securityEvidence || '', resolution, issues,
    summary: raw.summary, direction: raw.direction, confidence: raw.confidence, targetPrice: target.price,
    targetCurrency: target.currency, targetRaw: target.verbatim, targetEvidence: target.evidence, normalizationNotes: target.notes };
}
