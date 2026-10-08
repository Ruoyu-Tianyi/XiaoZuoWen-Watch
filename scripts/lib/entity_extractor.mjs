import { classifyDirection, getDirectionPatterns } from './direction_rules.mjs';
import { canonicalMarket } from './security_market.mjs';

const brokerNames = [
  '国联民生', '中信建投', '申万宏源', '国泰海通', '东方证券', '中信证券', '华泰证券',
  '中金', '中信', '申万', '国金', '天风', '财通', '东吴', '广发', '国盛', '长江',
  '招商', '东北', '华泰', '东方', '中泰', '国海', '国信', '兴业', '兴证', '华创',
  '开源', '德邦', '民生', '方正', '浙商', '华福', '华源', '东财', '中邮', '中银',
  '华西', '西部', '国联', '光大', '银河', '首创', '平安', '瑞银', '摩根', '信达',
  '浦银国际', '浦银', 'CICC',
];
const brokerCanonical = {
  CICC: '中金公司', 中金: '中金公司', 中信: '中信证券', 申万: '申万宏源证券', 申万宏源: '申万宏源证券',
  国泰海通: '国泰海通证券', 国金: '国金证券', 天风: '天风证券', 财通: '财通证券', 东吴: '东吴证券',
  广发: '广发证券', 国盛: '国盛证券', 长江: '长江证券', 招商: '招商证券', 东北: '东北证券',
  华泰: '华泰证券', 东方: '东方证券', 中泰: '中泰证券', 国海: '国海证券', 国信: '国信证券',
  兴证: '兴业证券', 兴业: '兴业证券', 华创: '华创证券', 开源: '开源证券', 德邦: '德邦证券',
  民生: '民生证券', 方正: '方正证券', 浙商: '浙商证券', 华福: '华福证券', 东财: '东方财富证券',
  中邮: '中邮证券', 中银: '中银证券', 华西: '华西证券', 西部: '西部证券', 国联: '国联民生证券',
  国联民生: '国联民生证券', 光大: '光大证券', 银河: '中国银河证券', 浦银: '浦银国际',
};
const sourceStop = /^(事件|核心观点|投资建议|风险提示|符合预期|观点|结论|摘要|更新|点评|逻辑|能力|背景|催化|受益|推荐|目标价|盈利预测|估值|人形机器人|具身智能|火箭链|卫星链|应用侧|AI|IDM)/i;
const ambiguousNames = new Set(['机器人', '指南针', '值得买', '我爱我家', '每日互动', '天下秀', '同花顺', '孩子王', '中国黄金']);
const brokerSecurityNames = new Set(['国泰海通', '申万宏源', '中信建投', '中信证券', '东方证券', '华泰证券', '国联民生', '招商证券', '国金证券', '兴业证券', '光大证券', '中国银河']);
const companyContextPattern = /公司|个股|标的|龙头|推荐|关注|受益|评级|目标价|市值|估值|股价|收入|营收|利润|净利|业绩|毛利率|订单|出货|产能|业务|产品|客户|份额|PE|PB/i;

function escapeRegex(text) {
  return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

export function normalizedAlias(name) {
  return String(name || '')
    .replace(/^\*?ST/i, '')
    .replace(/-(?:WR|SW|W|R|B|S|Z|U)$/i, '')
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (ch) => String.fromCharCode(ch.charCodeAt(0) - 0xfee0))
    .trim();
}

function normalizeMarket(suffix, code = '') {
  return canonicalMarket(suffix, code);
}

function normalizeCode(code, market) {
  const value = String(code || '').trim();
  if (market === 'HK') return value.padStart(5, '0');
  return value;
}

function contextAround(text, index, length, radius = 120) {
  return text.slice(Math.max(0, index - radius), Math.min(text.length, index + length + radius)).replace(/\s+/g, ' ').trim();
}

function isInsideDecoratedBracket(text, index, length) {
  const open = text.lastIndexOf('【', index);
  const close = text.indexOf('】', index + length);
  return open >= 0 && close >= index && close - open <= 80;
}

function createMultiMatcher(patterns) {
  const root = { next: new Map(), fail: null, outputs: [] };
  root.fail = root;
  for (const pattern of patterns) {
    let node = root;
    for (const character of pattern) {
      if (!node.next.has(character)) node.next.set(character, { next: new Map(), fail: root, outputs: [] });
      node = node.next.get(character);
    }
    node.outputs.push(pattern);
  }
  const queue = [...root.next.values()];
  for (let cursor = 0; cursor < queue.length; cursor += 1) {
    const node = queue[cursor];
    for (const [character, child] of node.next) {
      let fallback = node.fail;
      while (fallback !== root && !fallback.next.has(character)) fallback = fallback.fail;
      if (fallback.next.has(character) && fallback.next.get(character) !== child) fallback = fallback.next.get(character);
      child.fail = fallback;
      child.outputs.push(...fallback.outputs);
      queue.push(child);
    }
  }
  return function* find(text) {
    let node = root;
    for (let index = 0; index < text.length; index += 1) {
      const character = text[index];
      while (node !== root && !node.next.has(character)) node = node.fail;
      if (node.next.has(character)) node = node.next.get(character);
      for (const pattern of node.outputs) yield { 0: pattern, index: index - pattern.length + 1 };
    }
  };
}

export function createEntityExtractor(master, pipelineConfig) {
  const { positive: positivePattern, negative: negativePattern } = getDirectionPatterns(pipelineConfig);
  const brokerPattern = new RegExp([...brokerNames].sort((a, b) => b.length - a.length).map(escapeRegex).join('|'));
  const securityByKey = new Map();
  const aliasCandidates = new Map();

  function addAlias(alias, security, manualAlias = false, rejectFollowing = '') {
    if (!alias) return;
    if (!aliasCandidates.has(alias)) aliasCandidates.set(alias, []);
    aliasCandidates.get(alias).push({ ...security, manualAlias, rejectFollowing });
  }

  for (const security of master.securities || []) {
    const code = String(security.code);
    const normalized = { ...security, code, market: canonicalMarket(security.market, code) };
    securityByKey.set(`${normalized.market}:${normalized.code}`, normalized);
    const alias = normalizedAlias(normalized.name);
    if (alias.length >= 3) addAlias(alias, normalized);
  }
  for (const item of pipelineConfig.entity?.aliases || []) {
    const market = normalizeMarket(item.market, item.code);
    const code = normalizeCode(item.code, market);
    const security = securityByKey.get(`${market}:${code}`);
    if (security) addAlias(item.alias, security, true, item.reject_following || '');
  }

  const findAliases = createMultiMatcher([...aliasCandidates.keys()]);

  function extractSource(text) {
    const candidates = [];
    for (const match of text.matchAll(/【([^】]{2,50})】/g)) candidates.push(match[1].trim());
    for (const line of text.split(/\r?\n/).slice(0, 8)) candidates.push(line.trim());
    for (const line of text.split(/\r?\n/).slice(-6)) candidates.push(line.trim());
    for (const candidate of candidates) {
      const brokerMatch = candidate.match(brokerPattern);
      if (!brokerMatch || sourceStop.test(candidate)) continue;
      const broker = brokerMatch[0];
      const after = candidate.slice((brokerMatch.index || 0) + broker.length)
        .replace(/^[证券研究所团队·｜|&\s:：-]+/, '')
        .replace(/[#].*$/, '')
        .trim();
      return { broker: brokerCanonical[broker] || broker, team: after.slice(0, 60), raw: candidate.slice(0, 120) };
    }
    return { broker: '', team: '', raw: '' };
  }

  function resolveNumericTicker(code, suffix) {
    const market = normalizeMarket(suffix, code);
    const normalizedCode = normalizeCode(code, market);
    if (['SH', 'SZ', 'BJ'].includes(market) && normalizedCode.length !== 6) return null;
    return securityByKey.get(`${market}:${normalizedCode}`) || null;
  }

  function explicitTickerMentions(text) {
    const rows = [];
    const seen = new Set();
    const patterns = [
      /(?:\b|[$＄])(?:(SH|SZ|BJ|HK)\s*[:.]?\s*(\d{4,6})|(\d{4,6})\s*\.?\s*(HK|SZ|SH|BJ))\b/gi,
      /([A-Z]{1,6})\.US\s*[（(]([^）)\n]{2,30})[）)]/gi,
    ];
    for (const match of text.matchAll(patterns[0])) {
      const suffix = match[1] || match[4];
      const code = match[2] || match[3];
      const security = resolveNumericTicker(code, suffix);
      if (!security) continue;
      const key = `${security.market}:${security.code}`;
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push({ ...security, alias: match[0].trim(), index: match.index || 0, explicit: true });
    }
    for (const match of text.matchAll(patterns[1])) {
      const key = `US:${match[1].toUpperCase()}`;
      if (seen.has(key)) continue;
      seen.add(key);
      rows.push({ code: match[1].toUpperCase(), market: 'US', name: match[2].trim(), alias: match[0], index: match.index || 0, explicit: true });
    }
    return rows;
  }

  function chooseSecurity(alias, context) {
    const candidates = aliasCandidates.get(alias) || [];
    if (!candidates.length) return null;
    const codeMatch = context.match(/(?:\b|^)(\d{4,6})\s*\.?\s*(HK|SZ|SH|BJ)(?:\b|$)/i);
    if (codeMatch) {
      const market = normalizeMarket(codeMatch[2]);
      const code = normalizeCode(codeMatch[1], market);
      const exact = candidates.find((item) => item.code === code && item.market === market);
      if (exact) return exact;
    }
    return candidates.find((item) => ['SH', 'SZ', 'BJ'].includes(item.market)) || candidates[0];
  }

  function extractMentions(topic, source = extractSource(`${topic.title || ''}\n${topic.content || ''}`)) {
    const title = String(topic.title || '');
    const content = String(topic.content || '');
    const firstLine = content.split(/\r?\n/, 1)[0] || '';
    const text = `${title}\n${content}`;
    const byKey = new Map();
    for (const item of explicitTickerMentions(text)) {
      const context = contextAround(text, item.index, item.alias.length);
      byKey.set(`${item.market}:${item.code}`, { ...item, confidence: 0.99, contexts: [context] });
    }

    for (const match of findAliases(text)) {
      const alias = match[0];
      const index = match.index || 0;
      const context = contextAround(text, index, alias.length);
      const inTitle = index < title.length;
      const firstLineStart = title.length + 1;
      const inFirstLine = index >= firstLineStart && index < firstLineStart + firstLine.length;
      const headerMention = inTitle || inFirstLine;
      const bracketed = isInsideDecoratedBracket(text, index, alias.length);
      if (ambiguousNames.has(alias)) continue;

      const selected = chooseSecurity(alias, context);
      if (!selected) continue;
      if (selected.rejectFollowing && new RegExp(`${escapeRegex(alias)}(?:${selected.rejectFollowing})`, 'i').test(context)) continue;

      const brokerAttribution = brokerSecurityNames.has(alias) && source.raw?.includes(alias);
      if (brokerAttribution) continue;
      if (/证券$/.test(alias) && new RegExp(`${escapeRegex(alias)}.{0,12}(团队|研究|分析师|医药|电子|计算机|机械|通信|电新|军工|汽车|农业|传媒|交运)`).test(context)) continue;

      const hasPositive = positivePattern.test(context);
      const hasNegative = negativePattern.test(context);
      const hasCompanyContext = companyContextPattern.test(context);
      if (!headerMention && !bracketed && !hasPositive && !hasNegative && !hasCompanyContext) continue;
      if (selected.manualAlias && !headerMention && !bracketed && !hasPositive && !hasNegative) continue;

      const key = `${selected.market}:${selected.code}`;
      const confidence = selected.manualAlias
        ? (headerMention || bracketed ? 0.86 : 0.8)
        : (headerMention || bracketed ? 0.92 : hasPositive ? 0.9 : hasCompanyContext ? 0.78 : 0.72);
      if (!byKey.has(key)) {
        byKey.set(key, { code: selected.code, market: selected.market, name: selected.name, alias, index, explicit: false, confidence, contexts: [] });
      }
      byKey.get(key).contexts.push(context);
      byKey.get(key).confidence = Math.max(byKey.get(key).confidence, confidence);
    }

    const targetMatches = [];
    const targetRegex = /(?:目标价(?:分别)?(?:为|至|上调至|下调至|调整为)?|上看|看到|看至)\s*[：:]?\s*([\d,]+(?:\.\d+)?)\s*(港元|元|美元|人民币|印尼盾|HKD|USD)?|([\d,]+(?:\.\d+)?)\s*(港元|元|美元|人民币|印尼盾|HKD|USD)\s*目标价/gi;
    for (const match of text.matchAll(targetRegex)) {
      const currency = match[2] || match[4] || '';
      if (!currency && !/^目标价/i.test(match[0])) continue;
      targetMatches.push({ index: match.index || 0, value: Number(String(match[1] || match[3]).replace(/,/g, '')), currency, raw: match[0] });
    }

    const mentions = [...byKey.values()];
    for (const mention of mentions) {
      const joined = mention.contexts.join(' | ');
      const classification = classifyDirection(joined, pipelineConfig);
      mention.direction = classification.direction;
      mention.direction_confidence = classification.confidence;
      mention.occurrences = [];
      let offset = 0;
      while ((offset = text.indexOf(mention.alias, offset)) >= 0) {
        mention.occurrences.push(offset);
        offset += mention.alias.length;
      }
      mention.target = null;
      mention.evidence = mention.contexts.find((ctx) => positivePattern.test(ctx) || negativePattern.test(ctx)) || mention.contexts[0] || '';
    }
    for (const target of targetMatches) {
      const nearestBefore = mentions
        .map((mention) => ({ mention, position: Math.max(...mention.occurrences.filter((pos) => pos <= target.index)) }))
        .filter((item) => Number.isFinite(item.position) && target.index - item.position <= 1600)
        .sort((a, b) => b.position - a.position)[0];
      if (nearestBefore && (!nearestBefore.mention.target || target.index - nearestBefore.position < nearestBefore.mention.target.distance)) {
        nearestBefore.mention.target = { ...target, distance: target.index - nearestBefore.position };
      }
    }
    return mentions;
  }

  return { extractSource, extractMentions };
}
