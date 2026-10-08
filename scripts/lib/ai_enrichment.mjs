import { canonicalMarket } from './security_market.mjs';

export const AI_PROMPT_VERSION = 'gpt-history-v1';
export const ENTITY_THRESHOLD = 0.92;
export const DIRECTION_THRESHOLD = 0.90;
export const SOURCE_THRESHOLD = 0.95;

export function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

export function normalizeBroker(value) {
  return normalizeText(value).replace(/股份有限公司$/u, '').replace(/有限责任公司$/u, '').replace(/证券公司$/u, '证券').replace(/公司$/u, '');
}

export function securityKey(market, code) {
  const canonical = canonicalMarket(market, code);
  const value = canonical === 'HK' ? String(code || '').padStart(5, '0') : String(code || '').trim();
  return `${canonical}:${value}`;
}

export function buildSecurityIndex(master) {
  const byKey = new Map();
  const byName = new Map();
  for (const row of master.securities || []) {
    const market = canonicalMarket(row.market, row.code);
    const code = market === 'HK' ? String(row.code).padStart(5, '0') : String(row.code);
    const normalized = { ...row, code, market };
    byKey.set(securityKey(market, code), normalized);
    const name = normalizeText(row.name).replace(/^\*?ST/iu, '');
    if (!byName.has(name)) byName.set(name, []);
    byName.get(name).push(normalized);
  }
  return { byKey, byName };
}

export function resolveSecurity(mention, index) {
  const direct = index.byKey.get(securityKey(mention.market, mention.code));
  if (direct) return direct;
  const name = normalizeText(mention.canonical_name).replace(/^\*?ST/iu, '');
  const candidates = index.byName.get(name) || [];
  return candidates.length === 1 ? candidates[0] : null;
}

export function evidenceExists(topic, evidence) {
  const needle = String(evidence || '').trim();
  return Boolean(needle) && `${topic.title || ''}\n${topic.content || ''}`.includes(needle);
}

export function validateTopicResult(topic, result, securityIndex) {
  const errors = [];
  const fatalErrors = [];
  if (String(result?.topic_id) !== String(topic.topic_id)) fatalErrors.push('topic_id_mismatch');
  const mentions = [];
  const seen = new Set();
  for (const raw of Array.isArray(result?.mentions) ? result.mentions : []) {
    const security = resolveSecurity(raw, securityIndex);
    if (!security) { errors.push(`unknown_security:${raw.market}:${raw.code}`); continue; }
    if (!evidenceExists(topic, raw.evidence)) { errors.push(`missing_evidence:${security.market}:${security.code}`); continue; }
    const key = securityKey(security.market, security.code);
    if (seen.has(key)) { errors.push(`duplicate_security:${key}`); continue; }
    seen.add(key);
    mentions.push({
      ...raw,
      code: security.code,
      market: security.market,
      canonical_name: security.name,
      confidence: Number(raw.confidence),
      target_price: raw.target_price == null ? null : Number(raw.target_price),
      target_currency: String(raw.target_currency || ''),
    });
  }
  let source = result?.source || null;
  if (source) {
    if (!evidenceExists(topic, source.evidence)) {
      errors.push('source_missing_evidence');
      source = null;
    } else if (!normalizeBroker(source.broker)) {
      errors.push('source_empty_broker');
      source = null;
    } else {
      const brokerCore = normalizeBroker(source.broker).replace(/证券$/u, '');
      if (brokerCore && !normalizeText(source.evidence).includes(brokerCore)) {
        errors.push('source_broker_not_in_evidence');
        source = null;
      }
    }
  }
  return { valid: fatalErrors.length === 0, errors: [...fatalErrors, ...errors], mentions, source, suspected_false_positive_codes: result?.suspected_false_positive_codes || [], abstain_reason: result?.abstain_reason || '' };
}

export function compareVerified(primary, verifier) {
  const verifiedByKey = new Map(verifier.mentions.map((row) => [securityKey(row.market, row.code), row]));
  const mentions = primary.mentions.map((row) => {
    const other = verifiedByKey.get(securityKey(row.market, row.code));
    const targetMatches = row.target_price == null && other?.target_price == null
      || (row.target_price != null && other?.target_price != null && Math.abs(row.target_price - other.target_price) < 0.0001);
    return { ...row, verified: Boolean(other) && other.direction === row.direction && targetMatches, verifier_confidence: Number(other?.confidence || 0) };
  });
  const source = primary.source && verifier.source && normalizeBroker(primary.source.broker) === normalizeBroker(verifier.source.broker)
    ? { ...primary.source, verified: true, verifier_confidence: Number(verifier.source.confidence || 0) }
    : primary.source ? { ...primary.source, verified: false, verifier_confidence: 0 } : null;
  return { mentions, source };
}

export function shouldAutoAcceptMention(mention) {
  return Boolean(mention.verified) && mention.confidence >= ENTITY_THRESHOLD && mention.verifier_confidence >= ENTITY_THRESHOLD;
}

export function shouldAutoAcceptDirection(mention) {
  return Boolean(mention.verified) && mention.confidence >= DIRECTION_THRESHOLD && mention.verifier_confidence >= DIRECTION_THRESHOLD;
}

export function shouldAutoAcceptSource(source) {
  return Boolean(source?.verified) && source.confidence >= SOURCE_THRESHOLD && source.verifier_confidence >= SOURCE_THRESHOLD;
}
