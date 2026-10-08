import { evidenceExists, normalizeBroker, resolveSecurity, securityKey } from './ai_enrichment.mjs';

export const VIEWPOINT_PROMPT_VERSION = 'viewpoints-v3-resolution-queue';
export const VIEWPOINT_ENTITY_THRESHOLD = 0.92;
export const VIEWPOINT_DIRECTION_THRESHOLD = 0.90;
export const VIEWPOINT_SOURCE_THRESHOLD = 0.95;

export function splitViewpointBlocks(content) {
  const blocks = String(content || '')
    .replace(/\r/g, '')
    .split(/\n{2,}|(?=\n(?:[0-9]+[️⃣.)、]|[一二三四五六七八九十]+[、：]|[🔸➡️☀️🌞🍁🚀]))/)
    .map((text) => text.trim())
    .filter(Boolean);
  return (blocks.length ? blocks : [String(content || '').trim()].filter(Boolean))
    .map((text, index) => ({ ordinal: index + 1, text }));
}

function isSpecificSecuritySurface(value) {
  const surface = String(value || '').trim();
  if (!surface || surface.length > 80) return false;
  if (/^(?:其他|其它|很多|多家|若干|某些|相关|部分|上述|这些)/u.test(surface)) return false;
  return !/^(?:某)?(?:公司|企业|厂商|客户|供应商|合作方|电池厂|车企|机构|平台)$/u.test(surface);
}

export function validateViewpointTopicResult(topic, result, securityIndex) {
  const fatalErrors = [];
  const warnings = [];
  if (String(result?.topic_id) !== String(topic.topic_id)) fatalErrors.push('topic_id_mismatch');
  const viewpoints = [];
  const ordinals = new Set();
  const expectedBlocks = new Set((topic.viewpoint_blocks || []).map((block) => Number(block.ordinal)));
  const coveredBlocks = new Set();
  const blocksByOrdinal = new Map((topic.viewpoint_blocks || []).map((block) => [Number(block.ordinal), String(block.text || '').trim()]));
  for (const rawViewpoint of Array.isArray(result?.viewpoints) ? result.viewpoints : []) {
    const ordinal = Number(rawViewpoint.ordinal);
    if (!Number.isInteger(ordinal) || ordinal < 1 || ordinals.has(ordinal)) {
      warnings.push(`invalid_viewpoint_ordinal:${rawViewpoint.ordinal}`);
      continue;
    }
    const sourceBlockOrdinals = [...new Set((rawViewpoint.source_block_ordinals || []).map(Number))];
    if (!sourceBlockOrdinals.length || sourceBlockOrdinals.some((blockOrdinal) => !expectedBlocks.has(blockOrdinal))) {
      warnings.push(`invalid_source_blocks:${ordinal}`);
      continue;
    }
    let viewpointEvidence = String(rawViewpoint.evidence || '').trim();
    if (!evidenceExists(topic, viewpointEvidence)) {
      viewpointEvidence = blocksByOrdinal.get(sourceBlockOrdinals[0]) || '';
      warnings.push(`viewpoint_evidence_repaired_from_block:${ordinal}`);
    }
    if (!evidenceExists(topic, viewpointEvidence)) {
      warnings.push(`viewpoint_missing_evidence:${ordinal}`);
      continue;
    }
    ordinals.add(ordinal);
    for (const blockOrdinal of sourceBlockOrdinals) coveredBlocks.add(blockOrdinal);
    const entities = [];
    const unresolvedEntities = [];
    const seenUnresolved = new Set();
    const seenEntities = new Set();
    for (const rawEntity of Array.isArray(rawViewpoint.entities) ? rawViewpoint.entities : []) {
      const security = resolveSecurity(rawEntity, securityIndex);
      if (!security) {
        warnings.push(`unknown_security:${ordinal}:${rawEntity.market}:${rawEntity.code}`);
        const surface = String(rawEntity.surface || rawEntity.canonical_name || '').trim();
        const evidence = String(rawEntity.evidence || '').trim();
        const unresolvedKey = `${surface}|${evidence}`;
        if (isSpecificSecuritySurface(surface) && evidenceExists(topic, evidence) && !seenUnresolved.has(unresolvedKey)) {
          seenUnresolved.add(unresolvedKey);
          unresolvedEntities.push({
            surface,
            canonical_name: String(rawEntity.canonical_name || surface).trim(),
            direction: rawEntity.direction,
            evidence,
            confidence: Number(rawEntity.confidence),
            search_query: `${rawEntity.canonical_name || surface} ${rawEntity.market || ''} ${rawEntity.code || ''} 官方 股票 代码`.trim(),
            reason: `security_master_missing:${rawEntity.market || ''}:${rawEntity.code || ''}`,
          });
        }
        continue;
      }
      if (!evidenceExists(topic, rawEntity.evidence)) {
        warnings.push(`entity_missing_evidence:${ordinal}:${security.market}:${security.code}`);
        continue;
      }
      const key = securityKey(security.market, security.code);
      if (seenEntities.has(key)) {
        warnings.push(`duplicate_viewpoint_security:${ordinal}:${key}`);
        continue;
      }
      seenEntities.add(key);
      entities.push({
        ...rawEntity,
        canonical_name: security.name,
        code: security.code,
        market: security.market,
        confidence: Number(rawEntity.confidence),
        target_price: rawEntity.target_price == null ? null : Number(rawEntity.target_price),
        target_currency: String(rawEntity.target_currency || ''),
      });
    }
    for (const rawEntity of Array.isArray(rawViewpoint.unresolved_entities) ? rawViewpoint.unresolved_entities : []) {
      const surface = String(rawEntity.surface || '').trim();
      const evidence = String(rawEntity.evidence || '').trim();
      const key = `${surface}|${evidence}`;
      if (!isSpecificSecuritySurface(surface) || seenUnresolved.has(key)) continue;
      if (!evidenceExists(topic, evidence)) {
        warnings.push(`unresolved_missing_evidence:${ordinal}:${surface}`);
        continue;
      }
      seenUnresolved.add(key);
      unresolvedEntities.push({ ...rawEntity, surface, evidence, confidence: Number(rawEntity.confidence) });
    }
    viewpoints.push({
      ordinal,
      source_block_ordinals: sourceBlockOrdinals,
      summary: String(rawViewpoint.summary || '').trim(),
      evidence: viewpointEvidence,
      viewpoint_type: rawViewpoint.viewpoint_type,
      confidence: Number(rawViewpoint.confidence),
      entities,
      unresolved_entities: unresolvedEntities,
    });
  }
  const ignoredBlocks = [];
  for (const ignored of Array.isArray(result?.ignored_blocks) ? result.ignored_blocks : []) {
    const ordinal = Number(ignored.ordinal);
    if (!expectedBlocks.has(ordinal)) {
      warnings.push(`invalid_ignored_block:${ignored.ordinal}`);
      continue;
    }
    coveredBlocks.add(ordinal);
    ignoredBlocks.push({ ordinal, reason: String(ignored.reason || '').trim() });
  }
  const uncoveredBlocks = [...expectedBlocks].filter((ordinal) => !coveredBlocks.has(ordinal));
  if (uncoveredBlocks.length) fatalErrors.push(`uncovered_blocks:${uncoveredBlocks.join(',')}`);
  let source = result?.source || null;
  if (source) {
    const broker = normalizeBroker(source.broker);
    const brokerCore = broker.replace(/证券$/u, '');
    if (!evidenceExists(topic, source.evidence) || !broker || (brokerCore && !String(source.evidence || '').includes(brokerCore))) {
      warnings.push('invalid_source_evidence');
      source = null;
    }
  }
  if (!viewpoints.length && !String(result?.abstain_reason || '').trim()) fatalErrors.push('missing_viewpoints_without_reason');
  return {
    valid: fatalErrors.length === 0,
    errors: [...fatalErrors, ...warnings],
    viewpoints,
    ignored_blocks: ignoredBlocks,
    source,
    abstain_reason: String(result?.abstain_reason || ''),
  };
}

export function flattenViewpointEntities(validation) {
  const rows = [];
  for (const viewpoint of validation?.viewpoints || []) {
    for (const entity of viewpoint.entities || []) rows.push({ ...entity, viewpoint_ordinal: viewpoint.ordinal });
  }
  return rows;
}

export function validateViewpointVerifierResult(topic, result, securityIndex) {
  const errors = [];
  if (String(result?.topic_id) !== String(topic.topic_id)) {
    return { valid: false, errors: ['topic_id_mismatch'], viewpoints: [], source: null, abstain_reason: '' };
  }
  const entities = [];
  const seenEntities = new Set();
  for (const rawEntity of Array.isArray(result?.entities) ? result.entities : []) {
    const security = resolveSecurity(rawEntity, securityIndex);
    if (!security) {
      errors.push(`unknown_security:${rawEntity.market}:${rawEntity.code}`);
      continue;
    }
    if (!evidenceExists(topic, rawEntity.evidence)) {
      errors.push(`entity_missing_evidence:${security.market}:${security.code}`);
      continue;
    }
    const key = `${securityKey(security.market, security.code)}:${rawEntity.direction}`;
    if (seenEntities.has(key)) continue;
    seenEntities.add(key);
    entities.push({
      ...rawEntity,
      canonical_name: security.name,
      code: security.code,
      market: security.market,
      confidence: Number(rawEntity.confidence),
      target_price: null,
      target_currency: '',
    });
  }
  const unresolvedEntities = [];
  const seenUnresolved = new Set();
  for (const rawEntity of Array.isArray(result?.unresolved_entities) ? result.unresolved_entities : []) {
    const surface = String(rawEntity.surface || '').trim();
    const evidence = String(rawEntity.evidence || '').trim();
    const key = surface.toLowerCase();
    if (!isSpecificSecuritySurface(surface) || seenUnresolved.has(key)) continue;
    if (!evidenceExists(topic, evidence)) {
      errors.push(`unresolved_missing_evidence:${surface}`);
      continue;
    }
    seenUnresolved.add(key);
    unresolvedEntities.push({ ...rawEntity, surface, evidence, confidence: Number(rawEntity.confidence) });
  }
  let source = result?.source || null;
  if (source) {
    const broker = normalizeBroker(source.broker);
    const brokerCore = broker.replace(/证券$/u, '');
    if (!evidenceExists(topic, source.evidence) || !broker || (brokerCore && !String(source.evidence || '').includes(brokerCore))) {
      errors.push('invalid_source_evidence');
      source = null;
    }
  }
  return {
    valid: true,
    errors,
    viewpoints: [{ ordinal: 1, entities, unresolved_entities: unresolvedEntities }],
    ignored_blocks: [],
    source,
    abstain_reason: String(result?.abstain_reason || ''),
  };
}

export function compareViewpointEntities(primary, verifier) {
  const verifierByKey = new Map();
  for (const entity of flattenViewpointEntities(verifier)) {
    const key = securityKey(entity.market, entity.code);
    if (!verifierByKey.has(key)) verifierByKey.set(key, []);
    verifierByKey.get(key).push(entity);
  }
  return flattenViewpointEntities(primary).map((entity) => {
    const matches = verifierByKey.get(securityKey(entity.market, entity.code)) || [];
    const directionMatch = matches.find((row) => row.direction === entity.direction);
    const match = directionMatch || matches[0];
    return {
      ...entity,
      verified: Boolean(match) && match.direction === entity.direction,
      verifier_confidence: Number(match?.confidence || 0),
    };
  });
}
