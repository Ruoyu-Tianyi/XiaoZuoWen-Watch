import { createHash } from 'node:crypto';

function hash(value, length = 16) {
  return createHash('sha256').update(value).digest('hex').slice(0, length);
}

export function contentFingerprint(title, content) {
  const normalized = `${String(title || '').replace(/\r\n?/g, '\n').trim()}\u0000${String(content || '').replace(/\r\n?/g, '\n').trim()}`;
  return hash(normalized, 32);
}

export function securityMasterFingerprint(master) {
  const securities = (master?.securities || [])
    .map((item) => [String(item.market), String(item.code), String(item.name)])
    .sort((a, b) => a.join(':').localeCompare(b.join(':')));
  return hash(JSON.stringify(securities), 12);
}

export function extractionVersion(config, masterFingerprint = '') {
  const semanticRules = JSON.stringify({
    rules: config.rules,
    direction: config.direction,
    entity: config.entity || {},
    ingestion: config.ingestion || {},
    security_master: masterFingerprint,
  });
  return `extract-${Object.values(config.rules).join('+')}-${hash(semanticRules, 12)}`;
}

export function pipelineVersion(config) {
  return `${config.pipeline_version}-${config.rules.direction}-${hash(JSON.stringify(config), 12)}`;
}
