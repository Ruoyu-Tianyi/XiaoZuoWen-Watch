import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const dbPath = resolve(process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite');
const outputPath = resolve('reports/ai_quality_audit.json');
const db = new DatabaseSync(dbPath, { readOnly: true });

const auditRun = db.prepare(`SELECT * FROM ai_enrichment_runs
  WHERE provider='codex' AND model='gpt-5.6-terra' AND prompt_version='gpt-targets-v1'
  ORDER BY id DESC LIMIT 1`).get();
if (!auditRun) throw new Error('找不到 Terra 独立审计结果');

const rows = db.prepare(`SELECT topic_id,validation_json FROM ai_enrichment_results
  WHERE run_id=? AND phase='extract' AND status='complete'`).all(auditRun.id);
const audited = new Map(rows.map((row) => {
  const parsed = JSON.parse(row.validation_json || '{}');
  return [String(row.topic_id), new Map((parsed.mentions || []).map((mention) => [`${mention.market}:${mention.code}`, mention]))];
}));
const verifyRows = db.prepare(`SELECT topic_id,validation_json FROM ai_enrichment_results
  WHERE run_id=? AND phase='verify' AND status='complete'`).all(auditRun.id);
const verified = new Map(verifyRows.map((row) => {
  const parsed = JSON.parse(row.validation_json || '{}');
  return [String(row.topic_id), new Map((parsed.mentions || []).map((mention) => [`${mention.market}:${mention.code}`, mention]))];
}));
const adjudicationRun = db.prepare(`SELECT * FROM ai_enrichment_runs
  WHERE provider='codex' AND model='gpt-5.6-terra' AND prompt_version='gpt-target-adjudicate-v1'
  ORDER BY id DESC LIMIT 1`).get();
const adjudicated = new Map();
if (adjudicationRun) {
  const adjudicationRows = db.prepare(`SELECT topic_id,validation_json FROM ai_enrichment_results
    WHERE run_id=? AND phase='extract' AND status='complete'`).all(adjudicationRun.id);
  for (const row of adjudicationRows) {
    const parsed = JSON.parse(row.validation_json || '{}');
    adjudicated.set(String(row.topic_id), new Map((parsed.mentions || []).map((mention) => [`${mention.market}:${mention.code}`, mention])));
  }
}

const candidateMentions = db.prepare(`SELECT m.topic_id,s.market,s.code,s.name,m.direction,m.evidence,m.recognition_confidence
  FROM mentions m JOIN securities s ON s.id=m.security_id
  WHERE m.recognition_method='gpt_verified'
  ORDER BY m.topic_id,s.market,s.code`).all().filter((row) => audited.has(String(row.topic_id)));

let entityAgreed = 0;
let directionAgreed = 0;
let agreedByFirstPass = 0;
let agreedBySecondPass = 0;
let agreedByAdjudication = 0;
const disagreements = [];
const candidateKeys = new Set();
for (const mention of candidateMentions) {
  const key = `${mention.market}:${mention.code}`;
  candidateKeys.add(`${mention.topic_id}|${key}`);
  const firstPass = audited.get(String(mention.topic_id))?.get(key);
  const secondPass = verified.get(String(mention.topic_id))?.get(key);
  const adjudication = adjudicated.get(String(mention.topic_id))?.get(key);
  const independent = firstPass || secondPass || adjudication;
  if (independent) entityAgreed += 1;
  if (firstPass) agreedByFirstPass += 1;
  else if (secondPass) agreedBySecondPass += 1;
  else if (adjudication) agreedByAdjudication += 1;
  if (firstPass?.direction === mention.direction || secondPass?.direction === mention.direction || adjudication?.direction === mention.direction) directionAgreed += 1;
  if (!independent || (firstPass?.direction !== mention.direction && secondPass?.direction !== mention.direction && adjudication?.direction !== mention.direction)) {
    disagreements.push({
      topic_id: String(mention.topic_id), name: mention.name, code: mention.code, market: mention.market,
      candidate_direction: mention.direction,
      audit_direction: firstPass?.direction || null, verifier_direction: secondPass?.direction || null,
      adjudicator_direction: adjudication?.direction || null,
      candidate_evidence: mention.evidence,
      audit_evidence: firstPass?.evidence || null, verifier_evidence: secondPass?.evidence || null,
      adjudicator_evidence: adjudication?.evidence || null,
    });
  }
}

const extras = [];
for (const [topicId, mentions] of audited) for (const [key, mention] of mentions) {
  if (!candidateKeys.has(`${topicId}|${key}`)) extras.push({ topic_id: topicId, key, direction: mention.direction, evidence: mention.evidence });
}

const total = candidateMentions.length;
const payload = {
  generated_at: new Date().toISOString(), database: dbPath,
  audit_run: { id: Number(auditRun.id), model: auditRun.model, topics: rows.length },
  adjudication_run: adjudicationRun ? { id: Number(adjudicationRun.id), model: adjudicationRun.model, topics: adjudicated.size } : null,
  candidate_mentions: total,
  entity_agreed: entityAgreed,
  agreed_by_first_pass: agreedByFirstPass,
  agreed_by_second_pass_only: agreedBySecondPass,
  agreed_by_adjudication_only: agreedByAdjudication,
  entity_agreement_rate: total ? entityAgreed / total : 0,
  direction_agreed: directionAgreed,
  direction_agreement_rate: total ? directionAgreed / total : 0,
  missing_from_audit: total - entityAgreed,
  extra_in_audit: extras.length,
  gate_97_percent: total > 0 && entityAgreed / total >= 0.97,
  disagreements,
  extras,
};
db.close();
writeFileSync(outputPath, JSON.stringify(payload, null, 2), 'utf8');
console.log(JSON.stringify({ ...payload, disagreements: disagreements.slice(0, 20), extras: extras.slice(0, 20) }, null, 2));
