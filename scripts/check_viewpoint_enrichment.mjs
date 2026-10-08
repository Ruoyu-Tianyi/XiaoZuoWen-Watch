import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { normalizeBroker } from './lib/ai_enrichment.mjs';
import { compareViewpointEntities, flattenViewpointEntities, splitViewpointBlocks } from './lib/ai_viewpoint_enrichment.mjs';

const option = (name, fallback = null) => process.argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
const hasFlag = (name) => process.argv.includes(`--${name}`);
const dbPath = resolve(option('db', process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite'));
const db = new DatabaseSync(dbPath, { readOnly: true });
const requestedRun = Number(option('run', '0'));
const run = requestedRun
  ? db.prepare('SELECT * FROM ai_enrichment_runs WHERE id=?').get(requestedRun)
  : db.prepare("SELECT * FROM ai_enrichment_runs WHERE prompt_version LIKE 'viewpoints-%' ORDER BY id DESC LIMIT 1").get();
if (!run) throw new Error('找不到观点识别运行记录');

const resultRows = db.prepare(`SELECT r.*,t.title,t.content FROM ai_enrichment_results r
  JOIN topics t USING(topic_id) WHERE r.run_id=? ORDER BY r.topic_id,r.phase`).all(run.id);
const extracts = resultRows.filter((row) => row.phase === 'extract');
const verifies = new Map(resultRows.filter((row) => row.phase === 'verify').map((row) => [String(row.topic_id), row]));
const metrics = {
  run_id: Number(run.id), model: run.model, prompt_version: run.prompt_version,
  total_topics: Number(run.total_topics), extract_complete: 0, extract_failed: 0,
  structurally_valid_topics: 0, expected_blocks: 0, covered_blocks: 0, duplicate_block_assignments: 0,
  viewpoints: 0, resolved_entities: 0, unresolved_entities: 0, source_attributions: 0,
  verification_required: 0, verification_complete: 0, independently_verified_entities: 0,
  independently_verified_sources: 0,
  exact_evidence_failures: 0, invalid_security_references: 0,
};
const issues = [];

for (const row of extracts) {
  if (row.status !== 'complete') {
    metrics.extract_failed += 1;
    issues.push({ topic_id: String(row.topic_id), issue: row.error || 'extract_failed' });
    continue;
  }
  metrics.extract_complete += 1;
  let validation;
  try { validation = JSON.parse(row.validation_json); } catch {
    issues.push({ topic_id: String(row.topic_id), issue: 'invalid_validation_json' });
    continue;
  }
  if (validation.valid) metrics.structurally_valid_topics += 1;
  else issues.push({ topic_id: String(row.topic_id), issue: `invalid:${(validation.errors || []).join(',')}` });
  const expected = new Set(splitViewpointBlocks(row.content).map((block) => block.ordinal));
  const counts = new Map([...expected].map((ordinal) => [ordinal, 0]));
  for (const viewpoint of validation.viewpoints || []) {
    metrics.viewpoints += 1;
    for (const ordinal of viewpoint.source_block_ordinals || []) counts.set(Number(ordinal), (counts.get(Number(ordinal)) || 0) + 1);
    if (!`${row.title || ''}\n${row.content || ''}`.includes(String(viewpoint.evidence || '').trim())) metrics.exact_evidence_failures += 1;
    for (const entity of viewpoint.entities || []) {
      metrics.resolved_entities += 1;
      if (!['SH', 'SZ', 'BJ', 'HK', 'US'].includes(entity.market)) metrics.invalid_security_references += 1;
      if (!`${row.title || ''}\n${row.content || ''}`.includes(String(entity.evidence || '').trim())) metrics.exact_evidence_failures += 1;
    }
    metrics.unresolved_entities += (viewpoint.unresolved_entities || []).length;
  }
  for (const ignored of validation.ignored_blocks || []) counts.set(Number(ignored.ordinal), (counts.get(Number(ignored.ordinal)) || 0) + 1);
  metrics.expected_blocks += expected.size;
  metrics.covered_blocks += [...counts.values()].filter((count) => count > 0).length;
  metrics.duplicate_block_assignments += [...counts.values()].filter((count) => count > 1).length;
  if (validation.source) metrics.source_attributions += 1;
  const primaryEntities = flattenViewpointEntities(validation);
  const topicUnresolved = (validation.viewpoints || []).reduce((count, viewpoint) => count + (viewpoint.unresolved_entities || []).length, 0);
  if (primaryEntities.length || topicUnresolved || validation.source) {
    metrics.verification_required += 1;
    const verifyRow = verifies.get(String(row.topic_id));
    if (verifyRow?.status === 'complete') {
      metrics.verification_complete += 1;
      const verifier = JSON.parse(verifyRow.validation_json);
      metrics.independently_verified_entities += compareViewpointEntities(validation, verifier).filter((entity) => entity.verified).length;
      if (validation.source && verifier.source && normalizeBroker(validation.source.broker) === normalizeBroker(verifier.source.broker)) metrics.independently_verified_sources += 1;
    }
  }
}

metrics.completion_rate = metrics.total_topics ? metrics.extract_complete / metrics.total_topics : 0;
metrics.block_coverage_rate = metrics.expected_blocks ? metrics.covered_blocks / metrics.expected_blocks : 1;
metrics.structural_validity_rate = metrics.extract_complete ? metrics.structurally_valid_topics / metrics.extract_complete : 0;
metrics.verification_completion_rate = metrics.verification_required ? metrics.verification_complete / metrics.verification_required : 1;
metrics.entity_independent_agreement_rate = metrics.resolved_entities ? metrics.independently_verified_entities / metrics.resolved_entities : 1;
metrics.source_independent_agreement_rate = metrics.source_attributions ? metrics.independently_verified_sources / metrics.source_attributions : 1;
const gates = {
  all_topics_complete: metrics.extract_complete === metrics.total_topics && metrics.extract_failed === 0,
  all_results_structurally_valid: metrics.structurally_valid_topics === metrics.extract_complete,
  all_blocks_accounted_for: metrics.covered_blocks === metrics.expected_blocks,
  all_evidence_exact: metrics.exact_evidence_failures === 0,
  all_security_references_legal: metrics.invalid_security_references === 0,
  all_required_verification_complete: metrics.verification_complete === metrics.verification_required,
};
const hasMaterializedTables = Number(db.prepare("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name='model_viewpoints'").get().n) > 0;
const materialized = hasMaterializedTables ? {
  viewpoints: Number(db.prepare('SELECT count(*) n FROM model_viewpoints WHERE recognition_model=? AND prompt_version=?').get(run.model, run.prompt_version).n),
  viewpoint_mentions: Number(db.prepare(`SELECT count(*) n FROM model_viewpoint_mentions m JOIN model_viewpoints v ON v.id=m.viewpoint_id WHERE v.recognition_model=? AND v.prompt_version=?`).get(run.model, run.prompt_version).n),
  resolution_queue_pending: Number(db.prepare("SELECT count(*) n FROM security_resolution_queue WHERE recognition_model=? AND prompt_version=? AND status='pending'").get(run.model, run.prompt_version).n),
} : null;
const output = { generated_at: new Date().toISOString(), db: dbPath, run, metrics, materialized, gates, passed: Object.values(gates).every(Boolean), issues: issues.slice(0, 300) };
db.close();
if (hasFlag('write')) {
  writeFileSync(resolve('reports/viewpoint_enrichment_quality.json'), JSON.stringify(output, null, 2), 'utf8');
  const lines = ['# 观点识别质量门禁', '', `- 运行：${run.id}`, `- 模型：${run.model}`, `- 结果：${output.passed ? '通过' : '未通过'}`,
    `- 完成：${metrics.extract_complete}/${metrics.total_topics}`, `- 原文块覆盖：${metrics.covered_blocks}/${metrics.expected_blocks}`,
    `- 观点：${metrics.viewpoints}`, `- 已解析证券：${metrics.resolved_entities}`, `- 待解析证券：${metrics.unresolved_entities}`,
    `- 独立复核完成：${metrics.verification_complete}/${metrics.verification_required}`];
  writeFileSync(resolve('reports/viewpoint_enrichment_quality.md'), `${lines.join('\n')}\n`, 'utf8');
}
console.log(JSON.stringify(output, null, 2));
if (!output.passed) process.exitCode = 1;
