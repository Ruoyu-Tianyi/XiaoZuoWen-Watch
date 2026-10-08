import { DatabaseSync } from 'node:sqlite';
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { topicIsEligible } from './lib/topic_filter.mjs';
import { loadPipelineConfig } from './lib/direction_rules.mjs';

const dbPath = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const outputPath = resolve(process.env.ZSXQ_QUALITY_OUTPUT || 'reports/data_quality.json');
const db = new DatabaseSync(dbPath, { readOnly: true });
const pipelineConfig = loadPipelineConfig();

const scalar = (sql) => Number(Object.values(db.prepare(sql).get())[0] || 0);
const tableExists = (name) => scalar(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='${String(name).replace(/'/g, "''")}'`) > 0;
const totals = {
  topics: scalar('SELECT count(*) FROM topics'),
  mentions: scalar('SELECT count(*) FROM mentions'),
  viewpoints: scalar('SELECT count(*) FROM viewpoints'),
  securities: scalar('SELECT count(*) FROM securities'),
  model_viewpoints: tableExists('model_viewpoints') ? scalar('SELECT count(*) FROM model_viewpoints') : 0,
  model_viewpoint_mentions: tableExists('model_viewpoint_mentions') ? scalar('SELECT count(*) FROM model_viewpoint_mentions') : 0,
};
const topicRows = db.prepare('SELECT topic_id,title,content,raw_json FROM topics').all();
const ineligibleTopics = topicRows.filter((topic) => !topicIsEligible(topic, pipelineConfig)).length;
const checks = {
  duplicate_topic_ids: scalar('SELECT count(*) FROM (SELECT topic_id FROM topics GROUP BY topic_id HAVING count(*)>1)'),
  duplicate_topic_security: scalar('SELECT count(*) FROM (SELECT topic_id,security_id FROM mentions GROUP BY topic_id,security_id HAVING count(*)>1)'),
  orphan_mentions: scalar('SELECT count(*) FROM mentions m LEFT JOIN topics t ON t.topic_id=m.topic_id LEFT JOIN securities s ON s.id=m.security_id WHERE t.topic_id IS NULL OR s.id IS NULL'),
  invalid_directions: scalar("SELECT count(*) FROM mentions WHERE direction NOT IN ('bullish','bearish','neutral')"),
  invalid_security_markets: scalar("SELECT count(*) FROM securities WHERE market NOT IN ('SH','SZ','BJ','HK','US')"),
  legacy_a_markets: scalar("SELECT count(*) FROM securities WHERE market='A'"),
  non_pure_text_topics: ineligibleTopics,
  future_topics: scalar("SELECT count(*) FROM topics WHERE datetime(published_at_utc)>datetime('now','+10 minutes')"),
  empty_content_topics: scalar("SELECT count(*) FROM topics WHERE trim(content)=''"),
  topics_without_mentions: scalar('SELECT count(*) FROM topics t WHERE NOT EXISTS (SELECT 1 FROM mentions m WHERE m.topic_id=t.topic_id)'),
  topics_without_source: scalar("SELECT count(*) FROM topics WHERE trim(source_broker)=''"),
  low_confidence_mentions: scalar('SELECT count(*) FROM mentions WHERE entity_confidence<0.75'),
  duplicate_model_viewpoint_ordinals: tableExists('model_viewpoints') ? scalar('SELECT count(*) FROM (SELECT topic_id,content_hash,recognition_model,prompt_version,ordinal FROM model_viewpoints GROUP BY topic_id,content_hash,recognition_model,prompt_version,ordinal HAVING count(*)>1)') : 0,
  orphan_model_viewpoint_mentions: tableExists('model_viewpoint_mentions') ? scalar('SELECT count(*) FROM model_viewpoint_mentions vm LEFT JOIN model_viewpoints v ON v.id=vm.viewpoint_id LEFT JOIN securities s ON s.id=vm.security_id WHERE v.id IS NULL OR s.id IS NULL') : 0,
  invalid_model_viewpoint_directions: tableExists('model_viewpoint_mentions') ? scalar("SELECT count(*) FROM model_viewpoint_mentions WHERE direction NOT IN ('bullish','bearish','neutral')") : 0,
};
const latest = db.prepare('SELECT max(published_at_utc) latest_post, max(ingested_at) latest_ingestion, min(published_at_utc) earliest_post FROM topics').get();
const eligible = {
  text_topics: scalar("SELECT count(*) FROM topics WHERE length(trim(content))>=50"),
  text_topics_with_mentions: scalar("SELECT count(*) FROM topics t WHERE length(trim(content))>=50 AND EXISTS (SELECT 1 FROM mentions m WHERE m.topic_id=t.topic_id)"),
};
const rates = {
  mention_coverage: totals.topics ? (totals.topics - checks.topics_without_mentions) / totals.topics : 0,
  source_coverage: totals.topics ? (totals.topics - checks.topics_without_source) / totals.topics : 0,
  empty_content_rate: totals.topics ? checks.empty_content_topics / totals.topics : 0,
  low_confidence_rate: totals.mentions ? checks.low_confidence_mentions / totals.mentions : 0,
  eligible_text_mention_coverage: eligible.text_topics ? eligible.text_topics_with_mentions / eligible.text_topics : 0,
};
const criticalFailures = checks.duplicate_topic_ids + checks.duplicate_topic_security + checks.orphan_mentions + checks.invalid_directions + checks.invalid_security_markets + checks.legacy_a_markets + checks.non_pure_text_topics + checks.duplicate_model_viewpoint_ordinals + checks.orphan_model_viewpoint_mentions + checks.invalid_model_viewpoint_directions;
const freshnessLagHours = latest.latest_ingestion ? (Date.now() - new Date(latest.latest_ingestion).getTime()) / 3600000 : null;
const warningSignals = checks.future_topics > 0 || rates.empty_content_rate > 0.01 || rates.low_confidence_rate > 0.02 || freshnessLagHours == null || freshnessLagHours > 24;
const status = criticalFailures > 0 ? 'fail' : warningSignals ? 'warning' : 'pass';
const payload = { checked_at: new Date().toISOString(), status, database: dbPath, totals, checks, eligible, rates, freshness_lag_hours: freshnessLagHours, latest };
writeFileSync(outputPath, JSON.stringify(payload, null, 2), 'utf8');
console.log(JSON.stringify(payload, null, 2));
db.close();
