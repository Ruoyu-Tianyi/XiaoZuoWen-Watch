import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { classifyDirection, loadPipelineConfig } from './lib/direction_rules.mjs';
import { createEntityExtractor } from './lib/entity_extractor.mjs';
import { contentFingerprint, extractionVersion, pipelineVersion, securityMasterFingerprint } from './lib/pipeline_version.mjs';
import { topicIsEligible } from './lib/topic_filter.mjs';

const command = process.argv[2] || 'status';
const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const CANDIDATE_PATH = resolve(process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite');
const MANIFEST_PATH = resolve(process.env.PIPELINE_MANIFEST || 'data/pipeline_candidate.json');
const CONFIG_PATH = resolve(process.env.PIPELINE_CONFIG || 'config/pipeline.json');
const GOLDEN_PATH = resolve('config/golden_direction_samples.json');
const ENTITY_GOLDEN_PATH = resolve('config/golden_entity_samples.json');
const INGESTION_GOLDEN_PATH = resolve('config/golden_ingestion_samples.json');
const SECURITY_MASTER_PATH = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
const config = loadPipelineConfig(CONFIG_PATH);
const securityMaster = JSON.parse(readFileSync(SECURITY_MASTER_PATH, 'utf8'));
const configJson = JSON.stringify(config);
const configHash = createHash('sha256').update(configJson).digest('hex').slice(0, 12);
const version = pipelineVersion(config);
const currentExtractionVersion = extractionVersion(config, securityMasterFingerprint(securityMaster));

mkdirSync(dirname(CANDIDATE_PATH), { recursive: true });
mkdirSync(resolve('reports'), { recursive: true });

function sqlPath(path) { return path.replace(/'/g, "''"); }
function now() { return new Date().toISOString(); }
function readManifest() {
  if (!existsSync(MANIFEST_PATH)) throw new Error('没有候选版本，请先运行 npm run extract');
  return JSON.parse(readFileSync(MANIFEST_PATH, 'utf8'));
}
function writeManifest(payload) { writeFileSync(MANIFEST_PATH, JSON.stringify(payload, null, 2), 'utf8'); }
function runScript(script, env = {}) {
  const result = spawnSync(process.execPath, [resolve(script)], {
    cwd: process.cwd(), env: { ...process.env, ...env }, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.status !== 0) throw new Error((result.stderr || `${script} exited ${result.status}`).trim());
}
function initSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS pipeline_runs (
      id INTEGER PRIMARY KEY, command TEXT NOT NULL, status TEXT NOT NULL,
      rules_version TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
      candidate_db TEXT, source_json TEXT, metrics_json TEXT, error TEXT
    );
    CREATE TABLE IF NOT EXISTS extraction_versions (
      version TEXT PRIMARY KEY, config_json TEXT NOT NULL, status TEXT NOT NULL,
      created_at TEXT NOT NULL, published_at TEXT, run_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS pipeline_state (
      key TEXT PRIMARY KEY, value TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS ai_enrichment_runs (
      id INTEGER PRIMARY KEY, provider TEXT NOT NULL, model TEXT NOT NULL, prompt_version TEXT NOT NULL,
      status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
      total_topics INTEGER NOT NULL, completed_topics INTEGER NOT NULL DEFAULT 0,
      failed_topics INTEGER NOT NULL DEFAULT 0, batch_count INTEGER NOT NULL DEFAULT 0, error TEXT
    );
    CREATE TABLE IF NOT EXISTS ai_enrichment_results (
      topic_id TEXT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
      content_hash TEXT NOT NULL, provider TEXT NOT NULL, model TEXT NOT NULL,
      prompt_version TEXT NOT NULL, phase TEXT NOT NULL, status TEXT NOT NULL,
      result_json TEXT, validation_json TEXT, merge_status TEXT, error TEXT,
      processed_at TEXT NOT NULL, run_id INTEGER REFERENCES ai_enrichment_runs(id),
      PRIMARY KEY(topic_id,content_hash,provider,model,prompt_version,phase)
    );
    CREATE TABLE IF NOT EXISTS model_viewpoints (
      id INTEGER PRIMARY KEY, topic_id TEXT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
      content_hash TEXT NOT NULL, ordinal INTEGER NOT NULL, source_block_ordinals TEXT NOT NULL DEFAULT '[]', summary TEXT NOT NULL,
      evidence TEXT NOT NULL, viewpoint_type TEXT NOT NULL, confidence REAL NOT NULL,
      recognition_model TEXT NOT NULL, prompt_version TEXT NOT NULL, created_at TEXT NOT NULL,
      UNIQUE(topic_id,content_hash,recognition_model,prompt_version,ordinal)
    );
    CREATE TABLE IF NOT EXISTS model_viewpoint_mentions (
      id INTEGER PRIMARY KEY, viewpoint_id INTEGER NOT NULL REFERENCES model_viewpoints(id) ON DELETE CASCADE,
      security_id INTEGER NOT NULL REFERENCES securities(id), surface TEXT, direction TEXT NOT NULL,
      target_price REAL, target_currency TEXT, evidence TEXT NOT NULL, confidence REAL NOT NULL,
      verification_status TEXT NOT NULL, UNIQUE(viewpoint_id,security_id)
    );
    CREATE TABLE IF NOT EXISTS security_resolution_queue (
      id INTEGER PRIMARY KEY, topic_id TEXT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
      content_hash TEXT NOT NULL, viewpoint_ordinal INTEGER NOT NULL, surface TEXT NOT NULL,
      canonical_name TEXT, direction TEXT NOT NULL, evidence TEXT NOT NULL, confidence REAL NOT NULL,
      search_query TEXT, reason TEXT, status TEXT NOT NULL DEFAULT 'pending',
      resolved_security_id INTEGER REFERENCES securities(id), resolution_source TEXT,
      resolution_url TEXT, resolution_notes TEXT, recognition_model TEXT NOT NULL,
      prompt_version TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
      UNIQUE(topic_id,content_hash,viewpoint_ordinal,surface,recognition_model,prompt_version)
    );
    CREATE INDEX IF NOT EXISTS idx_model_viewpoints_topic ON model_viewpoints(topic_id,ordinal);
    CREATE INDEX IF NOT EXISTS idx_model_viewpoint_mentions_security ON model_viewpoint_mentions(security_id,direction);
    CREATE INDEX IF NOT EXISTS idx_security_resolution_status ON security_resolution_queue(status,confidence);
  `);
}
function ensureColumn(db, table, name, definition) {
  const exists = db.prepare(`PRAGMA table_info(${table})`).all().some((column) => column.name === name);
  if (!exists) db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
}
function ensureIncrementalMetadata(db) {
  ensureColumn(db, 'topics', 'content_hash', 'TEXT');
  ensureColumn(db, 'topics', 'extraction_version', 'TEXT');
  ensureColumn(db, 'topics', 'extracted_at', 'TEXT');
  ensureColumn(db, 'ingestion_runs', 'processed_topic_count', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'ingestion_runs', 'skipped_topic_count', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'ingestion_runs', 'new_topic_count', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'ingestion_runs', 'changed_topic_count', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'ingestion_runs', 'excluded_topic_count', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'mentions', 'recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
  ensureColumn(db, 'mentions', 'recognition_model', "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'mentions', 'recognition_confidence', 'REAL');
  ensureColumn(db, 'mentions', 'is_mixed', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'topics', 'source_recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
  ensureColumn(db, 'topics', 'source_recognition_model', "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'topics', 'source_recognition_confidence', 'REAL');
  ensureColumn(db, 'model_viewpoints', 'source_block_ordinals', "TEXT NOT NULL DEFAULT '[]'");
  db.exec('CREATE INDEX IF NOT EXISTS idx_topics_extraction_version ON topics(extraction_version)');
  const missing = db.prepare('SELECT topic_id,title,content,ingested_at FROM topics WHERE content_hash IS NULL OR extraction_version IS NULL').all();
  if (!missing.length) return { initialized: 0, extraction_version: currentExtractionVersion };
  const update = db.prepare('UPDATE topics SET content_hash=?,extraction_version=?,extracted_at=coalesce(extracted_at,ingested_at,?) WHERE topic_id=?');
  db.exec('BEGIN IMMEDIATE');
  try {
    const timestamp = now();
    for (const topic of missing) update.run(contentFingerprint(topic.title, topic.content), currentExtractionVersion, timestamp, topic.topic_id);
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
  return { initialized: missing.length, extraction_version: currentExtractionVersion };
}
function openProduction(options = {}) {
  const db = new DatabaseSync(DB_PATH, options);
  if (!options.readOnly) { initSchema(db); ensureIncrementalMetadata(db); }
  return db;
}
function directionCounts(db, schema = 'main') {
  return Object.fromEntries(db.prepare(`SELECT direction,count(*) n FROM ${schema}.mentions GROUP BY direction`).all().map((row) => [row.direction, Number(row.n)]));
}
function canonicalMarketSql(alias) {
  return `CASE WHEN ${alias}.market='A' THEN CASE
    WHEN substr(${alias}.code,1,3)='920' THEN 'BJ'
    WHEN substr(${alias}.code,1,1) IN ('6','9') THEN 'SH'
    WHEN substr(${alias}.code,1,1) IN ('0','2','3') THEN 'SZ'
    ELSE ${alias}.market END ELSE ${alias}.market END`;
}
function candidateDiff() {
  const db = openProduction();
  db.prepare('ATTACH DATABASE ? AS cand').run(CANDIDATE_PATH);
  const productionCounts = directionCounts(db, 'main');
  const candidateCounts = directionCounts(db, 'cand');
  const changed = Number(db.prepare(`
    WITH prod AS (
      SELECT p.topic_id,ps.code,${canonicalMarketSql('ps')} market,p.direction
      FROM main.mentions p JOIN main.securities ps ON ps.id=p.security_id
    ), candidate AS (
      SELECT c.topic_id,cs.code,${canonicalMarketSql('cs')} market,c.direction
      FROM cand.mentions c JOIN cand.securities cs ON cs.id=c.security_id
    )
    SELECT count(*) n FROM prod JOIN candidate USING(topic_id,code,market)
    WHERE candidate.direction<>prod.direction
  `).get().n);
  const added = Number(db.prepare(`
    WITH prod AS (
      SELECT p.topic_id,ps.code,${canonicalMarketSql('ps')} market
      FROM main.mentions p JOIN main.securities ps ON ps.id=p.security_id
    ), candidate AS (
      SELECT c.topic_id,cs.code,${canonicalMarketSql('cs')} market
      FROM cand.mentions c JOIN cand.securities cs ON cs.id=c.security_id
    )
    SELECT count(*) n FROM candidate LEFT JOIN prod USING(topic_id,code,market)
    WHERE prod.topic_id IS NULL
  `).get().n);
  const removed = Number(db.prepare(`
    WITH prod AS (
      SELECT p.topic_id,ps.code,${canonicalMarketSql('ps')} market
      FROM main.mentions p JOIN main.securities ps ON ps.id=p.security_id
    ), candidate AS (
      SELECT c.topic_id,cs.code,${canonicalMarketSql('cs')} market
      FROM cand.mentions c JOIN cand.securities cs ON cs.id=c.security_id
    )
    SELECT count(*) n FROM prod LEFT JOIN candidate USING(topic_id,code,market)
    WHERE candidate.topic_id IS NULL
  `).get().n);
  const productionTotal = Number(db.prepare('SELECT count(*) n FROM main.mentions').get().n);
  const candidateTotal = Number(db.prepare('SELECT count(*) n FROM cand.mentions').get().n);
  const ineligibleTopicIds = new Set(db.prepare('SELECT topic_id,title,content,raw_json FROM main.topics').all()
    .filter((topic) => !topicIsEligible(topic, config))
    .map((topic) => String(topic.topic_id)));
  const expectedFilteredMentions = db.prepare('SELECT topic_id,count(*) n FROM main.mentions GROUP BY topic_id').all()
    .filter((row) => ineligibleTopicIds.has(String(row.topic_id)))
    .reduce((total, row) => total + Number(row.n), 0);
  const unexpectedRemoved = Math.max(0, removed - expectedFilteredMentions);
  const newCandidateTopicIds = new Set(db.prepare('SELECT c.topic_id FROM cand.topics c LEFT JOIN main.topics p ON p.topic_id=c.topic_id WHERE p.topic_id IS NULL').all()
    .map((row) => String(row.topic_id)));
  const expectedAddedMentions = db.prepare('SELECT topic_id,count(*) n FROM cand.mentions GROUP BY topic_id').all()
    .filter((row) => newCandidateTopicIds.has(String(row.topic_id)))
    .reduce((total, row) => total + Number(row.n), 0);
  const unexpectedAdded = Math.max(0, added - expectedAddedMentions);
  const topicsAdded = Number(db.prepare('SELECT count(*) n FROM cand.topics c LEFT JOIN main.topics p ON p.topic_id=c.topic_id WHERE p.topic_id IS NULL').get().n);
  const topChanges = db.prepare(`
    WITH prod AS (
      SELECT s.code,${canonicalMarketSql('s')} market,max(s.name) name,count(*) mentions,sum(m.direction='bullish') bullish
      FROM main.mentions m JOIN main.securities s ON s.id=m.security_id GROUP BY s.code,${canonicalMarketSql('s')}
    ), candidate AS (
      SELECT s.code,${canonicalMarketSql('s')} market,max(s.name) name,count(*) mentions,sum(m.direction='bullish') bullish
      FROM cand.mentions m JOIN cand.securities s ON s.id=m.security_id GROUP BY s.code,${canonicalMarketSql('s')}
    )
    SELECT candidate.name,candidate.code,candidate.market,
           coalesce(prod.mentions,0) production_mentions,candidate.mentions candidate_mentions,
           coalesce(prod.bullish,0) production_bullish,candidate.bullish candidate_bullish
    FROM candidate LEFT JOIN prod USING(code,market)
    WHERE candidate.mentions<>coalesce(prod.mentions,0) OR candidate.bullish<>coalesce(prod.bullish,0)
    ORDER BY abs(candidate.mentions-coalesce(prod.mentions,0))+abs(candidate.bullish-coalesce(prod.bullish,0)) DESC
    LIMIT 20
  `).all();
  db.exec('DETACH DATABASE cand');
  db.close();
  return {
    productionCounts, candidateCounts, changed, added, removed, productionTotal, candidateTotal, topicsAdded,
    expectedFilteredTopics: ineligibleTopicIds.size, expectedFilteredMentions, unexpectedRemoved,
    expectedAddedTopics: newCandidateTopicIds.size, expectedAddedMentions, unexpectedAdded,
    changeRatio: productionTotal ? (changed + added + removed) / productionTotal : 0,
    gatedChangeRatio: productionTotal ? (changed + unexpectedAdded + unexpectedRemoved) / productionTotal : 0,
    topChanges,
  };
}

function initialize() {
  const db = openProduction();
  const timestamp = now();
  const incremental = ensureIncrementalMetadata(db);
  const current = db.prepare("SELECT value FROM pipeline_state WHERE key='current_version'").get()?.value || null;
  const versionStatus = !current || current === version ? 'published' : 'draft';
  db.prepare(`INSERT INTO extraction_versions(version,config_json,status,created_at,published_at) VALUES(?,?,?,?,?) ON CONFLICT(version) DO UPDATE SET config_json=excluded.config_json`)
    .run(version, configJson, versionStatus, timestamp, versionStatus === 'published' ? timestamp : null);
  if (!current) db.prepare(`INSERT INTO pipeline_state(key,value) VALUES('current_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`).run(version);
  db.prepare(`INSERT INTO pipeline_state(key,value) VALUES('pipeline_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value`).run(config.pipeline_version);
  const hasBaseline = db.prepare("SELECT count(*) n FROM pipeline_runs WHERE command='baseline'").get().n;
  if (!hasBaseline) db.prepare('INSERT INTO pipeline_runs(command,status,rules_version,started_at,finished_at,metrics_json) VALUES(?,?,?,?,?,?)')
    .run('baseline', 'published', version, timestamp, timestamp, JSON.stringify({ topics: db.prepare('SELECT count(*) n FROM topics').get().n, mentions: db.prepare('SELECT count(*) n FROM mentions').get().n }));
  db.close();
  console.log(JSON.stringify({ status: 'initialized', version, current_version: current || version, database: DB_PATH, incremental_metadata: incremental }, null, 2));
}

function extract(mode = 'incremental') {
  const db = openProduction();
  ensureIncrementalMetadata(db);
  const startedAt = now();
  const rawInput = process.env.PIPELINE_RAW_INPUT ? resolve(process.env.PIPELINE_RAW_INPUT) : null;
  const result = db.prepare('INSERT INTO pipeline_runs(command,status,rules_version,started_at,candidate_db,source_json) VALUES(?,?,?,?,?,?)')
    .run(mode === 'backfill' ? 'backfill' : mode === 'reprocess' ? 'reprocess' : 'extract', 'running', version, startedAt, CANDIDATE_PATH, rawInput || '');
  const runId = Number(result.lastInsertRowid);
  if (existsSync(CANDIDATE_PATH)) unlinkSync(CANDIDATE_PATH);
  db.exec(`VACUUM INTO '${sqlPath(CANDIDATE_PATH)}'`);
  db.close();
  try {
    if (mode === 'backfill') runScript('scripts/backfill_all.mjs', { ZSXQ_DB: CANDIDATE_PATH });
    else if (mode === 'reprocess') runScript('scripts/build_database.mjs', {
      ZSXQ_DB: CANDIDATE_PATH,
      ZSXQ_REPROCESS_ALL: '1',
      ZSXQ_PRESERVE_DIRECTIONS: process.env.PIPELINE_PRESERVE_DIRECTIONS || '1',
    });
    else if (rawInput) runScript('scripts/build_database.mjs', { ZSXQ_DB: CANDIDATE_PATH, ZSXQ_RAW_OUTPUT: rawInput });
    runScript('scripts/reclassify_directions.mjs', { ZSXQ_DB: CANDIDATE_PATH, ZSXQ_DIRECTION_REPORT: resolve('reports/candidate_direction_reclassification.json') });
    const candidate = new DatabaseSync(CANDIDATE_PATH, { readOnly: true });
    const lastIngestion = candidate.prepare('SELECT topic_count,processed_topic_count,skipped_topic_count,new_topic_count,changed_topic_count,excluded_topic_count FROM ingestion_runs ORDER BY id DESC LIMIT 1').get() || {};
    const metrics = { topics: candidate.prepare('SELECT count(*) n FROM topics').get().n, mentions: candidate.prepare('SELECT count(*) n FROM mentions').get().n, directions: directionCounts(candidate), incremental: lastIngestion };
    candidate.close();
    const production = openProduction();
    production.prepare('UPDATE pipeline_runs SET status=?,finished_at=?,metrics_json=? WHERE id=?').run('candidate', now(), JSON.stringify(metrics), runId);
    production.prepare('INSERT INTO extraction_versions(version,config_json,status,created_at,run_id) VALUES(?,?,?,?,?) ON CONFLICT(version) DO UPDATE SET config_json=excluded.config_json,status=excluded.status,created_at=excluded.created_at,run_id=excluded.run_id')
      .run(version, configJson, 'candidate', startedAt, runId);
    production.close();
    const manifest = { run_id: runId, version, config_hash: configHash, created_at: startedAt, candidate_db: CANDIDATE_PATH, source_json: rawInput, metrics, status: 'candidate' };
    writeManifest(manifest);
    console.log(JSON.stringify(manifest, null, 2));
  } catch (error) {
    const production = openProduction();
    production.prepare('UPDATE pipeline_runs SET status=?,finished_at=?,error=? WHERE id=?').run('failed', now(), error.message, runId);
    production.close();
    throw error;
  }
}

function validate() {
  const manifest = readManifest();
  if (!existsSync(CANDIDATE_PATH)) throw new Error('候选数据库不存在');
  const qualityPath = resolve('reports/candidate_data_quality.json');
  runScript('scripts/check_data_quality.mjs', { ZSXQ_DB: CANDIDATE_PATH, ZSXQ_QUALITY_OUTPUT: qualityPath });
  const quality = JSON.parse(readFileSync(qualityPath, 'utf8'));
  const samples = JSON.parse(readFileSync(GOLDEN_PATH, 'utf8'));
  const golden = samples.map((sample) => ({ ...sample, actual: classifyDirection(sample.text, config).direction })).map((sample) => ({ ...sample, passed: sample.actual === sample.expected }));
  const goldenPassed = golden.filter((sample) => sample.passed).length;
  const goldenRate = golden.length ? goldenPassed / golden.length : 0;
  const entityExtractor = createEntityExtractor(securityMaster, config);
  const entitySamples = JSON.parse(readFileSync(ENTITY_GOLDEN_PATH, 'utf8'));
  const entityGolden = entitySamples.map((sample) => {
    const source = entityExtractor.extractSource(`${sample.title || ''}\n${sample.content || ''}`);
    const actual = entityExtractor.extractMentions(sample, source)
      .map((mention) => `${mention.market}:${mention.code}`)
      .sort();
    const expected = [...sample.expected].sort();
    return { ...sample, actual, passed: JSON.stringify(actual) === JSON.stringify(expected) };
  });
  const entityGoldenPassed = entityGolden.filter((sample) => sample.passed).length;
  const entityGoldenRate = entityGolden.length ? entityGoldenPassed / entityGolden.length : 0;
  const ingestionSamples = JSON.parse(readFileSync(INGESTION_GOLDEN_PATH, 'utf8'));
  const ingestionGolden = ingestionSamples.map((sample) => ({
    ...sample,
    actual: topicIsEligible(sample, config),
    passed: topicIsEligible(sample, config) === sample.expected,
  }));
  const ingestionGoldenPassed = ingestionGolden.filter((sample) => sample.passed).length;
  const ingestionGoldenRate = ingestionGolden.length ? ingestionGoldenPassed / ingestionGolden.length : 0;
  const diff = candidateDiff();
  const critical = quality.checks.duplicate_topic_ids + quality.checks.duplicate_topic_security + quality.checks.orphan_mentions + quality.checks.invalid_directions + quality.checks.future_topics + quality.checks.invalid_security_markets + quality.checks.legacy_a_markets + quality.checks.non_pure_text_topics;
  const failures = [];
  if (critical > 0) failures.push(`critical_data_checks=${critical}`);
  if (goldenRate < config.quality_gates.golden_pass_rate) failures.push(`golden_pass_rate=${goldenRate}`);
  if (entityGoldenRate < config.quality_gates.golden_entity_pass_rate) failures.push(`golden_entity_pass_rate=${entityGoldenRate}`);
  if (ingestionGoldenRate < config.quality_gates.golden_ingestion_pass_rate) failures.push(`golden_ingestion_pass_rate=${ingestionGoldenRate}`);
  if (diff.gatedChangeRatio > config.quality_gates.max_direction_change_ratio) failures.push(`gated_change_ratio=${diff.gatedChangeRatio}`);
  if (!config.quality_gates.allow_coverage_warning && quality.status !== 'pass') failures.push(`quality_status=${quality.status}`);
  const candidateDb = new DatabaseSync(CANDIDATE_PATH, { readOnly: true });
  const hasAiRuns = candidateDb.prepare("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name='ai_enrichment_runs'").get().n > 0;
  let aiEnrichment = null;
  let viewpointQuality = null;
  if (hasAiRuns) {
    // Audit/pilot runs are intentionally paused and may cover only a sample. Validate
    // the enrichment run that actually produced this candidate, not the newest audit.
    aiEnrichment = manifest.ai_enrichment_run_id
      ? candidateDb.prepare('SELECT * FROM ai_enrichment_runs WHERE id=?').get(manifest.ai_enrichment_run_id) || null
      : candidateDb.prepare('SELECT * FROM ai_enrichment_runs ORDER BY id DESC LIMIT 1').get() || null;
    if (aiEnrichment) {
      // The enrichment runner can intentionally scope work (for example, only topics
      // with no existing target). It writes the manifest only after its scoped missing
      // and verification gates reach zero, so a completed run has no publish-blocking
      // pending items even though it does not have a row for every database topic.
      const pending = aiEnrichment.status === 'complete'
        ? 0
        : Math.max(0, Number(aiEnrichment.total_topics) - Number(aiEnrichment.completed_topics));
      const failedAi = Number(candidateDb.prepare("SELECT count(*) n FROM ai_enrichment_results WHERE run_id=? AND status='failed'").get(aiEnrichment.id).n);
      aiEnrichment = { ...aiEnrichment, pending_topics: pending, failed_results: failedAi };
      if (aiEnrichment.status !== 'complete') failures.push(`ai_enrichment_status=${aiEnrichment.status}`);
      if (pending > 0) failures.push(`ai_enrichment_pending=${pending}`);
      if (String(aiEnrichment.prompt_version || '').startsWith('viewpoints-') && aiEnrichment.status === 'complete') {
        runScript('scripts/check_viewpoint_enrichment.mjs', {}, [`--run=${aiEnrichment.id}`, '--write']);
        viewpointQuality = JSON.parse(readFileSync(resolve('reports/viewpoint_enrichment_quality.json'), 'utf8'));
        if (!viewpointQuality.passed) failures.push('viewpoint_quality_gate_failed');
        if (Number(viewpointQuality.materialized?.resolution_queue_pending || 0) > 0) failures.push(`security_resolution_pending=${viewpointQuality.materialized.resolution_queue_pending}`);
      }
    }
  }
  candidateDb.close();
  const validation = {
    checked_at: now(), passed: failures.length === 0, failures, quality,
    golden: { passed: goldenPassed, total: golden.length, rate: goldenRate, samples: golden },
    entity_golden: { passed: entityGoldenPassed, total: entityGolden.length, rate: entityGoldenRate, samples: entityGolden },
    ingestion_golden: { passed: ingestionGoldenPassed, total: ingestionGolden.length, rate: ingestionGoldenRate, samples: ingestionGolden },
    diff, ai_enrichment: aiEnrichment, viewpoint_quality: viewpointQuality,
  };
  writeFileSync(resolve('reports/pipeline_validation.json'), JSON.stringify(validation, null, 2), 'utf8');
  manifest.validation = { passed: validation.passed, checked_at: validation.checked_at, failures, golden_rate: goldenRate, entity_golden_rate: entityGoldenRate, ingestion_golden_rate: ingestionGoldenRate, change_ratio: diff.changeRatio, gated_change_ratio: diff.gatedChangeRatio };
  manifest.status = validation.passed ? 'validated' : 'failed';
  writeManifest(manifest);
  const db = openProduction();
  db.prepare('UPDATE pipeline_runs SET status=?,finished_at=?,metrics_json=?,error=? WHERE id=?')
    .run(manifest.status, now(), JSON.stringify({ ...manifest.metrics, validation: manifest.validation }), failures.join('; '), manifest.run_id);
  db.prepare('UPDATE extraction_versions SET status=? WHERE version=?').run(manifest.status, version);
  db.close();
  console.log(JSON.stringify({ status: manifest.status, failures, golden: `${goldenPassed}/${golden.length}`, entity_golden: `${entityGoldenPassed}/${entityGolden.length}`, ingestion_golden: `${ingestionGoldenPassed}/${ingestionGolden.length}`, change_ratio: diff.changeRatio, gated_change_ratio: diff.gatedChangeRatio, quality: quality.status }, null, 2));
  if (!validation.passed) process.exitCode = 1;
}

function diff() {
  const manifest = readManifest();
  const metrics = candidateDiff();
  const payload = { generated_at: now(), version: manifest.version, run_id: manifest.run_id, ...metrics };
  writeFileSync(resolve('reports/pipeline_diff.json'), JSON.stringify(payload, null, 2), 'utf8');
  const lines = [
    '# Pipeline 候选版本差异', '',
    `- 版本：${manifest.version}`, `- 运行 ID：${manifest.run_id}`, `- 新增主题：${metrics.topicsAdded}`,
    `- 正式提及：${metrics.productionTotal}`, `- 候选提及：${metrics.candidateTotal}`,
    `- 新增 / 删除 / 方向变化：${metrics.added} / ${metrics.removed} / ${metrics.changed}`,
    `- 预期过滤主题 / 提及：${metrics.expectedFilteredTopics} / ${metrics.expectedFilteredMentions}`,
    `- 预期恢复主题 / 提及：${metrics.expectedAddedTopics} / ${metrics.expectedAddedMentions}`,
    `- 总变化率：${(metrics.changeRatio * 100).toFixed(2)}%`, `- 排除口径收窄后的门禁变化率：${(metrics.gatedChangeRatio * 100).toFixed(2)}%`, '',
    '| 标的 | 正式提及 | 候选提及 | 正式看多 | 候选看多 |', '|---|---:|---:|---:|---:|',
    ...metrics.topChanges.map((row) => `| ${row.name} ${row.code}/${row.market} | ${row.production_mentions} | ${row.candidate_mentions} | ${row.production_bullish} | ${row.candidate_bullish} |`),
  ];
  writeFileSync(resolve('reports/pipeline_diff.md'), lines.join('\n'), 'utf8');
  console.log(JSON.stringify(payload, null, 2));
}

function publish() {
  const manifest = readManifest();
  if (manifest.status !== 'validated' || !manifest.validation?.passed) throw new Error('候选版本尚未通过质量门禁，禁止发布');
  if (!existsSync(CANDIDATE_PATH)) throw new Error('候选数据库不存在');
  const backupDir = resolve('data/backups');
  mkdirSync(backupDir, { recursive: true });
  const backupPath = resolve(backupDir, `broker_notes_before_${new Date().toISOString().replace(/[:.]/g, '-')}.sqlite`);
  const db = openProduction();
  ensureIncrementalMetadata(db);
  db.exec(`VACUUM INTO '${sqlPath(backupPath)}'`);
  db.prepare('ATTACH DATABASE ? AS cand').run(CANDIDATE_PATH);
  db.exec('PRAGMA foreign_keys=OFF; BEGIN IMMEDIATE;');
  try {
    db.exec(`
      DELETE FROM main.ai_enrichment_results;
      DELETE FROM main.model_viewpoint_mentions;
      DELETE FROM main.model_viewpoints;
      DELETE FROM main.security_resolution_queue;
      DELETE FROM main.viewpoints;
      DELETE FROM main.mentions;
      DELETE FROM main.securities;
      DELETE FROM main.topics;
      DELETE FROM main.ingestion_runs;
      DELETE FROM main.ai_enrichment_runs;
      INSERT INTO main.ingestion_runs SELECT * FROM cand.ingestion_runs;
      INSERT INTO main.topics SELECT * FROM cand.topics;
      INSERT INTO main.securities SELECT * FROM cand.securities;
      INSERT INTO main.mentions SELECT * FROM cand.mentions;
      INSERT INTO main.viewpoints SELECT * FROM cand.viewpoints;
      INSERT INTO main.ai_enrichment_runs SELECT * FROM cand.ai_enrichment_runs;
      INSERT INTO main.ai_enrichment_results SELECT * FROM cand.ai_enrichment_results;
      INSERT INTO main.model_viewpoints SELECT * FROM cand.model_viewpoints;
      INSERT INTO main.model_viewpoint_mentions SELECT * FROM cand.model_viewpoint_mentions;
      INSERT INTO main.security_resolution_queue SELECT * FROM cand.security_resolution_queue;
      COMMIT;
    `);
  } catch (error) {
    db.exec('ROLLBACK');
    db.exec('PRAGMA foreign_keys=ON');
    db.exec('DETACH DATABASE cand');
    db.close();
    throw error;
  }
  db.exec('PRAGMA foreign_keys=ON');
  db.exec('DETACH DATABASE cand');
  const publishedAt = now();
  db.prepare('UPDATE extraction_versions SET status=?,published_at=?,run_id=? WHERE version=?').run('published', publishedAt, manifest.run_id, manifest.version);
  db.prepare("UPDATE extraction_versions SET status='superseded' WHERE version<>? AND status='published'").run(manifest.version);
  db.prepare("INSERT INTO pipeline_state(key,value) VALUES('current_version',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(manifest.version);
  db.prepare("INSERT INTO pipeline_state(key,value) VALUES('last_published_at',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value").run(publishedAt);
  db.prepare('UPDATE pipeline_runs SET status=?,finished_at=? WHERE id=?').run('published', publishedAt, manifest.run_id);
  db.close();
  manifest.status = 'published'; manifest.published_at = publishedAt; manifest.backup = backupPath; writeManifest(manifest);
  console.log(JSON.stringify({ status: 'published', version: manifest.version, run_id: manifest.run_id, backup: backupPath }, null, 2));
}

function status() {
  const db = openProduction();
  const state = Object.fromEntries(db.prepare('SELECT key,value FROM pipeline_state').all().map((row) => [row.key, row.value]));
  const lastRuns = db.prepare('SELECT id,command,status,rules_version,started_at,finished_at,error FROM pipeline_runs ORDER BY id DESC LIMIT 10').all();
  db.close();
  console.log(JSON.stringify({ state, candidate: existsSync(MANIFEST_PATH) ? JSON.parse(readFileSync(MANIFEST_PATH, 'utf8')) : null, last_runs: lastRuns }, null, 2));
}

const handlers = { init: initialize, extract, reprocess: () => extract('reprocess'), backfill: () => extract('backfill'), validate, diff, publish, status };
if (!handlers[command]) throw new Error(`未知命令 ${command}`);
handlers[command]();
