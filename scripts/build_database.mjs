import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { loadPipelineConfig } from './lib/direction_rules.mjs';
import { createEntityExtractor } from './lib/entity_extractor.mjs';
import { contentFingerprint, extractionVersion, securityMasterFingerprint } from './lib/pipeline_version.mjs';
import { canonicalMarket } from './lib/security_market.mjs';
import { topicIsEligible } from './lib/topic_filter.mjs';

const rawPath = resolve(process.env.ZSXQ_RAW_OUTPUT || 'data/zsxq_topics_72h.json');
const masterPath = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
const dbPath = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const forceReprocess = process.env.ZSXQ_REPROCESS_ALL === '1';
const directionSourcePath = process.env.ZSXQ_DIRECTION_SOURCE_DB ? resolve(process.env.ZSXQ_DIRECTION_SOURCE_DB) : null;
const preserveDirections = (forceReprocess || directionSourcePath) && process.env.ZSXQ_PRESERVE_DIRECTIONS !== '0';
let raw = forceReprocess ? null : JSON.parse(readFileSync(rawPath, 'utf8'));
const master = JSON.parse(readFileSync(masterPath, 'utf8'));
const pipelineConfig = loadPipelineConfig();
const currentExtractionVersion = extractionVersion(pipelineConfig, securityMasterFingerprint(master));
const entityExtractor = createEntityExtractor(master, pipelineConfig);

function extractViewpoints(content) {
  const blocks = String(content || '')
    .replace(/\r/g, '')
    .split(/\n{2,}|(?=\n(?:[0-9]+[️⃣.)、]|[一二三四五六七八九十]+[、：]|[🔸➡️☀️🌞🍁🚀]))/)
    .map((text) => text.trim())
    .filter(Boolean);
  return blocks.length ? blocks : [String(content || '').trim()].filter(Boolean);
}

const db = new DatabaseSync(dbPath);
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA foreign_keys = ON;
  CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY, started_at TEXT NOT NULL, source_file TEXT NOT NULL,
    group_id TEXT NOT NULL, cutoff_at TEXT NOT NULL, topic_count INTEGER NOT NULL
  );
  CREATE TABLE IF NOT EXISTS topics (
    topic_id TEXT PRIMARY KEY, group_id TEXT NOT NULL, group_name TEXT,
    published_at TEXT NOT NULL, published_at_utc TEXT NOT NULL, title TEXT, content TEXT NOT NULL,
    poster_name TEXT, poster_alias TEXT, source_broker TEXT, source_team TEXT, source_raw TEXT,
    readers INTEGER, likes INTEGER, comments INTEGER, raw_json TEXT NOT NULL, ingested_at TEXT NOT NULL
  );
  CREATE TABLE IF NOT EXISTS securities (
    id INTEGER PRIMARY KEY, code TEXT NOT NULL, market TEXT NOT NULL, name TEXT NOT NULL,
    UNIQUE(code, market)
  );
  CREATE TABLE IF NOT EXISTS mentions (
    id INTEGER PRIMARY KEY, topic_id TEXT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    security_id INTEGER NOT NULL REFERENCES securities(id), direction TEXT NOT NULL,
    direction_confidence REAL NOT NULL, entity_confidence REAL NOT NULL,
    target_price REAL, target_currency TEXT, target_price_text TEXT, evidence TEXT,
    UNIQUE(topic_id, security_id)
  );
  CREATE TABLE IF NOT EXISTS viewpoints (
    id INTEGER PRIMARY KEY, topic_id TEXT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    mention_id INTEGER REFERENCES mentions(id) ON DELETE SET NULL, ordinal INTEGER NOT NULL, text TEXT NOT NULL
  );
  CREATE INDEX IF NOT EXISTS idx_topics_published ON topics(published_at_utc);
  CREATE INDEX IF NOT EXISTS idx_mentions_security ON mentions(security_id, direction);
  CREATE INDEX IF NOT EXISTS idx_viewpoints_topic ON viewpoints(topic_id, ordinal);
  CREATE VIEW IF NOT EXISTS v_mentions_enriched AS
    SELECT m.id AS mention_id, t.topic_id, t.published_at, t.published_at_utc,
           t.source_broker, t.source_team, s.code, s.market, s.name,
           m.direction, m.target_price, m.target_currency,
           m.entity_confidence, m.direction_confidence, m.evidence
    FROM mentions m JOIN topics t ON t.topic_id=m.topic_id JOIN securities s ON s.id=m.security_id;
  CREATE VIEW IF NOT EXISTS v_hotness_24h AS
    SELECT s.name,s.code,s.market,count(DISTINCT m.topic_id) mentions,
           sum(m.direction='bullish') bullish,sum(m.direction='bearish') bearish,sum(m.direction='neutral') neutral
    FROM mentions m JOIN securities s ON s.id=m.security_id JOIN topics t ON t.topic_id=m.topic_id
    WHERE t.published_at_utc>=datetime('now','-24 hours') AND m.entity_confidence>=0.75 GROUP BY s.id;
  CREATE VIEW IF NOT EXISTS v_hotness_72h AS
    SELECT s.name,s.code,s.market,count(DISTINCT m.topic_id) mentions,
           sum(m.direction='bullish') bullish,sum(m.direction='bearish') bearish,sum(m.direction='neutral') neutral
    FROM mentions m JOIN securities s ON s.id=m.security_id JOIN topics t ON t.topic_id=m.topic_id
    WHERE t.published_at_utc>=datetime('now','-72 hours') AND m.entity_confidence>=0.75 GROUP BY s.id;
  CREATE VIEW IF NOT EXISTS v_hotness_7d AS
    SELECT s.name,s.code,s.market,count(DISTINCT m.topic_id) mentions,
           sum(m.direction='bullish') bullish,sum(m.direction='bearish') bearish,sum(m.direction='neutral') neutral
    FROM mentions m JOIN securities s ON s.id=m.security_id JOIN topics t ON t.topic_id=m.topic_id
    WHERE t.published_at_utc>=datetime('now','-7 days') AND m.entity_confidence>=0.75 GROUP BY s.id;
`);

function ensureColumn(table, name, definition) {
  const exists = db.prepare(`PRAGMA table_info(${table})`).all().some((column) => column.name === name);
  if (!exists) db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
}

ensureColumn('topics', 'content_hash', 'TEXT');
ensureColumn('topics', 'extraction_version', 'TEXT');
ensureColumn('topics', 'extracted_at', 'TEXT');
ensureColumn('ingestion_runs', 'processed_topic_count', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('ingestion_runs', 'skipped_topic_count', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('ingestion_runs', 'new_topic_count', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('ingestion_runs', 'changed_topic_count', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('ingestion_runs', 'excluded_topic_count', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('mentions', 'recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
ensureColumn('mentions', 'recognition_model', "TEXT NOT NULL DEFAULT ''");
ensureColumn('mentions', 'recognition_confidence', 'REAL');
ensureColumn('mentions', 'is_mixed', 'INTEGER NOT NULL DEFAULT 0');
ensureColumn('topics', 'source_recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
ensureColumn('topics', 'source_recognition_model', "TEXT NOT NULL DEFAULT ''");
ensureColumn('topics', 'source_recognition_confidence', 'REAL');
db.exec(`
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
ensureColumn('model_viewpoints', 'source_block_ordinals', "TEXT NOT NULL DEFAULT '[]'");
db.exec('CREATE INDEX IF NOT EXISTS idx_topics_extraction_version ON topics(extraction_version)');

if (forceReprocess) {
  const topics = db.prepare('SELECT raw_json FROM topics ORDER BY published_at_utc DESC').all()
    .map((row) => {
      try { return JSON.parse(row.raw_json); } catch { return null; }
    })
    .filter(Boolean);
  raw = {
    group_id: topics[0]?.group?.group_id || '',
    group_name: topics[0]?.group?.name || '',
    cutoff_at: topics.at(-1)?.create_time || new Date(0).toISOString(),
    topics,
  };
}

const upsertTopic = db.prepare(`
  INSERT INTO topics(topic_id,group_id,group_name,published_at,published_at_utc,title,content,poster_name,poster_alias,
    source_broker,source_team,source_raw,readers,likes,comments,raw_json,ingested_at)
  VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  ON CONFLICT(topic_id) DO UPDATE SET
    published_at=excluded.published_at,published_at_utc=excluded.published_at_utc,title=excluded.title,
    content=excluded.content,source_broker=excluded.source_broker,source_team=excluded.source_team,
    source_raw=excluded.source_raw,readers=excluded.readers,likes=excluded.likes,comments=excluded.comments,
    raw_json=excluded.raw_json,ingested_at=excluded.ingested_at
`);
const upsertSecurity = db.prepare('INSERT INTO securities(code,market,name) VALUES(?,?,?) ON CONFLICT(code,market) DO UPDATE SET name=excluded.name');
const getSecurity = db.prepare('SELECT id FROM securities WHERE code=? AND market=?');
const insertMention = db.prepare(`INSERT INTO mentions(topic_id,security_id,direction,direction_confidence,entity_confidence,target_price,target_currency,target_price_text,evidence) VALUES(?,?,?,?,?,?,?,?,?)`);
const insertViewpoint = db.prepare('INSERT INTO viewpoints(topic_id,mention_id,ordinal,text) VALUES(?,?,?,?)');
const deleteMentions = db.prepare('DELETE FROM mentions WHERE topic_id=?');
const deleteViewpoints = db.prepare('DELETE FROM viewpoints WHERE topic_id=?');
const deleteTopic = db.prepare('DELETE FROM topics WHERE topic_id=?');
const getTopicExtraction = db.prepare('SELECT content_hash,extraction_version FROM topics WHERE topic_id=?');
const updateTopicExtraction = db.prepare('UPDATE topics SET content_hash=?,extraction_version=?,extracted_at=? WHERE topic_id=?');
function loadPreviousDirections() {
  if (!preserveDirections) return new Map();
  const sourceDb = directionSourcePath ? new DatabaseSync(directionSourcePath, { readOnly: true }) : db;
  try {
    return new Map(sourceDb.prepare(`
        SELECT m.topic_id,s.code,s.market,m.direction,m.direction_confidence
        FROM mentions m JOIN securities s ON s.id=m.security_id
      `).all().map((row) => [`${row.topic_id}\u0000${canonicalMarket(row.market, row.code)}\u0000${row.code}`, row]));
  } finally {
    if (sourceDb !== db) sourceDb.close();
  }
}
const previousDirections = loadPreviousDirections();
const previousAiMentions = new Map();
for (const row of db.prepare(`
  SELECT m.topic_id,t.content_hash,s.code,s.market,s.name,m.direction,m.direction_confidence,m.entity_confidence,
         m.target_price,m.target_currency,m.target_price_text,m.evidence,m.recognition_model,m.recognition_confidence
  FROM mentions m JOIN topics t ON t.topic_id=m.topic_id JOIN securities s ON s.id=m.security_id
  WHERE m.recognition_method='gpt_verified'
`).all()) {
  if (!previousAiMentions.has(String(row.topic_id))) previousAiMentions.set(String(row.topic_id), []);
  previousAiMentions.get(String(row.topic_id)).push(row);
}
const previousAiSources = new Map(db.prepare(`
  SELECT topic_id,content_hash,source_broker,source_team,source_raw,source_recognition_model,source_recognition_confidence
  FROM topics WHERE source_recognition_method='gpt_verified'
`).all().map((row) => [String(row.topic_id), row]));
const updatePreservedSource = db.prepare(`UPDATE topics SET source_broker=?,source_team=?,source_raw=?,source_recognition_method='gpt_verified',source_recognition_model=?,source_recognition_confidence=? WHERE topic_id=?`);
const updatePreservedMention = db.prepare(`UPDATE mentions SET direction=?,direction_confidence=?,entity_confidence=?,target_price=?,target_currency=?,target_price_text=?,evidence=?,recognition_method='gpt_verified',recognition_model=?,recognition_confidence=? WHERE id=?`);
const insertPreservedMention = db.prepare(`INSERT INTO mentions(topic_id,security_id,direction,direction_confidence,entity_confidence,target_price,target_currency,target_price_text,evidence,recognition_method,recognition_model,recognition_confidence) VALUES(?,?,?,?,?,?,?,?,?,'gpt_verified',?,?)`);

function runImport() {
  const importMetrics = { processed: 0, skipped: 0, new_topics: 0, changed_topics: 0, excluded_topics: 0, removed_ineligible_topics: 0 };
  db.exec('BEGIN IMMEDIATE');
  try {
  const now = new Date().toISOString();
  if (forceReprocess) {
    db.exec('DELETE FROM viewpoints; DELETE FROM mentions; DELETE FROM topics; DELETE FROM securities;');
  }
  for (const topic of raw.topics) {
    const topicId = String(topic.topic_id);
    const previous = getTopicExtraction.get(topicId);
    if (!topicIsEligible(topic, pipelineConfig)) {
      importMetrics.excluded_topics += 1;
      if (previous && !forceReprocess) {
        deleteTopic.run(topicId);
        importMetrics.removed_ineligible_topics += 1;
      }
      continue;
    }
    const nextContentHash = contentFingerprint(topic.title, topic.content);
    const source = entityExtractor.extractSource(`${topic.title || ''}\n${topic.content || ''}`);
    upsertTopic.run(
      String(topic.topic_id), String(topic.group?.group_id || raw.group_id), topic.group?.name || raw.group_name,
      topic.create_time, new Date(topic.create_time).toISOString(), topic.title || '', topic.content || '',
      topic.owner?.name || '', topic.owner?.alias || '', source.broker, source.team, source.raw,
      topic.counts?.readers || 0, topic.counts?.likes || 0, topic.counts?.comments || 0,
      JSON.stringify(topic), now,
    );
    const preservedSource = previousAiSources.get(topicId);
    if (preservedSource?.content_hash === nextContentHash) {
      updatePreservedSource.run(preservedSource.source_broker, preservedSource.source_team, preservedSource.source_raw,
        preservedSource.source_recognition_model, preservedSource.source_recognition_confidence, topicId);
    }
    if (!forceReprocess && previous?.content_hash === nextContentHash && previous?.extraction_version === currentExtractionVersion) {
      importMetrics.skipped += 1;
      continue;
    }
    importMetrics.processed += 1;
    if (previous) importMetrics.changed_topics += 1;
    else importMetrics.new_topics += 1;
    if (!forceReprocess) {
      deleteViewpoints.run(topicId);
      deleteMentions.run(topicId);
    }
    const mentions = entityExtractor.extractMentions(topic, source);
    const mentionIds = new Map();
    for (const mention of mentions) {
      const previousDirection = previousDirections.get(`${topicId}\u0000${mention.market}\u0000${mention.code}`);
      if (previousDirection) {
        mention.direction = previousDirection.direction;
        mention.direction_confidence = previousDirection.direction_confidence;
      }
      upsertSecurity.run(String(mention.code), mention.market, mention.name);
      const securityId = getSecurity.get(String(mention.code), mention.market).id;
      const result = insertMention.run(
        topicId, securityId, mention.direction, mention.direction_confidence, mention.confidence,
        mention.target?.value ?? null, mention.target?.currency || null, mention.target?.raw || null, mention.evidence.slice(0, 1000),
      );
      mentionIds.set(mention.name, Number(result.lastInsertRowid));
      mentionIds.set(mention.alias, Number(result.lastInsertRowid));
    }
    const preservedMentions = (previousAiMentions.get(topicId) || []).filter((row) => row.content_hash === nextContentHash);
    for (const preserved of preservedMentions) {
      const market = canonicalMarket(preserved.market, preserved.code);
      upsertSecurity.run(String(preserved.code), market, preserved.name);
      const securityId = getSecurity.get(String(preserved.code), market).id;
      const existingMention = db.prepare('SELECT id FROM mentions WHERE topic_id=? AND security_id=?').get(topicId, securityId);
      let mentionId;
      if (existingMention) {
        updatePreservedMention.run(preserved.direction, preserved.direction_confidence, preserved.entity_confidence,
          preserved.target_price, preserved.target_currency, preserved.target_price_text, preserved.evidence,
          preserved.recognition_model, preserved.recognition_confidence, existingMention.id);
        mentionId = Number(existingMention.id);
      } else {
        mentionId = Number(insertPreservedMention.run(topicId, securityId, preserved.direction, preserved.direction_confidence,
          preserved.entity_confidence, preserved.target_price, preserved.target_currency, preserved.target_price_text,
          preserved.evidence, preserved.recognition_model, preserved.recognition_confidence).lastInsertRowid);
      }
      mentionIds.set(preserved.name, mentionId);
    }
    const viewpoints = extractViewpoints(topic.content);
    viewpoints.forEach((viewpoint, index) => {
      let mentionId = null;
      const linked = [...mentionIds.entries()].filter(([name]) => viewpoint.includes(name)).map(([, id]) => id);
      if (new Set(linked).size === 1) mentionId = linked[0];
      else if (mentions.length === 1) mentionId = [...mentionIds.values()][0];
      insertViewpoint.run(topicId, mentionId, index + 1, viewpoint.slice(0, 8000));
    });
    updateTopicExtraction.run(nextContentHash, currentExtractionVersion, now, topicId);
  }
  db.prepare('INSERT INTO ingestion_runs(started_at,source_file,group_id,cutoff_at,topic_count,processed_topic_count,skipped_topic_count,new_topic_count,changed_topic_count,excluded_topic_count) VALUES(?,?,?,?,?,?,?,?,?,?)')
    .run(now, forceReprocess ? `database://${dbPath}` : rawPath, String(raw.group_id), raw.cutoff_at, raw.topics.length, importMetrics.processed, importMetrics.skipped, importMetrics.new_topics, importMetrics.changed_topics, importMetrics.excluded_topics);
    db.prepare('DELETE FROM securities WHERE id NOT IN (SELECT DISTINCT security_id FROM mentions)').run();
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
  return importMetrics;
}

const importMetrics = runImport();
const counts = {
  database: dbPath,
  topics: db.prepare('SELECT count(*) n FROM topics').get().n,
  mentions: db.prepare('SELECT count(*) n FROM mentions').get().n,
  securities: db.prepare('SELECT count(*) n FROM securities').get().n,
  viewpoints: db.prepare('SELECT count(*) n FROM viewpoints').get().n,
  sourced_topics: db.prepare("SELECT count(*) n FROM topics WHERE source_broker<>''").get().n,
  topics_with_mentions: db.prepare('SELECT count(DISTINCT topic_id) n FROM mentions').get().n,
  target_prices: db.prepare('SELECT count(*) n FROM mentions WHERE target_price IS NOT NULL').get().n,
  incremental: { ...importMetrics, extraction_version: currentExtractionVersion, attachment_policy: pipelineConfig.ingestion?.attachments?.policy || 'metadata_only' },
};
console.log(JSON.stringify(counts, null, 2));
db.close();
