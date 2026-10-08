import { mkdirSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

export const DEFAULT_DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');

export function openWorkflowDb(path = DEFAULT_DB_PATH, options = {}) {
  mkdirSync(dirname(path), { recursive: true });
  const db = new DatabaseSync(path, options);
  if (!options.readOnly) {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;');
    ensureWorkflowSchema(db);
  }
  return db;
}

export function ensureWorkflowSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS schema_meta (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    );
    INSERT INTO schema_meta(key,value) VALUES('schema_version','broker-note-v2')
      ON CONFLICT(key) DO UPDATE SET value=excluded.value;

    CREATE TABLE IF NOT EXISTS workflow_runs (
      id INTEGER PRIMARY KEY,
      command TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('running','success','failed','cancelled')),
      started_at TEXT NOT NULL,
      finished_at TEXT,
      input_count INTEGER NOT NULL DEFAULT 0,
      success_count INTEGER NOT NULL DEFAULT 0,
      failed_count INTEGER NOT NULL DEFAULT 0,
      skipped_count INTEGER NOT NULL DEFAULT 0,
      model_provider TEXT,
      model_id TEXT,
      prompt_version TEXT,
      metadata_json TEXT NOT NULL DEFAULT '{}',
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS workflow_steps (
      id INTEGER PRIMARY KEY,
      workflow_run_id INTEGER NOT NULL REFERENCES workflow_runs(id) ON DELETE CASCADE,
      step_key TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('pending','running','success','failed','skipped')),
      started_at TEXT,
      finished_at TEXT,
      input_count INTEGER NOT NULL DEFAULT 0,
      success_count INTEGER NOT NULL DEFAULT 0,
      failed_count INTEGER NOT NULL DEFAULT 0,
      message TEXT,
      UNIQUE(workflow_run_id,step_key)
    );

    CREATE TABLE IF NOT EXISTS ingestion_runs (
      id INTEGER PRIMARY KEY,
      workflow_run_id INTEGER REFERENCES workflow_runs(id),
      group_id TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      cutoff_at TEXT NOT NULL,
      window_hours INTEGER NOT NULL,
      pages_fetched INTEGER NOT NULL DEFAULT 0,
      topic_count INTEGER NOT NULL DEFAULT 0,
      eligible_count INTEGER NOT NULL DEFAULT 0,
      excluded_count INTEGER NOT NULL DEFAULT 0,
      new_count INTEGER NOT NULL DEFAULT 0,
      changed_count INTEGER NOT NULL DEFAULT 0,
      unchanged_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL,
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS raw_topics (
      topic_id TEXT PRIMARY KEY,
      group_id TEXT NOT NULL,
      group_name TEXT,
      topic_type TEXT,
      published_at TEXT NOT NULL,
      published_at_utc TEXT NOT NULL,
      title TEXT NOT NULL DEFAULT '',
      content TEXT NOT NULL DEFAULT '',
      poster_name TEXT,
      poster_alias TEXT,
      readers INTEGER NOT NULL DEFAULT 0,
      likes INTEGER NOT NULL DEFAULT 0,
      comments INTEGER NOT NULL DEFAULT 0,
      has_images INTEGER NOT NULL DEFAULT 0 CHECK(has_images IN (0,1)),
      has_files INTEGER NOT NULL DEFAULT 0 CHECK(has_files IN (0,1)),
      eligibility TEXT NOT NULL CHECK(eligibility IN ('eligible','excluded')),
      exclusion_reason TEXT,
      content_hash TEXT NOT NULL,
      raw_json TEXT NOT NULL,
      first_seen_at TEXT NOT NULL,
      last_seen_at TEXT NOT NULL,
      last_ingestion_run_id INTEGER REFERENCES ingestion_runs(id)
    );
    CREATE INDEX IF NOT EXISTS idx_raw_topics_published ON raw_topics(published_at_utc);
    CREATE INDEX IF NOT EXISTS idx_raw_topics_eligibility ON raw_topics(eligibility,published_at_utc);

    CREATE TABLE IF NOT EXISTS model_runs (
      id INTEGER PRIMARY KEY,
      workflow_run_id INTEGER REFERENCES workflow_runs(id),
      stage TEXT NOT NULL CHECK(stage IN ('classification','extraction')),
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      status TEXT NOT NULL,
      started_at TEXT NOT NULL,
      finished_at TEXT,
      input_count INTEGER NOT NULL DEFAULT 0,
      success_count INTEGER NOT NULL DEFAULT 0,
      failed_count INTEGER NOT NULL DEFAULT 0,
      total_duration_ms INTEGER NOT NULL DEFAULT 0,
      error TEXT
    );

    CREATE TABLE IF NOT EXISTS model_attempts (
      id INTEGER PRIMARY KEY,
      model_run_id INTEGER NOT NULL REFERENCES model_runs(id) ON DELETE CASCADE,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      stage TEXT NOT NULL CHECK(stage IN ('classification','extraction')),
      status TEXT NOT NULL CHECK(status IN ('success','failed')),
      raw_result_json TEXT,
      error TEXT,
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_model_attempts_topic ON model_attempts(topic_id,stage,created_at DESC);

    CREATE TABLE IF NOT EXISTS topic_classifications (
      id INTEGER PRIMARY KEY,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      model_run_id INTEGER NOT NULL REFERENCES model_runs(id),
      content_hash TEXT NOT NULL,
      classification TEXT NOT NULL CHECK(classification IN ('broker_note','not_broker_note','uncertain')),
      confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
      evidence TEXT NOT NULL,
      reasons_json TEXT NOT NULL DEFAULT '[]',
      negative_signals_json TEXT NOT NULL DEFAULT '[]',
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      review_status TEXT NOT NULL DEFAULT 'candidate' CHECK(review_status IN ('candidate','reviewed','rejected')),
      created_at TEXT NOT NULL,
      UNIQUE(topic_id,content_hash,provider,model_id,prompt_version)
    );
    CREATE INDEX IF NOT EXISTS idx_topic_classifications_latest ON topic_classifications(topic_id,created_at DESC);

    CREATE TABLE IF NOT EXISTS extraction_results (
      id INTEGER PRIMARY KEY,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      classification_id INTEGER REFERENCES topic_classifications(id),
      model_run_id INTEGER NOT NULL REFERENCES model_runs(id),
      content_hash TEXT NOT NULL,
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('success','failed','skipped')),
      raw_result_json TEXT,
      validation_json TEXT NOT NULL DEFAULT '{}',
      duration_ms INTEGER NOT NULL DEFAULT 0,
      error TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(topic_id,content_hash,provider,model_id,prompt_version)
    );

    CREATE TABLE IF NOT EXISTS securities (
      id INTEGER PRIMARY KEY,
      ts_code TEXT NOT NULL UNIQUE,
      symbol TEXT NOT NULL,
      market TEXT NOT NULL CHECK(market IN ('SH','SZ','BJ','HK')),
      name TEXT NOT NULL,
      full_name TEXT,
      area TEXT,
      industry TEXT,
      exchange TEXT,
      list_status TEXT,
      list_date TEXT,
      delist_date TEXT,
      source TEXT NOT NULL DEFAULT 'tushare',
      synced_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_securities_symbol ON securities(symbol,market);
    CREATE INDEX IF NOT EXISTS idx_securities_name ON securities(name);

    CREATE TABLE IF NOT EXISTS security_aliases (
      alias TEXT NOT NULL,
      security_id INTEGER NOT NULL REFERENCES securities(id) ON DELETE CASCADE,
      source TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 100,
      PRIMARY KEY(alias,security_id)
    );
    CREATE INDEX IF NOT EXISTS idx_security_alias ON security_aliases(alias,priority);

    CREATE TABLE IF NOT EXISTS viewpoints (
      id INTEGER PRIMARY KEY,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      extraction_result_id INTEGER NOT NULL REFERENCES extraction_results(id) ON DELETE CASCADE,
      ordinal INTEGER NOT NULL,
      summary TEXT NOT NULL,
      direction TEXT NOT NULL CHECK(direction IN ('bullish','bearish')),
      evidence TEXT NOT NULL,
      supporting_evidence_json TEXT NOT NULL DEFAULT '[]',
      recommendation_broker TEXT,
      recommendation_team TEXT NOT NULL DEFAULT '其他',
      recommendation_authors_json TEXT NOT NULL DEFAULT '[]',
      recommendation_evidence TEXT,
      target_price REAL,
      target_currency TEXT,
      target_price_raw TEXT,
      target_price_evidence TEXT,
      confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
      review_status TEXT NOT NULL DEFAULT 'candidate' CHECK(review_status IN ('candidate','reviewed','published','rejected')),
      review_version INTEGER NOT NULL DEFAULT 1,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL,
      published_at TEXT,
      UNIQUE(extraction_result_id,ordinal)
    );
    CREATE INDEX IF NOT EXISTS idx_viewpoints_status ON viewpoints(review_status,topic_id);

    CREATE TABLE IF NOT EXISTS viewpoint_securities (
      id INTEGER PRIMARY KEY,
      viewpoint_id INTEGER NOT NULL REFERENCES viewpoints(id) ON DELETE CASCADE,
      link_order INTEGER NOT NULL DEFAULT 1,
      security_id INTEGER REFERENCES securities(id),
      surface TEXT NOT NULL,
      code_hint TEXT,
      evidence TEXT NOT NULL,
      confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
      resolution_method TEXT,
      resolution_status TEXT NOT NULL CHECK(resolution_status IN ('resolved','ambiguous','unresolved')),
      resolved_at TEXT,
      UNIQUE(viewpoint_id,link_order)
    );
    CREATE INDEX IF NOT EXISTS idx_viewpoint_security_resolved ON viewpoint_securities(security_id,resolution_status);

    CREATE TABLE IF NOT EXISTS security_resolution_queue (
      id INTEGER PRIMARY KEY,
      viewpoint_id INTEGER NOT NULL UNIQUE REFERENCES viewpoints(id) ON DELETE CASCADE,
      surface TEXT NOT NULL,
      code_hint TEXT,
      evidence TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','resolved','dismissed')),
      candidate_security_ids_json TEXT NOT NULL DEFAULT '[]',
      search_query TEXT,
      resolution_source TEXT,
      resolution_url TEXT,
      resolution_notes TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_resolution_queue_status ON security_resolution_queue(status,created_at);

    CREATE TABLE IF NOT EXISTS review_events (
      id INTEGER PRIMARY KEY,
      viewpoint_id INTEGER REFERENCES viewpoints(id) ON DELETE CASCADE,
      classification_id INTEGER REFERENCES topic_classifications(id) ON DELETE CASCADE,
      action TEXT NOT NULL,
      before_json TEXT,
      after_json TEXT,
      note TEXT,
      created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS ai_approval_results (
      id INTEGER PRIMARY KEY,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      model_run_id INTEGER REFERENCES model_runs(id),
      content_hash TEXT NOT NULL,
      provider TEXT NOT NULL,
      model_id TEXT NOT NULL,
      prompt_version TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('success','failed')),
      decision TEXT CHECK(decision IN ('auto_accept','human_review')),
      confidence REAL,
      reasons_json TEXT NOT NULL DEFAULT '[]',
      comparison_json TEXT,
      raw_result_json TEXT,
      applied INTEGER NOT NULL DEFAULT 0 CHECK(applied IN (0,1)),
      error TEXT,
      duration_ms INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL,
      UNIQUE(topic_id,content_hash,provider,model_id,prompt_version)
    );
    CREATE INDEX IF NOT EXISTS idx_ai_approval_decision ON ai_approval_results(decision,status);

    CREATE TABLE IF NOT EXISTS ai_viewpoint_approval_results (
      id INTEGER PRIMARY KEY,
      approval_result_id INTEGER NOT NULL REFERENCES ai_approval_results(id) ON DELETE CASCADE,
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      viewpoint_id INTEGER NOT NULL REFERENCES viewpoints(id) ON DELETE CASCADE,
      decision TEXT NOT NULL CHECK(decision IN ('auto_accept','human_review')),
      confidence REAL NOT NULL,
      reasons_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL,
      UNIQUE(approval_result_id,viewpoint_id)
    );
    CREATE INDEX IF NOT EXISTS idx_ai_viewpoint_approval_decision ON ai_viewpoint_approval_results(decision,topic_id);

    CREATE TABLE IF NOT EXISTS pilot_samples (
      topic_id TEXT PRIMARY KEY REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
      stratum TEXT NOT NULL CHECK(stratum IN ('likely_broker_note','obvious_non_note','boundary')),
      sample_order INTEGER NOT NULL UNIQUE,
      selected_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS workflow_settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
  const viewpointColumns = new Set(db.prepare('PRAGMA table_info(viewpoints)').all().map((column) => column.name));
  if (!viewpointColumns.has('supporting_evidence_json')) {
    db.exec("ALTER TABLE viewpoints ADD COLUMN supporting_evidence_json TEXT NOT NULL DEFAULT '[]'");
  }
  const securitiesSql = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='securities'").get()?.sql || '';
  if (!securitiesSql.includes("'HK'")) migrateSecuritiesForHongKong(db);
  const linkColumns = new Set(db.prepare('PRAGMA table_info(viewpoint_securities)').all().map((column) => column.name));
  if (!linkColumns.has('id') || !linkColumns.has('link_order')) migrateViewpointSecurityLinks(db);
  db.exec('CREATE INDEX IF NOT EXISTS idx_viewpoint_securities_viewpoint ON viewpoint_securities(viewpoint_id,link_order)');
  const modelRunsSql = db.prepare("SELECT sql FROM sqlite_master WHERE type='table' AND name='model_runs'").get()?.sql || '';
  if (!modelRunsSql.includes("'approval'")) migrateModelStages(db);
}

function migrateModelStages(db) {
  db.exec('PRAGMA foreign_keys=OFF');
  try {
    db.exec(`
      CREATE TABLE model_runs_new (
        id INTEGER PRIMARY KEY, workflow_run_id INTEGER REFERENCES workflow_runs(id),
        stage TEXT NOT NULL CHECK(stage IN ('classification','extraction','approval')),
        provider TEXT NOT NULL, model_id TEXT NOT NULL, prompt_version TEXT NOT NULL, status TEXT NOT NULL,
        started_at TEXT NOT NULL, finished_at TEXT, input_count INTEGER NOT NULL DEFAULT 0,
        success_count INTEGER NOT NULL DEFAULT 0, failed_count INTEGER NOT NULL DEFAULT 0,
        total_duration_ms INTEGER NOT NULL DEFAULT 0, error TEXT
      );
      INSERT INTO model_runs_new SELECT * FROM model_runs;
      DROP TABLE model_runs;
      ALTER TABLE model_runs_new RENAME TO model_runs;

      CREATE TABLE model_attempts_new (
        id INTEGER PRIMARY KEY, model_run_id INTEGER NOT NULL REFERENCES model_runs(id) ON DELETE CASCADE,
        topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id) ON DELETE CASCADE,
        stage TEXT NOT NULL CHECK(stage IN ('classification','extraction','approval')),
        status TEXT NOT NULL CHECK(status IN ('success','failed')), raw_result_json TEXT, error TEXT, created_at TEXT NOT NULL
      );
      INSERT INTO model_attempts_new SELECT * FROM model_attempts;
      DROP TABLE model_attempts;
      ALTER TABLE model_attempts_new RENAME TO model_attempts;
      CREATE INDEX idx_model_attempts_topic ON model_attempts(topic_id,stage,created_at DESC);
    `);
  } finally {
    db.exec('PRAGMA foreign_keys=ON');
  }
}

function migrateSecuritiesForHongKong(db) {
  db.exec('PRAGMA foreign_keys=OFF');
  try {
    db.exec(`
      CREATE TABLE securities_new (
        id INTEGER PRIMARY KEY, ts_code TEXT NOT NULL UNIQUE, symbol TEXT NOT NULL,
        market TEXT NOT NULL CHECK(market IN ('SH','SZ','BJ','HK')), name TEXT NOT NULL,
        full_name TEXT, area TEXT, industry TEXT, exchange TEXT, list_status TEXT,
        list_date TEXT, delist_date TEXT, source TEXT NOT NULL DEFAULT 'tushare', synced_at TEXT NOT NULL
      );
      INSERT INTO securities_new SELECT * FROM securities;
      DROP TABLE securities;
      ALTER TABLE securities_new RENAME TO securities;
      CREATE INDEX idx_securities_symbol ON securities(symbol,market);
      CREATE INDEX idx_securities_name ON securities(name);
    `);
  } finally {
    db.exec('PRAGMA foreign_keys=ON');
  }
}

function migrateViewpointSecurityLinks(db) {
  db.exec('PRAGMA foreign_keys=OFF');
  try {
    db.exec(`
      ALTER TABLE viewpoint_securities RENAME TO viewpoint_securities_legacy;
      CREATE TABLE viewpoint_securities (
        id INTEGER PRIMARY KEY,
        viewpoint_id INTEGER NOT NULL REFERENCES viewpoints(id) ON DELETE CASCADE,
        link_order INTEGER NOT NULL DEFAULT 1,
        security_id INTEGER REFERENCES securities(id),
        surface TEXT NOT NULL, code_hint TEXT, evidence TEXT NOT NULL,
        confidence REAL NOT NULL CHECK(confidence>=0 AND confidence<=1),
        resolution_method TEXT,
        resolution_status TEXT NOT NULL CHECK(resolution_status IN ('resolved','ambiguous','unresolved')),
        resolved_at TEXT,
        UNIQUE(viewpoint_id,link_order)
      );
      INSERT INTO viewpoint_securities(viewpoint_id,link_order,security_id,surface,code_hint,evidence,confidence,resolution_method,resolution_status,resolved_at)
      SELECT viewpoint_id,1,security_id,surface,code_hint,evidence,confidence,resolution_method,resolution_status,resolved_at
      FROM viewpoint_securities_legacy;
      DROP TABLE viewpoint_securities_legacy;
      CREATE INDEX idx_viewpoint_securities_viewpoint ON viewpoint_securities(viewpoint_id,link_order);
      CREATE INDEX idx_viewpoint_security_resolved ON viewpoint_securities(security_id,resolution_status);
    `);
  } finally {
    db.exec('PRAGMA foreign_keys=ON');
  }
}

export function nowIso() {
  return new Date().toISOString();
}

export function latestClassificationSql(alias = 'tc') {
  return `${alias}.id = (SELECT tc2.id FROM topic_classifications tc2 WHERE tc2.topic_id=${alias}.topic_id ORDER BY tc2.created_at DESC,tc2.id DESC LIMIT 1)`;
}
