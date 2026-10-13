import { createHash, randomUUID } from 'node:crypto';
import { ensureApprovalSchema } from './approval_guard.mjs';

export const POLICY_VERSION = 'incremental-adjudication-v1';
export const DEFAULT_LIMITS = { max_calls: 20, max_input_chars: 120000, max_minutes: 30, limit: 100 };
export const DAILY_LIMITS = { max_calls: 40, max_input_chars: 600000 };
export const hash = (value) => createHash('sha256').update(typeof value === 'string' ? value : JSON.stringify(value)).digest('hex');
export const now = () => new Date().toISOString();

export function ensureAutomationSchema(db) {
  ensureApprovalSchema(db);
  db.exec(`
    CREATE TABLE IF NOT EXISTS workflow_batches (
      id INTEGER PRIMARY KEY, status TEXT NOT NULL, stage TEXT NOT NULL DEFAULT 'fetch',
      created_at TEXT NOT NULL, updated_at TEXT NOT NULL, window_start TEXT NOT NULL, window_end TEXT NOT NULL,
      config_json TEXT NOT NULL, config_hash TEXT NOT NULL, selected_count INTEGER NOT NULL DEFAULT 0,
      calls_used INTEGER NOT NULL DEFAULT 0, input_chars_used INTEGER NOT NULL DEFAULT 0,
      max_calls INTEGER NOT NULL, max_input_chars INTEGER NOT NULL, max_minutes INTEGER NOT NULL,
      elapsed_ms INTEGER NOT NULL DEFAULT 0, stop_requested INTEGER NOT NULL DEFAULT 0,
      fetch_cursor TEXT, fetch_pages INTEGER NOT NULL DEFAULT 0, fetch_complete INTEGER NOT NULL DEFAULT 0,
      ingestion_run_id INTEGER REFERENCES ingestion_runs(id), selection_complete INTEGER NOT NULL DEFAULT 0,
      error TEXT, lease_token TEXT, lease_pid INTEGER, heartbeat_at TEXT
    );
    CREATE TABLE IF NOT EXISTS workflow_batch_topics (
      batch_id INTEGER NOT NULL REFERENCES workflow_batches(id), topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id),
      content_hash TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
      primary_json TEXT, review_json TEXT, extraction_result_id INTEGER REFERENCES extraction_results(id),
      reasons_json TEXT NOT NULL DEFAULT '[]', error TEXT, updated_at TEXT NOT NULL,
      PRIMARY KEY(batch_id,topic_id)
    );
    CREATE INDEX IF NOT EXISTS idx_batch_topics_status ON workflow_batch_topics(batch_id,status);
    CREATE TABLE IF NOT EXISTS workflow_model_cache (
      cache_key TEXT PRIMARY KEY, stage TEXT NOT NULL, input_hash TEXT NOT NULL,
      provider TEXT NOT NULL, model_id TEXT NOT NULL, prompt_version TEXT NOT NULL,
      result_json TEXT NOT NULL, actual_model TEXT, usage_json TEXT, created_at TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS model_call_ledger (
      id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL REFERENCES workflow_batches(id), stage TEXT NOT NULL,
      provider TEXT NOT NULL, requested_model TEXT NOT NULL, actual_model TEXT, input_chars INTEGER NOT NULL,
      status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, duration_ms INTEGER,
      usage_json TEXT, raw_result_json TEXT, error_class TEXT, error TEXT
    );
    CREATE TABLE IF NOT EXISTS topic_processing_state (
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id), content_hash TEXT NOT NULL, config_hash TEXT NOT NULL,
      batch_id INTEGER NOT NULL REFERENCES workflow_batches(id), status TEXT NOT NULL, updated_at TEXT NOT NULL,
      PRIMARY KEY(topic_id,content_hash,config_hash)
    );
    CREATE TABLE IF NOT EXISTS raw_topic_versions (
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id), content_hash TEXT NOT NULL,
      title TEXT NOT NULL, content TEXT NOT NULL, raw_json TEXT NOT NULL, saved_at TEXT NOT NULL,
      PRIMARY KEY(topic_id,content_hash)
    );
    CREATE TABLE IF NOT EXISTS workflow_topic_issues (
      id INTEGER PRIMARY KEY, topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id), batch_id INTEGER REFERENCES workflow_batches(id),
      content_hash TEXT NOT NULL, reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
      evidence_json TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, resolved_at TEXT
    );
  `);
  if(!db.prepare('PRAGMA table_info(workflow_batches)').all().some(c=>c.name==='pause_code'))
    db.exec('ALTER TABLE workflow_batches ADD COLUMN pause_code TEXT');
}

const hasTable = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name);
export function dailyBudget(db, at = Date.now()) {
  const day = new Date(Number(at) + 8 * 3600000).toISOString().slice(0, 10);
  const start = new Date(`${day}T00:00:00+08:00`).toISOString();
  const end = new Date(Date.parse(start) + 86400000).toISOString();
  const used = hasTable(db, 'model_call_ledger') ? db.prepare(`SELECT count(*) calls_used,
    coalesce(sum(input_chars),0) input_chars_used FROM model_call_ledger WHERE started_at>=? AND started_at<?`).get(start,end)
    : { calls_used: 0, input_chars_used: 0 };
  return { day, timezone: 'Asia/Shanghai', ...DAILY_LIMITS, ...used,
    calls_remaining: Math.max(0, DAILY_LIMITS.max_calls-used.calls_used),
    input_chars_remaining: Math.max(0, DAILY_LIMITS.max_input_chars-used.input_chars_used), resets_at: end };
}

export function requireReviewCapacity(db, batchId) {
  const batch = db.prepare('SELECT calls_used,max_calls FROM workflow_batches WHERE id=?').get(batchId);
  if(batch.max_calls-batch.calls_used<2 || dailyBudget(db).calls_remaining<2)
    throw Object.assign(new Error('剩余调用预算不足以完成识别和复核，保留待处理内容'), { code:'MODEL_BUDGET_EXCEEDED' });
}
export function getAutomationStatus(db) {
  const coverage = hasTable(db, 'raw_topics') ? db.prepare(`SELECT min(published_at_utc) earliest,max(published_at_utc) latest,
    (SELECT max(finished_at) FROM ingestion_runs WHERE status='success') last_fetch_at FROM raw_topics`).get() : {};
  const batches = hasTable(db, 'workflow_batches') ? db.prepare(`SELECT id,status,stage,created_at,updated_at,window_start,window_end,
    selected_count,calls_used,input_chars_used,max_calls,max_input_chars,max_minutes,elapsed_ms,stop_requested,error,fetch_pages,fetch_complete,
    heartbeat_at,lease_pid FROM workflow_batches ORDER BY id DESC LIMIT 20`).all().map(b => {
      let alive=false; if(b.lease_pid)try{process.kill(b.lease_pid,0);alive=true;}catch(e){alive=e.code==='EPERM';}
      return {...b,worker_alive:alive,resumable:['paused','failed'].includes(b.status)||(b.status==='running'&&!alive),
        remaining_issues:db.prepare("SELECT count(*) n FROM workflow_topic_issues WHERE batch_id=? AND status='pending'").get(b.id).n,
        counts:Object.fromEntries(db.prepare('SELECT status,count(*) n FROM workflow_batch_topics WHERE batch_id=? GROUP BY status').all(b.id).map(r=>[r.status,r.n]))};
    }) : [];
  const pending = hasTable(db, 'topic_processing_state') ? db.prepare(`SELECT count(*) n FROM raw_topics rt
    WHERE eligibility='eligible' AND published_at_utc>=? AND NOT EXISTS(SELECT 1 FROM topic_processing_state p
      WHERE p.topic_id=rt.topic_id AND p.content_hash=rt.content_hash AND p.status IN ('accepted','excluded','uncertain'))`).get(new Date(Date.now() - 168 * 3600000).toISOString()).n : 0;
  const daily=dailyBudget(db);
  return { batches, latest_batch: batches[0] || null, coverage, pending_recent: pending, defaults: DEFAULT_LIMITS, daily_budget: daily,
    cost_note: `上海日期 ${daily.day} 全部批次合计：${daily.calls_used}/${daily.max_calls} 次调用、${daily.input_chars_used}/${daily.max_input_chars} 输入字符。CLI内部网络重试不可逐次计数，也不是美元账单。预算达到后暂停，续跑不清零；尚未启用每日定时触发。` };
}

export function requestBatchStop(db, id) {
  const result = db.prepare("UPDATE workflow_batches SET stop_requested=1,updated_at=? WHERE id=? AND status='running'").run(now(), id);
  return { requested: !!result.changes, batch_id: id };
}

export function acquireBatch(db, id, { isAlive = pid => { try { process.kill(pid, 0); return true; } catch (e) { return e.code === 'EPERM'; } } } = {}) {
  const token = randomUUID();
  db.exec('BEGIN IMMEDIATE');
  try {
    const live = db.prepare("SELECT * FROM workflow_batches WHERE lease_token IS NOT NULL").all();
    for (const b of live) {
      if (b.lease_pid && isAlive(b.lease_pid)) throw new Error(`批次 ${b.id} 仍在运行，不能重复启动`);
      db.prepare("UPDATE workflow_batches SET status='paused',lease_token=NULL,lease_pid=NULL,error='工作进程退出，可续跑' WHERE id=?").run(b.id);
      db.prepare("UPDATE model_call_ledger SET status='interrupted',finished_at=? WHERE batch_id=? AND status='running'").run(now(), b.id);
    }
    db.prepare("UPDATE workflow_batches SET status='running',stop_requested=0,error=NULL,pause_code=NULL,lease_token=?,lease_pid=?,heartbeat_at=?,updated_at=? WHERE id=?")
      .run(token, process.pid, now(), now(), id);
    db.exec('COMMIT');
    return token;
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

export function reserveCall(db, batchId, { stage, provider, model, chars, elapsedMs = 0 }) {
  db.exec('BEGIN IMMEDIATE');
  try {
    const b = db.prepare('SELECT * FROM workflow_batches WHERE id=?').get(batchId);
    const stamp=now();
    const daily = dailyBudget(db,Date.parse(stamp));
    let message = null;
    if (b.stop_requested) message = '用户请求暂停';
    else if (daily.calls_remaining < 1) message = '已达到上海自然日跨批次模型调用上限';
    else if (chars > daily.input_chars_remaining) message = '已达到上海自然日跨批次模型输入字符上限';
    else if (b.calls_used >= b.max_calls) message = '已达到本批次模型调用上限';
    else if (b.input_chars_used + chars > b.max_input_chars) message = '已达到本批次模型输入字符上限';
    else if (b.elapsed_ms + elapsedMs >= b.max_minutes * 60000) message = '已达到本批次累计运行时长上限';
    if (message) { const e = new Error(message); e.code = 'MODEL_BUDGET_EXCEEDED'; throw e; }
    db.prepare('UPDATE workflow_batches SET calls_used=calls_used+1,input_chars_used=input_chars_used+?,updated_at=? WHERE id=?').run(chars, stamp, batchId);
    const id = Number(db.prepare(`INSERT INTO model_call_ledger(batch_id,stage,provider,requested_model,input_chars,status,started_at)
      VALUES(?,?,?,?,?,'running',?)`).run(batchId, stage, provider, model, chars, stamp).lastInsertRowid);
    db.exec('COMMIT');
    return id;
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

export function finishCall(db, id, event) {
  db.prepare(`UPDATE model_call_ledger SET status=?,finished_at=?,duration_ms=?,actual_model=?,usage_json=?,raw_result_json=?,error_class=?,error=? WHERE id=?`)
    .run(event.status, now(), event.durationMs || 0, event.actualModel || null, event.usage ? JSON.stringify(event.usage) : null,
      event.rawResult ? JSON.stringify(event.rawResult) : null,event.errorClass || null, event.error?.message || (typeof event.error === 'string' ? event.error : null), id);
}

export function finishTopic(db, batch, topic, status, reasons = []) {
  db.prepare('UPDATE workflow_batch_topics SET status=?,reasons_json=?,error=NULL,updated_at=? WHERE batch_id=? AND topic_id=?')
    .run(status, JSON.stringify(reasons), now(), batch.id, topic.topic_id);
  db.prepare(`INSERT INTO topic_processing_state(topic_id,content_hash,config_hash,batch_id,status,updated_at) VALUES(?,?,?,?,?,?)
    ON CONFLICT(topic_id,content_hash,config_hash) DO UPDATE SET batch_id=excluded.batch_id,status=excluded.status,updated_at=excluded.updated_at`)
    .run(topic.topic_id, topic.content_hash, batch.config_hash, batch.id, status, now());
}
