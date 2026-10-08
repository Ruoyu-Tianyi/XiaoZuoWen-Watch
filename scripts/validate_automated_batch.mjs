import { DatabaseSync } from 'node:sqlite';
import { createHash } from 'node:crypto';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { validatePublishBatch } from './lib/approval_guard.mjs';

const TERMINAL_BATCH = new Set(['completed', 'completed_with_issues']);
const TERMINAL_TOPIC = new Set(['accepted', 'excluded', 'uncertain']);
const brief = value => value == null ? null : String(value).slice(0, 400);
const tally = (rows, field = 'status') => rows.reduce((out, row) => {
  out[row[field]] = (out[row[field]] || 0) + 1;
  return out;
}, {});

export function parseBatchArgs(args) {
  if (args.length !== 1 || !/^--batch=[1-9]\d*$/.test(args[0])) {
    throw new Error('用法：node scripts/validate_automated_batch.mjs --batch=正整数');
  }
  const id = Number(args[0].slice('--batch='.length));
  if (!Number.isSafeInteger(id)) throw new Error('batch ID 必须是安全范围内的正整数');
  return id;
}

// Sum only reported numeric token fields, preserving their paths and coverage.
// Missing usage stays unavailable; overlapping total/input/cached fields are never added together.
function tokenFields(value, prefix = '', output = {}) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return output;
  for (const [key, item] of Object.entries(value)) {
    const path = prefix ? `${prefix}.${key}` : key;
    if (typeof item === 'number' && Number.isFinite(item) && item >= 0 && /token/i.test(path)) output[path] = item;
    else if (item && typeof item === 'object') tokenFields(item, path, output);
  }
  return output;
}

export function inspectAutomatedBatch(dbPath, batchId) {
  if (!Number.isSafeInteger(batchId) || batchId <= 0) throw new Error('batch ID 必须是正整数');
  const db = new DatabaseSync(resolve(dbPath), { readOnly: true });
  try {
    db.exec('PRAGMA query_only=ON; BEGIN');
    const batch = db.prepare('SELECT * FROM workflow_batches WHERE id=?').get(batchId);
    if (!batch) throw new Error(`找不到批次 ${batchId}`);
    const anomalies = [];
    const add = (code, detail) => anomalies.push({ code, ...detail });
    const parse = (text, label, fallback = null) => {
      if (text == null) return fallback;
      try { return JSON.parse(text); }
      catch { add('invalid_json', { field: label }); return fallback; }
    };
    const config = parse(batch.config_json, 'batch.config_json', {});
    const groupId = config?.group_id == null ? null : String(config.group_id);
    if (!groupId) add('missing_group_id', {});
    const topics = db.prepare(`SELECT bt.*, rt.topic_id raw_id, rt.group_id, rt.content_hash current_hash,
      rt.title, rt.content, er.topic_id extraction_topic_id, er.content_hash extraction_hash
      FROM workflow_batch_topics bt LEFT JOIN raw_topics rt ON rt.topic_id=bt.topic_id
      LEFT JOIN extraction_results er ON er.id=bt.extraction_result_id
      WHERE bt.batch_id=? ORDER BY bt.topic_id`).all(batchId);
    for (const topic of topics) {
      const id = { topic_id: topic.topic_id };
      if (!topic.raw_id) add('missing_raw_topic', id);
      else {
        const hash = createHash('sha256').update(`${topic.title}\n${topic.content}`).digest('hex');
        if (topic.current_hash !== topic.content_hash || hash !== topic.content_hash) add('changed_original', id);
        if (groupId && topic.group_id !== groupId) add('wrong_group', id);
      }
      if (!TERMINAL_TOPIC.has(topic.status) && !['queued', 'extracted', 'failed'].includes(topic.status)) add('unknown_topic_status', { ...id, status: topic.status });
      for (const field of ['primary_json', 'review_json', 'reasons_json']) parse(topic[field], `${topic.topic_id}.${field}`);
      if (['accepted', 'excluded'].includes(topic.status) && (!topic.primary_json || !topic.review_json || !topic.extraction_result_id)) add('incomplete_terminal_topic', id);
      if (topic.extraction_result_id && (topic.extraction_topic_id !== topic.topic_id || topic.extraction_hash !== topic.content_hash)) add('extraction_mismatch', id);
    }
    if (batch.selected_count !== topics.length) add('selected_count_mismatch', { recorded: batch.selected_count, actual: topics.length });
    if (TERMINAL_BATCH.has(batch.status) && (!batch.fetch_complete || !batch.selection_complete)) add('incomplete_terminal_batch', {});
    const windowCounts = db.prepare(`SELECT count(*) raw,
      coalesce(sum(eligibility='eligible'),0) eligible, coalesce(sum(eligibility='excluded'),0) excluded
      FROM raw_topics WHERE group_id=? AND published_at_utc>=? AND published_at_utc<=?`)
      .get(groupId || '', batch.window_start, batch.window_end);
    const views = db.prepare(`SELECT v.id,v.topic_id,v.extraction_result_id,v.direction,v.review_status,
      v.recommendation_broker broker,v.recommendation_team team,v.target_price,v.target_currency,
      bt.topic_id batch_topic_id FROM viewpoints v JOIN workflow_batch_topics bt
      ON bt.extraction_result_id=v.extraction_result_id WHERE bt.batch_id=? ORDER BY v.id`).all(batchId);
    const links = db.prepare(`SELECT vs.viewpoint_id,vs.surface,vs.resolution_status,s.name,s.ts_code,s.market
      FROM viewpoint_securities vs LEFT JOIN securities s ON s.id=vs.security_id
      JOIN viewpoints v ON v.id=vs.viewpoint_id JOIN workflow_batch_topics bt
      ON bt.extraction_result_id=v.extraction_result_id WHERE bt.batch_id=? ORDER BY v.id,vs.link_order`).all(batchId);
    for (const view of views) if (view.topic_id !== view.batch_topic_id) add('viewpoint_topic_mismatch', { viewpoint_id: view.id });
    const reviewedIds = views.filter(v => v.review_status === 'reviewed').map(v => v.id);
    // An explicit ID array is essential: never validate unrelated batches or publish.
    const gate = validatePublishBatch(db, reviewedIds);
    const ledger = db.prepare(`SELECT id,stage,provider,requested_model,actual_model,status,duration_ms,
      usage_json,error_class,error FROM model_call_ledger WHERE batch_id=? ORDER BY id`).all(batchId);
    if (batch.calls_used !== ledger.length) add('call_count_mismatch', { recorded: batch.calls_used, actual: ledger.length });
    const tokenTotals = {};
    const calls = ledger.map(call => {
      const usage = parse(call.usage_json, `call.${call.id}.usage_json`);
      const tokens = tokenFields(usage);
      for (const [field, count] of Object.entries(tokens)) {
        tokenTotals[field] ||= { total: 0, calls_reporting: 0 };
        tokenTotals[field].total += count;
        tokenTotals[field].calls_reporting++;
      }
      const { usage_json, error, ...rest } = call;
      return { ...rest, reported_tokens: Object.keys(tokens).length ? tokens : null, error: brief(error) };
    });
    const unfinished = topics.filter(topic => !TERMINAL_TOPIC.has(topic.status)).map(topic => ({ topic_id: topic.topic_id, status: topic.status, error: brief(topic.error) }));
    const issues = db.prepare(`SELECT id,topic_id,status,reason FROM workflow_topic_issues
      WHERE batch_id=? ORDER BY id`).all(batchId).map(issue => ({ ...issue, reason: brief(issue.reason) }));
    const foreignKeys = db.prepare('PRAGMA foreign_key_check').all();
    const integrity = db.prepare('PRAGMA integrity_check').all().map(row => Object.values(row)[0]);
    const databaseOk = foreignKeys.length === 0 && integrity.length === 1 && integrity[0] === 'ok';
    const terminal = TERMINAL_BATCH.has(batch.status);
    const runningCalls = calls.filter(call => call.status === 'running');
    const ok = terminal && unfinished.length === 0 && runningCalls.length === 0 && anomalies.length === 0 && databaseOk && gate.ok;
    const result = {
      ok, read_only: true, batch: { id: batch.id, status: batch.status, stage: batch.stage, terminal,
        window_start: batch.window_start, window_end: batch.window_end, group_id: groupId,
        selected_count: batch.selected_count, fetch_complete: !!batch.fetch_complete, selection_complete: !!batch.selection_complete,
        elapsed_ms: batch.elapsed_ms, error: brief(batch.error) },
      window_counts: windowCounts,
      topic_status_counts: tally(topics),
      topics: topics.map(topic => ({ topic_id: topic.topic_id, status: topic.status, extraction_result_id: topic.extraction_result_id })),
      viewpoint_status_counts: tally(views, 'review_status'),
      viewpoints: views.map(({ batch_topic_id, ...view }) => ({ ...view, securities: links.filter(link => link.viewpoint_id === view.id).map(({ viewpoint_id, ...link }) => link) })),
      model_calls: { recorded_count: batch.calls_used, ledger_count: calls.length, status_counts: tally(calls),
        duration_ms: calls.reduce((sum, call) => sum + (call.duration_ms || 0), 0),
        input_chars: batch.input_chars_used, reported_token_totals: tokenTotals, calls },
      unfinished, running_calls: runningCalls.map(call => call.id), structural_anomalies: anomalies,
      topic_issues: issues, historical_call_errors: calls.filter(call => call.status !== 'success' && call.status !== 'running').map(call => ({ id: call.id, status: call.status, error_class: call.error_class, error: call.error })),
      reviewed_viewpoint_check: { ok: gate.ok, checked_ids: gate.ids, issues: gate.issues, published: false },
      database_checks: { ok: databaseOk, scope: 'entire_database', foreign_key_check: foreignKeys, integrity_check: integrity },
      limitations: ['仅核验状态、数据关系和现有发布门禁；不代表语义准确率。',
        '存疑主题与候选观点单独报告，不自动视为机器失败；历史失败调用保留记录。',
        '窗口统计为该星球当前原文记录，可能多于本批选择；调用数不是底层网络重试次数或费用。',
        'Token仅累加服务实际提供的字段，各字段可能重叠，缺失不视为零。'],
    };
    db.exec('COMMIT');
    return result;
  } finally {
    if (db.isTransaction) db.exec('ROLLBACK');
    db.close();
  }
}

export function runCli(args = process.argv.slice(2), environment = process.env) {
  try {
    const id = parseBatchArgs(args);
    const report = inspectAutomatedBatch(environment.ZSXQ_DB || 'data/broker_notes.sqlite', id);
    console.log(JSON.stringify(report, null, 2));
    return report.ok ? 0 : 1;
  } catch (error) {
    console.log(JSON.stringify({ ok: false, read_only: true, error: error.message }, null, 2));
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) process.exitCode = runCli();
