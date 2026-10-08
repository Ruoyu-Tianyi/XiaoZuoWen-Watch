import http from 'node:http';
import { spawn } from 'node:child_process';
import { readFile, stat } from 'node:fs/promises';
import { resolve, extname, normalize } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { ensureWorkflowSchema } from '../scripts/lib/workflow_db.mjs';
import { ensureApprovalSchema, currentApproval, invalidateApproval, recordApproval, publishAccepted, validatePublishBatch, viewpointFingerprint } from '../scripts/lib/approval_guard.mjs';
import { ensureAutomationSchema, getAutomationStatus, requestBatchStop } from '../scripts/lib/automation_state.mjs';

const HOST = process.env.DASHBOARD_HOST || '127.0.0.1';
const PORT = Number(process.env.DASHBOARD_PORT || '4173');
const ROOT = resolve('app/public');
const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const VALID_WINDOWS = new Set([24, 72, 168]);
const MIME = { '.html': 'text/html; charset=utf-8', '.css': 'text/css; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.json': 'application/json; charset=utf-8', '.png': 'image/png' };
const WORKFLOW_ACTIONS = new Set(['fetch', 'sync-securities', 'pilot', 'classify-pilot', 'extract-pilot', 'approve-pilot', 'validate', 'all']);

let workflowState = { status: 'idle', action: null, started_at: null, finished_at: null, output: '', error: null };

function sendJson(response, status, payload) {
  response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(payload));
}

async function readJson(request) {
  let body = '';
  for await (const chunk of request) {
    body += chunk.toString();
    if (body.length > 2 * 1024 * 1024) throw new Error('请求正文过大');
  }
  return body ? JSON.parse(body) : {};
}

function parseWindow(url) {
  const value = Number(url.searchParams.get('window') || '24');
  return VALID_WINDOWS.has(value) ? value : 24;
}

function cutoffIso(hours) {
  return new Date(Date.now() - hours * 60 * 60 * 1000).toISOString();
}

function withDb(callback, { writable = false } = {}) {
  const db = new DatabaseSync(DB_PATH, writable ? {} : { readOnly: true });
  try {
    if (writable) { db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;'); ensureWorkflowSchema(db); ensureApprovalSchema(db); }
    return callback(db);
  } finally { db.close(); }
}

function getDashboard(db, url) {
  const hours = parseWindow(url);
  const cutoff = cutoffIso(hours);
  const q = (url.searchParams.get('q') || '').trim();
  const direction = url.searchParams.get('direction');
  const broker = url.searchParams.get('broker');
  const conditions = ["v.review_status='published'", 'rt.published_at_utc>=?', "vs.resolution_status='resolved'"];
  const params = [cutoff];
  if (['bullish', 'bearish'].includes(direction)) { conditions.push('v.direction=?'); params.push(direction); }
  if (broker) { conditions.push('v.recommendation_broker=?'); params.push(broker); }
  if (q) { conditions.push('(s.name LIKE ? OR s.symbol LIKE ? OR s.ts_code LIKE ?)'); params.push(`%${q}%`, `%${q}%`, `%${q}%`); }
  const where = conditions.join(' AND ');
  const core = db.prepare(`SELECT count(DISTINCT v.topic_id) posts,count(DISTINCT v.id) viewpoints,count(DISTINCT s.id) securities,
      count(DISTINCT CASE WHEN v.direction='bullish' THEN v.id END) bullish,count(DISTINCT CASE WHEN v.direction='bearish' THEN v.id END) bearish,
      count(DISTINCT CASE WHEN v.target_price IS NOT NULL THEN v.id END) targets,
      count(DISTINCT CASE WHEN trim(coalesce(v.recommendation_broker,''))<>'' THEN v.topic_id END) sourced_posts,
      max(rt.published_at_utc) latest_post
    FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN securities s ON s.id=vs.security_id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE ${where}`).get(...params);
  const coverage = db.prepare("SELECT min(published_at_utc) earliest,max(published_at_utc) latest FROM raw_topics WHERE eligibility='eligible'").get();
  const latestIngestion = db.prepare('SELECT started_at,finished_at,cutoff_at,topic_count,eligible_count,excluded_count,status FROM ingestion_runs ORDER BY id DESC LIMIT 1').get() || {};
  const hotness = db.prepare(`SELECT s.name,s.symbol code,s.market,s.ts_code,count(DISTINCT v.topic_id) mentions,
      sum(v.direction='bullish') bullish,sum(v.direction='bearish') bearish,0 neutral,round(avg(v.confidence),2) entity_confidence,
      max(rt.published_at_utc) latest_mention,group_concat(DISTINCT nullif(v.recommendation_broker,'')) brokers
    FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN securities s ON s.id=vs.security_id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE ${where} GROUP BY s.id ORDER BY mentions DESC,bullish DESC,latest_mention DESC LIMIT 50`).all(...params);
  const bucket = hours === 24 ? "strftime('%m-%d %H:00',rt.published_at_utc,'+8 hours')"
    : hours === 72 ? "strftime('%m-%d ',rt.published_at_utc,'+8 hours')||printf('%02d:00',(cast(strftime('%H',rt.published_at_utc,'+8 hours') as integer)/6)*6)"
      : "strftime('%m-%d',rt.published_at_utc,'+8 hours')";
  const trendRows = db.prepare(`SELECT ${bucket} bucket,count(DISTINCT v.id) mentions,
    count(DISTINCT CASE WHEN v.direction='bullish' THEN v.id END) bullish,count(DISTINCT CASE WHEN v.direction='bearish' THEN v.id END) bearish,0 neutral
    FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN securities s ON s.id=vs.security_id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE ${where} GROUP BY bucket ORDER BY min(rt.published_at_utc)`).all(...params);
  const byBucket = new Map(trendRows.map((row) => [row.bucket, row]));
  const intervalHours = hours === 24 ? 1 : hours === 72 ? 6 : 24;
  const intervalMs = intervalHours * 3600000;
  const chinaOffset = 8 * 3600000;
  const first = Math.floor((new Date(cutoff).getTime() + chinaOffset) / intervalMs) * intervalMs;
  const end = Date.now() + chinaOffset;
  const pad = (value) => String(value).padStart(2, '0');
  const trend = [];
  for (let cursor = first; cursor <= end; cursor += intervalMs) {
    const date = new Date(cursor);
    const day = `${pad(date.getUTCMonth() + 1)}-${pad(date.getUTCDate())}`;
    const key = hours === 168 ? day : `${day} ${pad(date.getUTCHours())}:00`;
    trend.push(byBucket.get(key) || { bucket: key, mentions: 0, bullish: 0, bearish: 0, neutral: 0 });
  }
  const sources = db.prepare(`SELECT recommendation_broker broker,count(DISTINCT topic_id) posts FROM viewpoints
    WHERE review_status='published' AND trim(coalesce(recommendation_broker,''))<>'' GROUP BY recommendation_broker ORDER BY posts DESC`).all();
  const posts = Number(core.posts || 0);
  return {
    summary: { window_hours: hours, posts, mentions: Number(core.viewpoints || 0), securities: Number(core.securities || 0),
      bullish: Number(core.bullish || 0), bearish: Number(core.bearish || 0), neutral: 0, targets: Number(core.targets || 0),
      sourced_posts: Number(core.sourced_posts || 0), source_coverage: posts ? Number(core.sourced_posts || 0) / posts : 0,
      latest_post: core.latest_post, coverage, last_ingestion: latestIngestion, pipeline: { current_version: 'broker-note-v2' } },
    hotness, trend, sources, quality: getQualitySummary(db), refresh: workflowState,
  };
}

function getQualitySummary(db) {
  const total = Number(db.prepare("SELECT count(*) n FROM viewpoints WHERE review_status='published'").get().n);
  const resolved = Number(db.prepare("SELECT count(DISTINCT v.id) n FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id WHERE v.review_status='published' AND vs.resolution_status='resolved'").get().n);
  const sourced = Number(db.prepare("SELECT count(*) n FROM viewpoints WHERE review_status='published' AND trim(coalesce(recommendation_broker,''))<>''").get().n);
  return { status: total && resolved === total ? 'pass' : total ? 'warning' : 'unknown', checked_at: new Date().toISOString(),
    population: '正式发布观点', total, resolved, sourced, accuracy: null,
    rates: { mention_coverage: total ? resolved / total : null, source_coverage: total ? sourced / total : null, low_confidence_rate: null } };
}

function getSecurity(db, url) {
  const code = url.searchParams.get('code');
  const market = url.searchParams.get('market');
  if (!code || !market) return null;
  const security = db.prepare('SELECT id,symbol code,market,ts_code,name FROM securities WHERE symbol=? AND market=?').get(code, market);
  if (!security) return null;
  const all = url.searchParams.get('scope') === 'all';
  const conditions = ["v.review_status='published'", "vs.resolution_status='resolved'", 'vs.security_id=?'];
  const params = [security.id];
  if (!all) { conditions.push('rt.published_at_utc>=?'); params.push(cutoffIso(parseWindow(url))); }
  const mentions = db.prepare(`SELECT rt.topic_id,rt.published_at,v.summary viewpoint_summary,v.evidence,v.direction,v.target_price,v.target_currency,
      v.target_price_raw,v.recommendation_broker source_broker,v.recommendation_team source_team,v.confidence
    FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE ${conditions.join(' AND ')} ORDER BY rt.published_at_utc DESC,v.ordinal LIMIT 1000`).all(...params);
  return { ...security, scope: all ? 'all' : 'window', total_mentions: new Set(mentions.map((row) => row.topic_id)).size, total_viewpoints: mentions.length, mentions, viewpoint_mentions: mentions };
}

function searchHistory(db, url) {
  const query = (url.searchParams.get('q') || '').trim();
  if (!query) return [];
  const pattern = `%${query}%`;
  return db.prepare(`SELECT s.name,s.symbol code,s.market,s.ts_code,count(DISTINCT v.topic_id) mentions,
      sum(v.direction='bullish') bullish,sum(v.direction='bearish') bearish,0 neutral,min(rt.published_at_utc) first_mention,max(rt.published_at_utc) latest_mention,
      count(DISTINCT nullif(v.recommendation_broker,'')) sources
    FROM securities s JOIN viewpoint_securities vs ON vs.security_id=s.id JOIN viewpoints v ON v.id=vs.viewpoint_id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE v.review_status='published' AND vs.resolution_status='resolved' AND (s.name LIKE ? OR s.symbol LIKE ? OR s.ts_code LIKE ?)
    GROUP BY s.id ORDER BY CASE WHEN s.symbol=? OR s.ts_code=? OR s.name=? THEN 0 ELSE 1 END,mentions DESC LIMIT 50`)
    .all(pattern, pattern, pattern, query, query, query);
}

function getTopic(db, id, scope = 'published') {
  const topic = db.prepare(`SELECT topic_id,published_at,title,content,poster_name,readers,likes,comments,eligibility,exclusion_reason FROM raw_topics WHERE topic_id=?`).get(id);
  if (!topic) return null;
  const viewpoints = db.prepare(`SELECT v.id,v.ordinal,v.summary,v.evidence,v.direction,v.target_price,v.target_currency,v.target_price_raw,
      v.recommendation_broker source_broker,v.recommendation_team source_team,v.confidence,v.review_status
    FROM viewpoints v WHERE v.topic_id=? AND (?='management' OR v.review_status='published') ORDER BY v.ordinal`).all(id, scope);
  const linkQuery = db.prepare(`SELECT vs.surface,vs.resolution_status,s.name,s.symbol code,s.market,s.ts_code
    FROM viewpoint_securities vs LEFT JOIN securities s ON s.id=vs.security_id WHERE vs.viewpoint_id=? ORDER BY vs.link_order`);
  for (const viewpoint of viewpoints) viewpoint.entities = linkQuery.all(viewpoint.id);
  return { ...topic, source_broker: viewpoints[0]?.source_broker || null, source_team: viewpoints[0]?.source_team || '其他',
    model_viewpoints: viewpoints.map((row) => ({ ...row, viewpoint_type: row.direction })), mentions: viewpoints };
}

function getWorkflowStatus(db) {
  const count = (sql) => Number(db.prepare(sql).get().n || 0);
  return {
    runtime: workflowState,
    counts: {
      raw: count('SELECT count(*) n FROM raw_topics'), eligible: count("SELECT count(*) n FROM raw_topics WHERE eligibility='eligible'"),
      excluded: count("SELECT count(*) n FROM raw_topics WHERE eligibility='excluded'"), samples: count('SELECT count(*) n FROM pilot_samples'),
      classified: count('SELECT count(DISTINCT topic_id) n FROM topic_classifications'), candidate: count("SELECT count(*) n FROM viewpoints WHERE review_status='candidate'"),
      reviewed: count("SELECT count(*) n FROM viewpoints WHERE review_status='reviewed'"), published: count("SELECT count(*) n FROM viewpoints WHERE review_status='published'"),
      rejected: count(`SELECT count(*) n FROM viewpoints v WHERE v.review_status='rejected'
        AND (SELECT re.action FROM review_events re WHERE re.viewpoint_id=v.id ORDER BY re.id DESC LIMIT 1)='reject'`),
      unresolved: count("SELECT count(*) n FROM security_resolution_queue q JOIN viewpoints v ON v.id=q.viewpoint_id WHERE q.status='pending' AND v.review_status<>'rejected'"),
      ai_auto: count("SELECT count(*) n FROM ai_approval_results WHERE status='success' AND decision='auto_accept'"),
      ai_human: count("SELECT count(*) n FROM ai_approval_results WHERE status='success' AND decision='human_review'"),
      ai_auto_viewpoints: count("SELECT count(*) n FROM ai_viewpoint_approval_results WHERE decision='auto_accept'"),
      ai_human_viewpoints: count("SELECT count(*) n FROM ai_viewpoint_approval_results WHERE decision='human_review'"),
      securities: count('SELECT count(*) n FROM securities'),
    },
    latest_ingestion: db.prepare('SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1').get() || null,
    runs: db.prepare('SELECT * FROM workflow_runs ORDER BY id DESC LIMIT 12').all(),
    model_runs: db.prepare('SELECT * FROM model_runs ORDER BY id DESC LIMIT 8').all(),
  };
}

function getReviewTopics(db, url) {
  const status = url.searchParams.get('status') || 'candidate';
  if (!['raw', 'candidate', 'reviewed', 'published', 'rejected', 'all'].includes(status)) throw new Error('无效的审核状态');
  const limit = Math.min(100, Math.max(1, Math.floor(Number(url.searchParams.get('page_size') || url.searchParams.get('limit') || 25)) || 25));
  let page = Math.max(1, Math.floor(Number(url.searchParams.get('page') || 1)) || 1);
  const hasIssues = Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE name='workflow_topic_issues'").get());
  const pendingIssue = hasIssues ? "EXISTS(SELECT 1 FROM workflow_topic_issues wi WHERE wi.topic_id=rt.topic_id AND wi.status='pending' AND wi.content_hash=rt.content_hash)" : '0';
  const noCurrentIssueRecord = hasIssues ? "NOT EXISTS(SELECT 1 FROM workflow_topic_issues wi WHERE wi.topic_id=rt.topic_id AND wi.content_hash=rt.content_hash)" : '1';
  const joins = `FROM raw_topics rt LEFT JOIN pilot_samples ps ON ps.topic_id=rt.topic_id
    LEFT JOIN topic_classifications tc ON tc.id=(SELECT tc2.id FROM topic_classifications tc2 WHERE tc2.topic_id=rt.topic_id ORDER BY tc2.created_at DESC,tc2.id DESC LIMIT 1)
    LEFT JOIN ai_approval_results ar ON ar.id=(SELECT ar2.id FROM ai_approval_results ar2 WHERE ar2.topic_id=rt.topic_id ORDER BY ar2.created_at DESC,ar2.id DESC LIMIT 1)`;
  const where = `WHERE ? IN ('all','raw') OR EXISTS(SELECT 1 FROM viewpoints v WHERE v.topic_id=rt.topic_id AND v.review_status=?
      AND (?<>'rejected' OR (SELECT re.action FROM review_events re WHERE re.viewpoint_id=v.id ORDER BY re.id DESC LIMIT 1)='reject'
        OR EXISTS(SELECT 1 FROM viewpoint_approval_audit aa WHERE aa.viewpoint_id=v.id AND aa.decision='exclude')))
       OR (?='candidate' AND (${pendingIssue} OR (${noCurrentIssueRecord} AND tc.id IS NOT NULL AND tc.review_status='candidate'
         AND NOT EXISTS(SELECT 1 FROM viewpoints vx WHERE vx.topic_id=rt.topic_id AND vx.review_status='candidate')
         AND (ar.status='failed' OR ar.decision='human_review'))))`;
  // Read endpoints must work before the new approval schema has been created.
  const approvalTable = Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE name='viewpoint_approval_audit'").get());
  const safeWhere = approvalTable ? where : where.replace("OR EXISTS(SELECT 1 FROM viewpoint_approval_audit aa WHERE aa.viewpoint_id=v.id AND aa.decision='exclude')", '');
  const total = Number(db.prepare(`SELECT count(*) n ${joins} ${safeWhere}`).get(status, status, status, status).n);
  const totalPages = Math.max(1, Math.ceil(total / limit)); page = Math.min(page, totalPages);
  const rows = db.prepare(`SELECT rt.topic_id,rt.published_at,rt.title,rt.content,rt.eligibility,rt.exclusion_reason,ps.stratum,ps.sample_order,
      tc.id classification_id,tc.classification,tc.confidence classification_confidence,tc.evidence classification_evidence,tc.review_status classification_review_status,
      ar.status ai_approval_status,ar.decision ai_approval_decision,ar.confidence ai_approval_confidence,
      ar.reasons_json ai_approval_reasons_json,ar.error ai_approval_error,ar.model_id ai_approval_model
    ${joins} ${safeWhere}
    ORDER BY rt.published_at_utc DESC,rt.topic_id DESC LIMIT ? OFFSET ?`).all(status, status, status, status, limit, (page - 1) * limit);
  const excludedClause = `(SELECT re.action FROM review_events re WHERE re.viewpoint_id=v.id ORDER BY re.id DESC LIMIT 1)='reject'${approvalTable ? " OR (SELECT aa.decision FROM viewpoint_approval_audit aa WHERE aa.viewpoint_id=v.id ORDER BY aa.id DESC LIMIT 1)='exclude'" : ''}`;
  const viewpointQuery = db.prepare(`SELECT v.*,avr.decision ai_viewpoint_decision,avr.confidence ai_viewpoint_confidence,
      avr.reasons_json ai_viewpoint_reasons_json FROM viewpoints v
    LEFT JOIN ai_viewpoint_approval_results avr ON avr.id=(SELECT avr2.id FROM ai_viewpoint_approval_results avr2
      WHERE avr2.viewpoint_id=v.id ORDER BY avr2.created_at DESC,avr2.id DESC LIMIT 1)
    WHERE v.topic_id=? AND (
      (?='all' AND (v.review_status<>'rejected' OR ${excludedClause}))
      OR (?<>'all' AND v.review_status=?
        AND (?<>'rejected' OR ${excludedClause}))
    ) ORDER BY v.ordinal`);
  const linkQuery = db.prepare(`SELECT vs.id link_id,vs.link_order,vs.surface,vs.code_hint,vs.resolution_status,vs.resolution_method,
    s.id security_id,s.ts_code,s.symbol,s.market,s.name FROM viewpoint_securities vs LEFT JOIN securities s ON s.id=vs.security_id
    WHERE vs.viewpoint_id=? ORDER BY vs.link_order`);
  const items = rows.map((row) => ({ ...row,
    issues: hasIssues ? db.prepare("SELECT wi.id,wi.reason,wi.evidence_json FROM workflow_topic_issues wi JOIN raw_topics rt ON rt.topic_id=wi.topic_id WHERE wi.topic_id=? AND wi.content_hash=rt.content_hash AND wi.status='pending' ORDER BY wi.id").all(row.topic_id) : [],
    viewpoints: status === 'raw' ? [] : viewpointQuery.all(row.topic_id, status, status, status, status).map((viewpoint) => {
    const security_links = linkQuery.all(viewpoint.id);
    const approval = currentApproval(db, viewpoint.id);
    const fresh = approval && approval.fingerprint === viewpointFingerprint(db, viewpoint.id).fingerprint;
    return { ...viewpoint, security_links, ...(security_links[0] || {}), approval,
      approval_valid: Boolean(fresh && approval.decision === 'accept'),
      ai_viewpoint_decision: approval ? (fresh && approval.decision === 'accept' ? 'auto_accept' : 'human_review') : viewpoint.ai_viewpoint_decision };
  }) }));
  return { items, total, page, page_size: limit, total_pages: totalPages };
}

function startAutomation(body) {
  if (workflowState.status === 'running') return false;
  if (body.resume != null && ['limit', 'max_calls', 'max_input_chars', 'max_minutes', 'skip_fetch'].some((field) => Object.hasOwn(body, field))) {
    throw new Error('界面续跑只接受批次ID，沿用原批次范围及累计预算；不会用新批次表单提高额度');
  }
  const args = [resolve('scripts/automated_workflow.mjs'), 'run'];
  for (const [field, flag, max] of [['limit', 'limit', 10000], ['max_calls', 'max-calls', 1000], ['max_input_chars', 'max-chars', 10000000], ['max_minutes', 'max-minutes', 1440], ['resume', 'resume', Number.MAX_SAFE_INTEGER]]) {
    if (body[field] == null || body[field] === '') continue;
    const value = Number(body[field]);
    if (!Number.isSafeInteger(value) || value < 1 || value > max) throw new Error(`${field} 超出允许范围`);
    args.push(`--${flag}=${value}`);
  }
  if (body.skip_fetch === true) args.push('--skip-fetch');
  spawnWorkflow(args, body.resume ? `续跑批次 ${body.resume}` : '增量入库');
  return true;
}

function spawnWorkflow(args, action) {
  workflowState = { status: 'running', action, started_at: new Date().toISOString(), finished_at: null, output: '', error: null };
  const child = spawn(process.execPath, args, { cwd: process.cwd(), env: process.env, windowsHide: true });
  const append = (chunk) => { workflowState.output = (workflowState.output + chunk.toString()).slice(-30000); };
  child.stdout.on('data', append); child.stderr.on('data', append);
  child.on('error', (error) => { workflowState.status = 'failed'; workflowState.error = error.message; workflowState.finished_at = new Date().toISOString(); });
  child.on('close', (code) => {
    let batch;
    if (args[0].endsWith('automated_workflow.mjs')) { try { batch = withDb((db) => getAutomationStatus(db).latest_batch); } catch {} }
    workflowState.status = code === 2 && batch?.status === 'paused' ? 'paused' : code === 0 ? 'success' : 'failed';
    workflowState.error = workflowState.status === 'failed' ? `进程退出码 ${code}` : null;
    workflowState.finished_at = new Date().toISOString();
  });
}

function resolveTopicIssues(db, topicId, body) {
  const note = String(body.note || '').trim();
  if (note.length < 2 || note.length > 4000) throw new Error('请填写核对说明（2至4000字）');
  const topic = db.prepare('SELECT topic_id,content_hash FROM raw_topics WHERE topic_id=?').get(topicId);
  if (!topic) throw new Error('原文不存在');
  ensureAutomationSchema(db);
  db.exec('BEGIN IMMEDIATE');
  try {
    if (db.prepare("SELECT 1 FROM viewpoints WHERE topic_id=? AND review_status='candidate'").get(topicId)) throw new Error('本篇仍有待复核观点，请先逐条处理');
    const active = db.prepare(`SELECT status FROM workflow_batch_topics WHERE topic_id=? AND content_hash=? ORDER BY batch_id DESC LIMIT 1`).get(topicId, topic.content_hash);
    if (active && ['failed', 'extracted', 'queued'].includes(active.status)) throw new Error('本篇仍有失败或未完成处理，请先续跑完成当前批次');
    const before = db.prepare("SELECT * FROM workflow_topic_issues WHERE topic_id=? AND content_hash=? AND status='pending'").all(topicId, topic.content_hash);
    if (!before.length) throw new Error('本篇没有待关闭的疑点，可能已由其他操作处理');
    const when = new Date().toISOString();
    db.prepare("UPDATE workflow_topic_issues SET status='resolved',resolved_at=? WHERE topic_id=? AND content_hash=? AND status='pending'").run(when, topicId, topic.content_hash);
    const classification = db.prepare('SELECT id,classification FROM topic_classifications WHERE topic_id=? ORDER BY created_at DESC,id DESC LIMIT 1').get(topicId);
    if (classification?.classification === 'not_broker_note' && !db.prepare("SELECT 1 FROM viewpoints WHERE topic_id=? AND review_status='published'").get(topicId)) db.prepare("UPDATE topic_classifications SET review_status='reviewed' WHERE id=?").run(classification.id);
    db.prepare('INSERT INTO review_events(classification_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)')
      .run(classification?.id || null, 'resolve_topic_issues', JSON.stringify({ topic_id: topicId, issues: before }), JSON.stringify({ topic_id: topicId, issue_ids: before.map((item) => item.id), decision: 'no_additional_equity_viewpoint' }), note, when);
    db.exec('COMMIT');
    return { resolved: before.length, topic_id: topicId };
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

function automationStatus(db) {
  const result = getAutomationStatus(db);
  for (const batch of result.batches) {
    batch.reviewed_viewpoints = Number(db.prepare(`SELECT count(DISTINCT v.id) n FROM workflow_batch_topics bt
      JOIN viewpoints v ON v.extraction_result_id=bt.extraction_result_id AND v.topic_id=bt.topic_id
      WHERE bt.batch_id=? AND v.review_status='reviewed'`).get(batch.id).n);
    batch.published_viewpoints = Number(db.prepare(`SELECT count(DISTINCT v.id) n FROM workflow_batch_topics bt
      JOIN viewpoints v ON v.extraction_result_id=bt.extraction_result_id AND v.topic_id=bt.topic_id
      WHERE bt.batch_id=? AND v.review_status='published'`).get(batch.id).n);
  }
  const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE name='workflow_topic_issues'").get();
  const remainingIssues = exists ? Number(db.prepare("SELECT count(*) n FROM workflow_topic_issues wi JOIN raw_topics rt ON rt.topic_id=wi.topic_id WHERE wi.content_hash=rt.content_hash AND wi.status='pending'").get().n) : 0;
  const calls = result.latest_batch ? db.prepare(`SELECT id,stage,provider,requested_model,actual_model,status,started_at,finished_at,duration_ms,input_chars,error_class,error
    FROM model_call_ledger WHERE batch_id=? ORDER BY id DESC LIMIT 12`).all(result.latest_batch.id) : [];
  return { ...result, runtime: workflowState, remaining_issues: remainingIssues, recent_calls: calls };
}

function startWorkflow(action) {
  if (workflowState.status === 'running') return false;
  if (!WORKFLOW_ACTIONS.has(action)) throw new Error('不支持的工作流动作');
  spawnWorkflow([resolve('scripts/workflow.mjs'), action], `实验：${action}`);
  return true;
}

function updateClassification(db, id, body) {
  const before = db.prepare('SELECT * FROM topic_classifications WHERE id=?').get(id);
  if (!before) return null;
  if (db.prepare("SELECT 1 FROM viewpoints WHERE topic_id=? AND review_status='published'").get(before.topic_id)) throw new Error('本篇已有正式发布观点，不能直接覆盖分类；请先建立更正版本');
  const classification = ['broker_note', 'not_broker_note', 'uncertain'].includes(body.classification) ? body.classification : before.classification;
  db.exec('BEGIN IMMEDIATE');
  try {
    db.prepare("UPDATE topic_classifications SET classification=?,review_status='reviewed' WHERE id=?").run(classification, id);
    const affected = db.prepare("SELECT id FROM viewpoints WHERE topic_id=? AND review_status<>'rejected'").all(before.topic_id);
    for (const row of affected) {
      invalidateApproval(db, row.id, '人工修订整篇分类，旧审批失效');
      db.prepare('UPDATE viewpoints SET review_status=?,review_version=review_version+1,updated_at=? WHERE id=?')
        .run(classification === 'not_broker_note' ? 'rejected' : 'candidate', new Date().toISOString(), row.id);
      if (classification === 'not_broker_note') recordApproval(db, { viewpointId: row.id, decision: 'exclude', method: 'human', scopeDecision: 'not_broker_note', reason: body.note || '人工确认整篇非小作文' });
    }
    const after = db.prepare('SELECT * FROM topic_classifications WHERE id=?').get(id);
    db.prepare('INSERT INTO review_events(classification_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)')
      .run(id, 'edit_classification', JSON.stringify(before), JSON.stringify(after), body.note || null, new Date().toISOString());
    db.exec('COMMIT');
    return after;
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

function updateViewpoint(db, id, body) {
  const before = db.prepare('SELECT * FROM viewpoints WHERE id=?').get(id);
  if (!before) return null;
  if (before.review_status === 'published') throw new Error('正式发布记录不可直接修改；请先建立更正版本');
  if (body.review_version != null && Number(body.review_version) !== before.review_version) throw new Error('观点已被其他操作修改，请刷新后重新审核');
  before.security_links = db.prepare('SELECT * FROM viewpoint_securities WHERE viewpoint_id=? ORDER BY link_order').all(id);
  const direction = ['bullish', 'bearish'].includes(body.direction) ? body.direction : before.direction;
  const codesProvided = Object.hasOwn(body, 'ts_codes') || Object.hasOwn(body, 'ts_code');
  const tsCodes = [...new Set(String(body.ts_codes ?? body.ts_code ?? '').toUpperCase().split(/[,，;；\s/]+/u).map((value) => value.trim()).filter(Boolean))];
  const securities = tsCodes.map((tsCode) => {
    const security = db.prepare('SELECT * FROM securities WHERE upper(ts_code)=?').get(tsCode);
    if (!security) throw new Error(`Tushare证券主表中没有代码 ${tsCode}`);
    if (!['SH','SZ','BJ','HK'].includes(security.market)) throw new Error('当前只接受 SH / SZ / BJ / HK 市场');
    return security;
  });
  db.exec('BEGIN IMMEDIATE');
  try {
    invalidateApproval(db, id, '人工编辑观点，之前的 AI 或人工审批失效');
    const target = Object.hasOwn(body, 'target_price') ? (body.target_price === '' || body.target_price == null ? null : Number(body.target_price)) : before.target_price;
    if (target != null && (!Number.isFinite(target) || target <= 0)) throw new Error('目标价必须为大于零的数字');
    db.prepare(`UPDATE viewpoints SET summary=?,direction=?,recommendation_broker=?,recommendation_team=?,target_price=?,target_currency=?,target_price_raw=?,
      recommendation_evidence=?,target_price_evidence=?,
      review_status='candidate',review_version=review_version+1,updated_at=? WHERE id=?`).run(
      body.summary ?? before.summary, direction, body.recommendation_broker === '' ? null : (body.recommendation_broker ?? before.recommendation_broker),
      body.recommendation_team || before.recommendation_team || '其他', target,
      target == null ? null : (body.target_currency === '' ? null : (body.target_currency ?? before.target_currency)), target == null ? null : (body.target_price_raw === '' ? null : (body.target_price_raw ?? before.target_price_raw)),
      body.recommendation_evidence ?? before.recommendation_evidence, target == null ? null : (body.target_price_evidence ?? before.target_price_evidence),
      new Date().toISOString(), id);
    if (codesProvided) {
      const template = before.security_links[0];
      db.prepare('DELETE FROM viewpoint_securities WHERE viewpoint_id=?').run(id);
      const insertLink = db.prepare(`INSERT INTO viewpoint_securities(viewpoint_id,link_order,security_id,surface,code_hint,evidence,confidence,resolution_method,resolution_status,resolved_at)
        VALUES(?,?,?,?,?,?,?,'human_tushare','resolved',?)`);
      securities.forEach((security, index) => insertLink.run(id, index + 1, security.id, template?.surface || security.name, security.ts_code,
        template?.evidence || security.name, template?.confidence ?? before.confidence, new Date().toISOString()));
      db.prepare("UPDATE security_resolution_queue SET status=?,resolution_source='human_tushare',updated_at=? WHERE viewpoint_id=?")
        .run(securities.length ? 'resolved' : 'pending', new Date().toISOString(), id);
    }
    if (body.accept === true) {
      const resolution = db.prepare(`SELECT count(*) link_count,
        sum(CASE WHEN resolution_status='resolved' AND security_id IS NOT NULL THEN 1 ELSE 0 END) resolved_count
        FROM viewpoint_securities WHERE viewpoint_id=?`).get(id);
      if (!resolution.link_count || Number(resolution.resolved_count) !== Number(resolution.link_count)) {
        throw new Error('标的代码尚未全部解决，不能接受');
      }
      db.prepare("UPDATE viewpoints SET review_status='reviewed',updated_at=? WHERE id=?").run(new Date().toISOString(), id);
      recordApproval(db, { viewpointId: id, decision: 'accept', method: 'human', scopeDecision: 'broker_note', reason: body.note || '人工对照原文确认标的、方向及署名证据' });
      const gate = validatePublishBatch(db, [id]);
      if (!gate.ok) throw new Error(gate.issues.map((issue) => issue.message).join('；'));
    }
    const after = db.prepare('SELECT * FROM viewpoints WHERE id=?').get(id);
    after.security_links = db.prepare('SELECT * FROM viewpoint_securities WHERE viewpoint_id=? ORDER BY link_order').all(id);
    db.prepare('INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)')
      .run(id, body.accept === true ? 'edit_and_accept' : 'edit_viewpoint', JSON.stringify(before), JSON.stringify(after), body.note || null, new Date().toISOString());
    db.exec('COMMIT');
    return after;
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

function reviewViewpoint(db, id, action, note) {
  const before = db.prepare(`SELECT v.*,
    (SELECT count(*) FROM viewpoint_securities vs WHERE vs.viewpoint_id=v.id) link_count,
    (SELECT count(*) FROM viewpoint_securities vs WHERE vs.viewpoint_id=v.id AND vs.resolution_status='resolved' AND vs.security_id IS NOT NULL) resolved_count
    FROM viewpoints v WHERE v.id=?`).get(id);
  if (!before) return null;
  if (before.review_status === 'published') throw new Error('正式发布记录不可直接覆盖审批');
  if (action === 'accept' && (!before.link_count || before.resolved_count !== before.link_count)) throw new Error('标的代码尚未全部解决，不能接受');
  const next = action === 'accept' ? 'reviewed' : 'rejected';
  db.exec('BEGIN IMMEDIATE');
  try {
  recordApproval(db, { viewpointId: id, decision: action === 'accept' ? 'accept' : 'exclude', method: 'human', scopeDecision: action === 'accept' ? 'broker_note' : 'uncertain', reason: note || (action === 'accept' ? '人工对照原文确认观点' : '人工驳回观点') });
  db.prepare('UPDATE viewpoints SET review_status=?,updated_at=? WHERE id=?').run(next, new Date().toISOString(), id);
  if (action === 'accept') { const gate = validatePublishBatch(db, [id]); if (!gate.ok) throw new Error(gate.issues.map((issue) => issue.message).join('；')); }
  if (action === 'reject') db.prepare("UPDATE security_resolution_queue SET status='dismissed',resolution_source='human_reject',updated_at=? WHERE viewpoint_id=? AND status='pending'")
    .run(new Date().toISOString(), id);
  const after = db.prepare('SELECT * FROM viewpoints WHERE id=?').get(id);
  db.prepare('INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)')
    .run(id, action, JSON.stringify(before), JSON.stringify(after), note || null, new Date().toISOString());
  db.exec('COMMIT');
  return after;
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

async function publishReviewed(db, body = {}) {
  if (workflowState.status === 'running') throw new Error('工作流仍在运行，结束后才能发布');
  const hasBatches = db.prepare("SELECT 1 FROM sqlite_master WHERE name='workflow_batches'").get();
  if (hasBatches && db.prepare("SELECT 1 FROM workflow_batches WHERE status='running' LIMIT 1").get()) throw new Error('存在运行中的批次，请等待完成后再发布');
  let ids;
  if (body.batch_id != null) {
    const batchId = Number(body.batch_id);
    if (!Number.isSafeInteger(batchId) || batchId < 1) throw new Error('无效批次 ID');
    const batch = db.prepare('SELECT id,status FROM workflow_batches WHERE id=?').get(batchId);
    if (!batch) throw new Error('要发布的批次不存在');
    if (!['completed', 'completed_with_issues'].includes(batch.status)) throw new Error('仅已完成的批次可发布；运行中、暂停或失败批次请先完成处理');
    const unfinished = db.prepare("SELECT 1 FROM workflow_batch_topics WHERE batch_id=? AND status IN ('queued','extracted','failed') LIMIT 1").get(batchId);
    if (unfinished) throw new Error('该批次仍有未完成或失败的内容，不能发布');
    ids = db.prepare(`SELECT DISTINCT v.id FROM workflow_batch_topics bt
      JOIN viewpoints v ON v.extraction_result_id=bt.extraction_result_id AND v.topic_id=bt.topic_id
      WHERE bt.batch_id=? AND v.review_status='reviewed' ORDER BY v.id`).all(batchId).map((row) => row.id);
    if (body.expected_count != null && Number(body.expected_count) !== ids.length) throw new Error('本批次待发布数量已变化，请刷新后再次确认');
    // Do not prefilter by approval validity: every reviewed record in the
    // selected batch must pass the same gate, or none of them is published.
  }
  const result = await publishAccepted(db, { ids, backupDir: resolve(process.env.ZSXQ_BACKUP_DIR || 'data/backups-v2') });
  return { ...result, source_batch_id: body.batch_id == null ? null : Number(body.batch_id) };
}

async function serveStatic(response, url) {
  const relative = url.pathname === '/' ? 'index.html' : decodeURIComponent(url.pathname.slice(1));
  const target = resolve(ROOT, normalize(relative));
  if (!target.startsWith(ROOT)) return sendJson(response, 403, { error: 'forbidden' });
  try {
    const info = await stat(target);
    if (!info.isFile()) throw new Error('not file');
    response.writeHead(200, { 'Content-Type': MIME[extname(target)] || 'application/octet-stream', 'Cache-Control': 'no-cache' });
    response.end(await readFile(target));
  } catch { sendJson(response, 404, { error: 'not found' }); }
}

const server = http.createServer(async (request, response) => {
  const url = new URL(request.url, `http://${request.headers.host || `${HOST}:${PORT}`}`);
  try {
    if (url.pathname.startsWith('/api/') && ['POST', 'PATCH', 'PUT', 'DELETE'].includes(request.method) && request.headers.origin) {
      let origin;
      try { origin = new URL(request.headers.origin).origin; } catch { origin = null; }
      if (origin !== url.origin) return sendJson(response, 403, { error: 'CROSS_ORIGIN_WRITE_DENIED', message: '拒绝跨站网页修改本地数据或触发模型；请从本地管理页面操作' });
    }
    if (url.pathname === '/favicon.ico') { response.writeHead(204); return response.end(); }
    if (url.pathname === '/api/health') return sendJson(response, 200, { ok: true, database: DB_PATH, schema: 'broker-note-v2' });
    if (url.pathname === '/api/dashboard') return sendJson(response, 200, withDb((db) => getDashboard(db, url)));
    if (url.pathname === '/api/security') { const value = withDb((db) => getSecurity(db, url)); return value ? sendJson(response, 200, value) : sendJson(response, 404, { error: 'security not found' }); }
    if (url.pathname === '/api/history/search') return sendJson(response, 200, withDb((db) => searchHistory(db, url)));
    if (url.pathname.startsWith('/api/topic/')) { const value = withDb((db) => getTopic(db, decodeURIComponent(url.pathname.slice(11)), url.searchParams.get('scope'))); return value ? sendJson(response, 200, value) : sendJson(response, 404, { error: 'topic not found' }); }
    if (url.pathname === '/api/automation/status' && request.method === 'GET') return sendJson(response, 200, withDb(automationStatus));
    if (url.pathname === '/api/automation/run' && request.method === 'POST') { const body = await readJson(request); const started = startAutomation(body); return sendJson(response, started ? 202 : 409, { started, runtime: workflowState, message: started ? undefined : '已有工作流运行中' }); }
    if (url.pathname === '/api/automation/stop' && request.method === 'POST') { const body = await readJson(request); const id = Number(body.batch_id); if (!Number.isSafeInteger(id) || id < 1) throw new Error('无效批次 ID'); return sendJson(response, 200, withDb((db) => { ensureAutomationSchema(db); return requestBatchStop(db, id); }, { writable: true })); }
    if (url.pathname === '/api/workflow' && request.method === 'GET') return sendJson(response, 200, withDb(getWorkflowStatus));
    if (url.pathname === '/api/workflow/run' && request.method === 'POST') { const body = await readJson(request); const started = startWorkflow(body.action); return sendJson(response, started ? 202 : 409, { started, runtime: workflowState }); }
    if (url.pathname === '/api/review' && request.method === 'GET') return sendJson(response, 200, withDb((db) => getReviewTopics(db, url)));
    if (/^\/api\/review\/topic\/[^/]+\/resolve$/.test(url.pathname) && request.method === 'POST') { const topicId = decodeURIComponent(url.pathname.split('/')[4]); const body = await readJson(request); return sendJson(response, 200, withDb((db) => resolveTopicIssues(db, topicId, body), { writable: true })); }
    if (url.pathname.startsWith('/api/review/classification/') && request.method === 'PATCH') { const id = Number(url.pathname.split('/').at(-1)); const body = await readJson(request); const value = withDb((db) => updateClassification(db, id, body), { writable: true }); return sendJson(response, 200, value); }
    if (url.pathname.startsWith('/api/review/viewpoint/') && request.method === 'PATCH') { const id = Number(url.pathname.split('/').at(-1)); const body = await readJson(request); const value = withDb((db) => updateViewpoint(db, id, body), { writable: true }); return sendJson(response, 200, value); }
    if (/^\/api\/review\/viewpoint\/\d+\/(accept|reject)$/.test(url.pathname) && request.method === 'POST') {
      const parts = url.pathname.split('/'); const id = Number(parts[4]); const action = parts[5]; const body = await readJson(request);
      return sendJson(response, 200, withDb((db) => reviewViewpoint(db, id, action, body.note), { writable: true }));
    }
    if (url.pathname === '/api/workflow/publish' && request.method === 'POST') {
      const body = await readJson(request);
      const db = new DatabaseSync(DB_PATH); try { db.exec('PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;'); ensureWorkflowSchema(db); return sendJson(response, 200, await publishReviewed(db, body)); } finally { db.close(); }
    }
    if (url.pathname.startsWith('/api/')) return sendJson(response, 404, { error: 'api not found' });
    return serveStatic(response, url);
  } catch (error) {
    console.error(error);
    sendJson(response, error.code === 'PUBLISH_VALIDATION_FAILED' ? 422 : 500, { error: error.code || 'internal error', message: error.message, issues: error.issues });
  }
});

server.listen(PORT, HOST, () => {
  console.log(`券商小作文看板：http://${HOST}:${PORT}`);
  console.log(`SQLite：${DB_PATH}`);
});
