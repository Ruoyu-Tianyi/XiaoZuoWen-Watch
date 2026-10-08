import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import { openWorkflowDb, nowIso } from './lib/workflow_db.mjs';
import { callStructuredModel, classificationSchema, extractionSchema, approvalSchema, exactEvidenceExists, repairExactEvidence } from './lib/model_client.mjs';
import { fetchAStockMaster, fetchHKStockMaster, syncSecurityMaster } from './lib/tushare_client.mjs';

const ROOT = process.cwd();
const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const WORKFLOW_CONFIG_PATH = resolve('config/workflow.json');
const PROMPTS_PATH = resolve('config/broker_note_prompts.json');
const RAW_PATH = resolve('data/current_window_topics.json');
const REPORT_JSON = resolve('reports/pilot_quality.json');
const REPORT_MD = resolve('reports/pilot_quality.md');
const command = process.argv[2] || 'status';
const config = JSON.parse(readFileSync(WORKFLOW_CONFIG_PATH, 'utf8'));
const prompts = JSON.parse(readFileSync(PROMPTS_PATH, 'utf8'));
if (!process.env.CLAUDE_CLI && config.model.claude_cli_path) process.env.CLAUDE_CLI = config.model.claude_cli_path;
const db = openWorkflowDb(DB_PATH);

function option(name, fallback = null) {
  return process.argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
}

function hashText(value) {
  return createHash('sha256').update(String(value || ''), 'utf8').digest('hex');
}

function contentHash(topic) {
  return hashText(`${topic.title || ''}\n${topic.content || ''}`);
}

function createWorkflowRun(name, metadata = {}) {
  const result = db.prepare(`INSERT INTO workflow_runs(command,status,started_at,model_provider,model_id,metadata_json)
    VALUES(?,'running',?,?,?,?)`).run(name, nowIso(), config.model.provider, config.model.model_id, JSON.stringify(metadata));
  return Number(result.lastInsertRowid);
}

function finishWorkflowRun(id, status, metrics = {}, error = null) {
  db.prepare(`UPDATE workflow_runs SET status=?,finished_at=?,input_count=?,success_count=?,failed_count=?,skipped_count=?,metadata_json=?,error=? WHERE id=?`)
    .run(status, nowIso(), Number(metrics.input || 0), Number(metrics.success || 0), Number(metrics.failed || 0), Number(metrics.skipped || 0), JSON.stringify(metrics), error, id);
}

async function withinRun(name, callback, metadata = {}) {
  const runId = createWorkflowRun(name, metadata);
  try {
    const metrics = await callback(runId);
    finishWorkflowRun(runId, 'success', metrics);
    return { workflow_run_id: runId, ...metrics };
  } catch (error) {
    finishWorkflowRun(runId, 'failed', {}, error.message);
    throw error;
  }
}

function fetchRawWindow() {
  const result = spawnSync(process.execPath, [resolve('scripts/fetch_recent.mjs')], {
    cwd: ROOT,
    encoding: 'utf8',
    windowsHide: true,
    maxBuffer: 64 * 1024 * 1024,
    env: { ...process.env, ZSXQ_HOURS: String(config.window_hours), ZSXQ_RAW_OUTPUT: RAW_PATH, ZSXQ_GROUP_ID: config.group_id },
  });
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || `抓取进程退出码 ${result.status}`).trim());
  return JSON.parse(readFileSync(RAW_PATH, 'utf8'));
}

function attachmentInfo(topic) {
  const content = String(topic.content || '').trim();
  const hasImages = Array.isArray(topic.images) && topic.images.length > 0;
  const hasFiles = Array.isArray(topic.files) && topic.files.length > 0;
  const reasons = [];
  if (!content) reasons.push('正文为空');
  if (hasImages) reasons.push('包含图片');
  if (hasFiles) reasons.push('包含文件');
  return { hasImages, hasFiles, eligible: reasons.length === 0, reason: reasons.join('、') || null };
}

function ingestPayload(payload, workflowRunId) {
  const started = nowIso();
  const ingestion = db.prepare(`INSERT INTO ingestion_runs(workflow_run_id,group_id,started_at,cutoff_at,window_hours,pages_fetched,topic_count,eligible_count,excluded_count,status)
    VALUES(?,?,?,?,?,?,?,?,?,'running')`).run(workflowRunId, String(payload.group_id), started, payload.cutoff_at, payload.window_hours, payload.pages_fetched, payload.count, 0, 0);
  const ingestionId = Number(ingestion.lastInsertRowid);
  const existing = db.prepare('SELECT content_hash FROM raw_topics WHERE topic_id=?');
  const upsert = db.prepare(`
    INSERT INTO raw_topics(topic_id,group_id,group_name,topic_type,published_at,published_at_utc,title,content,poster_name,poster_alias,
      readers,likes,comments,has_images,has_files,eligibility,exclusion_reason,content_hash,raw_json,first_seen_at,last_seen_at,last_ingestion_run_id)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(topic_id) DO UPDATE SET group_name=excluded.group_name,topic_type=excluded.topic_type,published_at=excluded.published_at,
      published_at_utc=excluded.published_at_utc,title=excluded.title,content=excluded.content,poster_name=excluded.poster_name,
      poster_alias=excluded.poster_alias,readers=excluded.readers,likes=excluded.likes,comments=excluded.comments,
      has_images=excluded.has_images,has_files=excluded.has_files,eligibility=excluded.eligibility,exclusion_reason=excluded.exclusion_reason,
      content_hash=excluded.content_hash,raw_json=excluded.raw_json,last_seen_at=excluded.last_seen_at,last_ingestion_run_id=excluded.last_ingestion_run_id
  `);
  let eligible = 0;
  let excluded = 0;
  let created = 0;
  let changed = 0;
  let unchanged = 0;
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const topic of payload.topics || []) {
      const topicId = String(topic.topic_id);
      const prior = existing.get(topicId);
      const nextHash = contentHash(topic);
      const attachment = attachmentInfo(topic);
      attachment.eligible ? eligible++ : excluded++;
      if (!prior) created++;
      else if (prior.content_hash !== nextHash) changed++;
      else unchanged++;
      upsert.run(topicId, String(topic.group?.group_id || payload.group_id), topic.group?.name || payload.group_name || '', topic.type || topic.topic_type || 'talk',
        topic.create_time, new Date(topic.create_time).toISOString(), topic.title || '', topic.content || '', topic.owner?.name || '', topic.owner?.alias || '',
        topic.counts?.readers || 0, topic.counts?.likes || 0, topic.counts?.comments || 0, Number(attachment.hasImages), Number(attachment.hasFiles),
        attachment.eligible ? 'eligible' : 'excluded', attachment.reason, nextHash, JSON.stringify(topic), started, started, ingestionId);
    }
    db.prepare(`UPDATE ingestion_runs SET finished_at=?,eligible_count=?,excluded_count=?,new_count=?,changed_count=?,unchanged_count=?,status='success' WHERE id=?`)
      .run(nowIso(), eligible, excluded, created, changed, unchanged, ingestionId);
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    db.prepare(`UPDATE ingestion_runs SET finished_at=?,status='failed',error=? WHERE id=?`).run(nowIso(), error.message, ingestionId);
    throw error;
  }
  return { input: payload.count, success: payload.count, eligible, excluded, created, changed, unchanged, pages: payload.pages_fetched };
}

async function fetchAndIngest(workflowRunId) {
  const payload = fetchRawWindow();
  return ingestPayload(payload, workflowRunId);
}

async function syncSecurities() {
  const [aShares, hkShares] = await Promise.all([fetchAStockMaster(config.tushare), fetchHKStockMaster(config.tushare)]);
  const rows = [...aShares, ...hkShares];
  const count = syncSecurityMaster(db, rows);
  db.prepare(`INSERT INTO workflow_settings(key,value,updated_at) VALUES('tushare_last_sync',?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`)
    .run(JSON.stringify({ count, a_shares: aShares.length, hk_shares: hkShares.length }), nowIso());
  return { input: rows.length, success: count, failed: 0 };
}

function scoreTopic(topic) {
  const text = `${topic.title || ''}\n${topic.content || ''}`;
  let score = Math.min(5, Math.floor(text.length / 350));
  if (/(推荐|看好|关注|高弹性|目标价|买入|增持|催化|估值|业绩|盈利预测|受益)/u.test(text)) score += 4;
  if (/(证券|研究所|研究组|团队|分析师|联系人)/u.test(text)) score += 3;
  if (/^(?:【?快讯|新闻|公告|盘中|异动)/u.test(text.trim())) score -= 5;
  if (text.length < 180) score -= 3;
  return score;
}

function spreadPick(rows, count) {
  if (rows.length <= count) return rows;
  const picked = [];
  for (let index = 0; index < count; index++) picked.push(rows[Math.floor(index * (rows.length - 1) / Math.max(1, count - 1))]);
  return [...new Map(picked.map((row) => [row.topic_id, row])).values()];
}

function ensurePilotSamples() {
  const present = Number(db.prepare('SELECT count(*) n FROM pilot_samples').get().n);
  if (present >= Number(config.pilot.size)) return present;
  const rows = db.prepare(`SELECT topic_id,title,content,published_at_utc FROM raw_topics WHERE eligibility='eligible' ORDER BY published_at_utc DESC`).all()
    .map((row) => ({ ...row, score: scoreTopic(row), length: `${row.title}\n${row.content}`.length }));
  if (rows.length < Number(config.pilot.size)) throw new Error(`合格主题仅 ${rows.length} 篇，不足以选择 ${config.pilot.size} 篇试跑样本`);
  const selected = [];
  const existingSamples = db.prepare('SELECT topic_id,stratum,sample_order FROM pilot_samples ORDER BY sample_order').all();
  const used = new Set(existingSamples.map((row) => row.topic_id));
  const existingCounts = new Map(['likely_broker_note','obvious_non_note','boundary'].map((stratum) =>
    [stratum, existingSamples.filter((row) => row.stratum === stratum).length]));
  const add = (items, stratum) => {
    for (const item of items) if (!used.has(item.topic_id)) { used.add(item.topic_id); selected.push({ ...item, stratum }); }
  };
  const likely = rows.filter((row) => row.score >= 5 && !used.has(row.topic_id)).sort((a, b) => a.length - b.length);
  add(spreadPick(likely, Math.max(0, config.pilot.likely_broker_notes - existingCounts.get('likely_broker_note'))), 'likely_broker_note');
  const non = rows.filter((row) => row.score <= 0 && !used.has(row.topic_id)).sort((a, b) => a.length - b.length);
  add(spreadPick(non, Math.max(0, config.pilot.obvious_non_notes - existingCounts.get('obvious_non_note'))), 'obvious_non_note');
  const boundary = rows.filter((row) => !used.has(row.topic_id)).sort((a, b) => Math.abs(a.score - 3) - Math.abs(b.score - 3));
  add(boundary.slice(0, Math.max(0, config.pilot.boundary_cases - existingCounts.get('boundary'))), 'boundary');
  add(rows.filter((row) => !used.has(row.topic_id)).slice(0, config.pilot.size - present - selected.length), 'boundary');
  const insert = db.prepare('INSERT OR IGNORE INTO pilot_samples(topic_id,stratum,sample_order,selected_at) VALUES(?,?,?,?)');
  selected.slice(0, config.pilot.size - present).forEach((row, index) => insert.run(row.topic_id, row.stratum, present + index + 1, nowIso()));
  return Number(db.prepare('SELECT count(*) n FROM pilot_samples').get().n);
}

function modelSettings(stage) {
  const rowProvider = db.prepare("SELECT value FROM workflow_settings WHERE key='model_provider'").get();
  const rowModel = db.prepare("SELECT value FROM workflow_settings WHERE key='model_id'").get();
  const provider = option('provider', rowProvider?.value || config.model.provider);
  return {
    provider,
    model: option('model', rowModel?.value || (provider === 'claude' ? 'claude-cli-default' : provider === 'codex' ? 'gpt-5.6-luna' : config.model.model_id)),
    contextSize: config.model.context_size,
    temperature: config.model.temperature,
    promptVersion: prompts[stage].version,
  };
}

function modelInput(topic) {
  return `标题：${topic.title || '（无标题）'}\n正文：\n${topic.content}`;
}

function createModelRun(workflowRunId, stage, settings, count) {
  const result = db.prepare(`INSERT INTO model_runs(workflow_run_id,stage,provider,model_id,prompt_version,status,started_at,input_count)
    VALUES(?,?,?,?,?,'running',?,?)`).run(workflowRunId, stage, settings.provider, settings.model, settings.promptVersion, nowIso(), count);
  return Number(result.lastInsertRowid);
}

function normalizeClassification(topic, result) {
  if (!config.classification.labels.includes(result.classification)) throw new Error('分类枚举非法');
  const evidence = repairExactEvidence(topic, result.evidence);
  if (!evidence) throw new Error('分类证据不在原文中');
  return {
    classification: result.classification,
    confidence: Math.max(0, Math.min(1, Number(result.confidence))),
    evidence,
    reasons: Array.isArray(result.reasons) ? result.reasons.map(String) : [],
    negativeSignals: Array.isArray(result.negative_signals) ? result.negative_signals.map(String) : [],
  };
}

async function classifyPilot(workflowRunId) {
  ensurePilotSamples();
  const settings = modelSettings('classification');
  const allTopics = db.prepare(`SELECT rt.* FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id ORDER BY ps.sample_order`).all();
  const requestedLimit = Number(option('limit', '0'));
  const topics = requestedLimit > 0 ? allTopics.slice(0, requestedLimit) : allTopics;
  const modelRunId = createModelRun(workflowRunId, 'classification', settings, topics.length);
  const existing = db.prepare(`SELECT id FROM topic_classifications WHERE topic_id=? AND content_hash=? AND provider=? AND model_id=? AND prompt_version=?`);
  const insert = db.prepare(`INSERT INTO topic_classifications(topic_id,model_run_id,content_hash,classification,confidence,evidence,reasons_json,negative_signals_json,provider,model_id,prompt_version,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`);
  let success = 0;
  let failed = 0;
  let skipped = 0;
  let duration = 0;
  const pendingTopics = topics.filter((topic) => {
    if (existing.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion)) {
      skipped++;
      return false;
    }
    return true;
  });
  const processTopic = async (topic) => {
    let call = null;
    try {
      call = await callStructuredModel({ provider: settings.provider, model: settings.model, contextSize: settings.contextSize, temperature: settings.temperature,
        schema: classificationSchema, system: prompts.classification.system,
        prompt: `${prompts.classification.instruction}\n\n${modelInput(topic)}` });
      const value = normalizeClassification(topic, call.result);
      insert.run(topic.topic_id, modelRunId, topic.content_hash, value.classification, value.confidence, value.evidence,
        JSON.stringify(value.reasons), JSON.stringify(value.negativeSignals), settings.provider, settings.model, settings.promptVersion, nowIso());
      db.prepare("INSERT INTO model_attempts(model_run_id,topic_id,stage,status,raw_result_json,created_at) VALUES(?,?,'classification','success',?,?)")
        .run(modelRunId, topic.topic_id, JSON.stringify(call.result), nowIso());
      duration += Math.round(call.durationMs || 0);
      success++;
    } catch (error) {
      failed++;
      db.prepare("INSERT INTO model_attempts(model_run_id,topic_id,stage,status,raw_result_json,error,created_at) VALUES(?,?,'classification','failed',?,?,?)")
        .run(modelRunId, topic.topic_id, call ? JSON.stringify(call.result) : null, error.message, nowIso());
      console.error(`分类失败 ${topic.topic_id}: ${error.message}`);
    }
    db.prepare('UPDATE model_runs SET success_count=?,failed_count=?,total_duration_ms=? WHERE id=?').run(success, failed, duration, modelRunId);
  };
  const concurrency = Math.max(1, Math.min(2, Number(option('concurrency', '2')) || 2));
  let nextTopic = 0;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (nextTopic < pendingTopics.length) await processTopic(pendingTopics[nextTopic++]);
  }));
  db.prepare(`UPDATE model_runs SET status=?,finished_at=?,success_count=?,failed_count=?,total_duration_ms=?,error=? WHERE id=?`)
    .run(failed ? 'failed' : 'success', nowIso(), success, failed, duration, failed ? `${failed} 篇分类失败` : null, modelRunId);
  if (failed) throw new Error(`${failed} 篇分类失败，已保存成功进度，可重跑续接`);
  return { input: topics.length, success, failed, skipped, model_run_id: modelRunId, duration_ms: duration };
}

const BROKER_ALIASES = [
  { canonical: '申万宏源', aliases: ['申万宏源'] },
  { canonical: '国泰海通', aliases: ['国泰海通'] },
  { canonical: '中金公司', aliases: ['中金公司', '中金'] },
  { canonical: '国投证券', aliases: ['国投证券', '国投'] },
  { canonical: '长江证券', aliases: ['长江证券', '长江'] },
  { canonical: '浙商证券', aliases: ['浙商证券', '浙商'] },
  { canonical: '广发证券', aliases: ['广发证券', '广发'] },
  { canonical: '开源证券', aliases: ['开源证券', '开源'] },
];

function normalizeRecommendation(topic, raw) {
  const rawBroker = raw?.broker == null ? null : String(raw.broker).trim() || null;
  const rawGroup = raw?.research_group == null ? null : String(raw.research_group).trim() || null;
  const brokerMention = rawBroker ? repairExactEvidence(topic, rawBroker) : null;
  const groupMention = rawGroup ? repairExactEvidence(topic, rawGroup) : null;
  const rawEvidence = raw?.evidence == null ? null : repairExactEvidence(topic, raw.evidence);
  let broker = null;
  let group = null;
  if (rawBroker && (brokerMention || rawEvidence)) {
    const known = BROKER_ALIASES.flatMap((entry) => entry.aliases.map((alias) => ({ ...entry, alias })))
      .sort((left, right) => right.alias.length - left.alias.length)
      .find((entry) => rawBroker.includes(entry.alias) && (brokerMention || rawEvidence?.includes(entry.alias)));
    if (known) {
      broker = known.canonical;
      const suffix = rawBroker.replace(known.alias, '').replace(/^(?:证券|研究所)/u, '').trim();
      if (suffix) group = suffix;
    }
  }
  if (!broker && brokerMention) {
    const formal = rawBroker.match(/^(.+?(?:证券|研究所|资本|国际))(.*)$/u);
    if (formal) {
      broker = formal[1].trim();
      group = formal[2].trim() || null;
    } else {
      group = rawBroker;
    }
  }
  if (groupMention) group = rawGroup;
  let evidence = brokerMention || groupMention || rawEvidence;
  if (!broker && !group) evidence = null;
  const authors = (Array.isArray(raw?.authors) ? raw.authors : []).map(String).map((value) => value.trim()).filter((value) => value && repairExactEvidence(topic, value));
  return { broker, group: group || '其他', authors, evidence };
}

const GENERIC_SECURITY_SURFACE = /^(?:A股|港股|H股|美股|个股|公司|标的|受益标的|相关标的|板块|行业)$/iu;

function securitySurfaceExists(topic, surface) {
  const original = `${topic.title || ''}\n${topic.content || ''}`;
  if (original.includes(surface)) return true;
  const scoped = String(surface || '').trim().match(/^(.*?)(?:\s+(?:AH|A|H)|[（(](?:AH|A|H)[）)])$/iu);
  return Boolean(scoped?.[1] && original.includes(scoped[1].trim()));
}

function normalizeSecuritySurface(topic, raw) {
  const supplied = String(raw?.security_surface || '').trim();
  const original = `${topic.title || ''}\n${topic.content || ''}`;
  if (supplied && !GENERIC_SECURITY_SURFACE.test(supplied) && securitySurfaceExists(topic, supplied)) {
    const hasMarketScope = /(?:\s+(?:AH|A|H)|[（(](?:AH|A|H)[）)])$/iu.test(supplied);
    if (!hasMarketScope) {
      const canonical = db.prepare(`SELECT a.alias
        FROM security_aliases a JOIN securities s ON s.id=a.security_id
        WHERE s.market IN ('SH','SZ','BJ') AND length(a.alias)>=2 AND instr(?,a.alias)>0
        GROUP BY a.alias HAVING count(DISTINCT s.id)=1
        ORDER BY length(a.alias) DESC,a.priority LIMIT 1`).get(supplied);
      if (canonical?.alias) return canonical.alias;
    }
    return supplied;
  }

  // 模型偶尔会在摘要中正确写出公司，却把结构化标的字段错填为“A股”等泛称。
  // 仅用证券主表中唯一的A股精确别名、且别名确实存在于原文时进行确定性修复。
  const summary = String(raw?.summary || '').trim();
  const recovered = db.prepare(`SELECT a.alias
    FROM security_aliases a JOIN securities s ON s.id=a.security_id
    WHERE s.market IN ('SH','SZ','BJ') AND length(a.alias)>=2
      AND instr(?,a.alias)>0 AND instr(?,a.alias)>0
    GROUP BY a.alias HAVING count(DISTINCT s.id)=1
    ORDER BY length(a.alias) DESC,instr(?,a.alias),a.priority LIMIT 1`).get(summary, original, summary);
  return recovered?.alias || supplied;
}

function normalizeExtraction(topic, result) {
  const recommendation = normalizeRecommendation(topic, result.recommendation);
  const viewpoints = [];
  const invalid = [];
  const seen = new Set();
  for (const raw of Array.isArray(result.viewpoints) ? result.viewpoints : []) {
    const surface = normalizeSecuritySurface(topic, raw);
    const evidence = repairExactEvidence(topic, raw.evidence);
    const securityEvidence = repairExactEvidence(topic, raw.security_evidence);
    if (!['bullish', 'bearish'].includes(raw.direction)) { invalid.push('direction'); continue; }
    if (!surface || GENERIC_SECURITY_SURFACE.test(surface) || !securitySurfaceExists(topic, surface) || !securityEvidence || !evidence) {
      invalid.push(`evidence_or_surface:${surface}`);
      continue;
    }
    const codeHint = raw.security_code_hint == null ? null : String(raw.security_code_hint).trim() || null;
    const explicitForeignCode = /\.US$/iu.test(codeHint || '');
    const latinOnlyUnresolved = /^[\p{Script=Latin}\d .&+\-_/]+$/u.test(surface)
      && !/(?:\s+(?:AH|A|H)|[（(](?:AH|A|H)[）)])$/iu.test(surface)
      && !/\.HK$/iu.test(codeHint || '')
      && resolveSecurity(surface, codeHint).status !== 'resolved';
    if (explicitForeignCode || latinOnlyUnresolved) {
      invalid.push(`out_of_scope_foreign_or_non_security:${surface}`);
      continue;
    }
    const targetEvidence = raw.target_price_evidence == null ? null : repairExactEvidence(topic, raw.target_price_evidence);
    if (raw.target_price_evidence != null && !targetEvidence) { invalid.push(`target_evidence:${surface}`); continue; }
    const key = `${surface}\u0000${raw.direction}\u0000${evidence}`;
    if (seen.has(key)) continue;
    seen.add(key);
    viewpoints.push({
      summary: String(raw.summary || '').trim() || evidence.slice(0, 180),
      direction: raw.direction,
      surface,
      codeHint,
      securityEvidence,
      evidence,
      targetPrice: raw.target_price == null ? null : Number(raw.target_price),
      targetCurrency: raw.target_currency == null ? null : String(raw.target_currency).trim() || null,
      targetRaw: raw.target_price_raw == null ? null : String(raw.target_price_raw).trim() || null,
      targetEvidence,
      confidence: Math.max(0, Math.min(1, Number(raw.confidence))),
    });
  }
  return { recommendation, viewpoints, invalid, abstainReason: result.abstain_reason || null };
}

function resolveSecurity(surface, codeHint) {
  const code = String(codeHint || '').trim().toUpperCase();
  if (code) {
    const parsedCode = code.match(/^(\d{1,6})(?:\.(SH|SZ|BJ|HK))?$/u);
    const normalizedCode = parsedCode?.[2]
      ? `${parsedCode[1].padStart(parsedCode[2] === 'HK' ? 5 : 6, '0')}.${parsedCode[2]}`
      : code;
    const direct = db.prepare('SELECT * FROM securities WHERE upper(ts_code)=?').get(normalizedCode);
    if (direct) return { status: 'resolved', security: direct, securities: [direct], method: 'tushare_ts_code', candidates: [direct.id] };
    const symbol = parsedCode?.[1]
      ? parsedCode[1].padStart(parsedCode[2] === 'HK' ? 5 : 6, '0')
      : code.match(/\d{5,6}/)?.[0];
    if (symbol) {
      const matches = parsedCode?.[2]
        ? db.prepare('SELECT * FROM securities WHERE symbol=? AND market=?').all(symbol, parsedCode[2])
        : db.prepare('SELECT * FROM securities WHERE symbol=?').all(symbol);
      if (matches.length === 1) return { status: 'resolved', security: matches[0], securities: matches, method: 'tushare_symbol', candidates: [matches[0].id] };
      if (matches.length > 1) return { status: 'ambiguous', securities: [], candidates: matches.map((row) => row.id) };
    }
  }
  const scopeMatch = String(surface || '').trim().match(/^(.*?)(?:\s+(AH|A|H)|[（(](AH|A|H)[）)])$/iu);
  const lookupSurface = scopeMatch ? scopeMatch[1].trim() : surface;
  const scope = (scopeMatch?.[2] || scopeMatch?.[3] || '').toUpperCase() || null;
  const exact = db.prepare(`SELECT s.* FROM security_aliases a JOIN securities s ON s.id=a.security_id WHERE a.alias=? ORDER BY a.priority,s.list_status='L' DESC`).all(lookupSurface);
  const allUnique = [...new Map(exact.map((row) => [row.id, row])).values()];
  // 业务口径：原文没有 A/H/AH 标记，也没有明确的港股代码时，名称默认指 A 股。
  // 这样双重上市的同名证券不会因为同步港股主表而被误判为歧义。
  const unique = scope ? allUnique : allUnique.filter((row) => ['SH', 'SZ', 'BJ'].includes(row.market));
  if (scope) {
    const eligible = allUnique.filter((row) => scope === 'AH' || (scope === 'H' ? row.market === 'HK' : ['SH', 'SZ', 'BJ'].includes(row.market)));
    const byMarket = new Map();
    for (const row of eligible) {
      if (!byMarket.has(row.market)) byMarket.set(row.market, []);
      byMarket.get(row.market).push(row);
    }
    const scoped = [...byMarket.values()].map((rows) => rows.sort((left, right) =>
      Number(right.list_status === 'L') - Number(left.list_status === 'L')
      || Number(String(right.symbol).startsWith('0')) - Number(String(left.symbol).startsWith('0'))
      || String(left.symbol).localeCompare(String(right.symbol)))[0]);
    const complete = scope === 'AH' ? scoped.some((row) => row.market === 'HK') && scoped.some((row) => row.market !== 'HK') : scoped.length > 0;
    if (complete) return { status: 'resolved', security: scoped[0], securities: scoped, method: `tushare_alias_${scope.toLowerCase()}`, candidates: scoped.map((row) => row.id) };
    return { status: 'unresolved', securities: [], candidates: allUnique.map((row) => row.id), scope, lookupSurface };
  }
  if (unique.length === 1) return { status: 'resolved', security: unique[0], securities: unique, method: 'tushare_alias_exact', candidates: [unique[0].id] };
  if (unique.length > 1) return { status: 'ambiguous', securities: [], candidates: unique.map((row) => row.id) };
  const hkExact = allUnique.filter((row) => row.market === 'HK');
  if (hkExact.length === 1) return { status: 'resolved', security: hkExact[0], securities: hkExact, method: 'tushare_alias_hk_fallback', candidates: [hkExact[0].id] };
  if (hkExact.length > 1) return { status: 'ambiguous', securities: [], candidates: hkExact.map((row) => row.id) };
  if (lookupSurface.length >= 2) {
    const fuzzy = db.prepare(`SELECT DISTINCT s.* FROM security_aliases a JOIN securities s ON s.id=a.security_id
      WHERE s.market IN ('SH','SZ','BJ') AND length(a.alias)>=2 AND (a.alias LIKE ? OR ? LIKE '%'||a.alias||'%') LIMIT 8`).all(`%${lookupSurface}%`, lookupSurface);
    if (fuzzy.length === 1) return { status: 'resolved', security: fuzzy[0], securities: fuzzy, method: 'tushare_alias_unique_fuzzy', candidates: [fuzzy[0].id] };
    if (fuzzy.length > 1) return { status: 'ambiguous', securities: [], candidates: fuzzy.map((row) => row.id) };
  }
  return { status: 'unresolved', securities: [], candidates: [] };
}

function mergeViewpointsBySecurity(viewpoints) {
  const groups = new Map();
  for (const viewpoint of viewpoints) {
    const resolution = resolveSecurity(viewpoint.surface, viewpoint.codeHint);
    const securityIds = (resolution.securities || []).map((security) => security.id).sort((a, b) => a - b);
    const identity = securityIds.length
      ? `security:${securityIds.join(',')}`
      : `surface:${viewpoint.surface.trim().toLocaleLowerCase('zh-CN')}:${String(viewpoint.codeHint || '').toUpperCase()}`;
    if (!groups.has(identity)) groups.set(identity, { identity, resolution, items: [] });
    groups.get(identity).items.push(viewpoint);
  }
  return [...groups.values()].map((group) => {
    const rankedDirections = ['bullish', 'bearish'].map((direction) => {
      const items = group.items.filter((item) => item.direction === direction);
      return { direction, items, count: items.length, confidence: items.reduce((sum, item) => sum + item.confidence, 0) };
    }).sort((left, right) => right.count - left.count || right.confidence - left.confidence || (left.direction === group.items[0].direction ? -1 : 1));
    const chosen = rankedDirections[0];
    const opposite = rankedDirections[1];
    const preferred = chosen.items.find((item) => item.targetPrice != null) || chosen.items[0] || group.items[0];
    const unique = (values) => [...new Set(values.filter(Boolean))];
    const positiveSummary = unique(chosen.items.map((item) => item.summary)).join('；');
    const oppositeSummary = unique(opposite.items.map((item) => item.summary)).join('；');
    return {
      ...preferred,
      mergeKey: group.identity,
      resolution: group.resolution,
      direction: chosen.direction,
      summary: oppositeSummary ? `${positiveSummary}；风险/反向因素：${oppositeSummary}` : positiveSummary,
      supportingEvidence: unique(group.items.map((item) => item.evidence)),
      securityEvidenceList: unique(group.items.map((item) => item.securityEvidence)),
      evidence: group.items[0].evidence,
      securityEvidence: group.items[0].securityEvidence,
      confidence: Math.min(...group.items.map((item) => item.confidence)),
    };
  });
}

async function extractPilot(workflowRunId) {
  const settings = modelSettings('extraction');
  const allTopics = db.prepare(`
    SELECT rt.*,tc.id classification_id,tc.classification
    FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id
    JOIN topic_classifications tc ON tc.id=(SELECT tc2.id FROM topic_classifications tc2 WHERE tc2.topic_id=rt.topic_id ORDER BY tc2.created_at DESC,tc2.id DESC LIMIT 1)
    WHERE tc.classification IN ('broker_note','uncertain') ORDER BY ps.sample_order
  `).all();
  const requestedLimit = Number(option('limit', '0'));
  const topics = requestedLimit > 0 ? allTopics.slice(0, requestedLimit) : allTopics;
  const modelRunId = createModelRun(workflowRunId, 'extraction', settings, topics.length);
  const existing = db.prepare(`SELECT id FROM extraction_results WHERE topic_id=? AND content_hash=? AND provider=? AND model_id=? AND prompt_version=? AND status='success'`);
  const existingAnyModel = db.prepare(`SELECT id FROM extraction_results WHERE topic_id=? AND content_hash=? AND status='success' LIMIT 1`);
  const insertResult = db.prepare(`INSERT INTO extraction_results(topic_id,classification_id,model_run_id,content_hash,provider,model_id,prompt_version,status,raw_result_json,validation_json,duration_ms,created_at)
    VALUES(?,?,?,?,?,?,?,'success',?,?,?,?)
    ON CONFLICT(topic_id,content_hash,provider,model_id,prompt_version) DO UPDATE SET
      classification_id=excluded.classification_id,model_run_id=excluded.model_run_id,status='success',raw_result_json=excluded.raw_result_json,
      validation_json=excluded.validation_json,duration_ms=excluded.duration_ms,error=NULL,created_at=excluded.created_at`);
  const findResult = db.prepare(`SELECT id FROM extraction_results WHERE topic_id=? AND content_hash=? AND provider=? AND model_id=? AND prompt_version=?`);
  const replayAttempt = db.prepare(`SELECT ma.raw_result_json FROM model_attempts ma JOIN model_runs mr ON mr.id=ma.model_run_id
    WHERE ma.topic_id=? AND ma.stage='extraction' AND ma.raw_result_json IS NOT NULL AND mr.provider=? AND mr.model_id=? AND mr.prompt_version=?
    ORDER BY ma.id DESC LIMIT 1`);
  const insertViewpoint = db.prepare(`INSERT INTO viewpoints(topic_id,extraction_result_id,ordinal,summary,direction,evidence,supporting_evidence_json,recommendation_broker,recommendation_team,
    recommendation_authors_json,recommendation_evidence,target_price,target_currency,target_price_raw,target_price_evidence,confidence,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const insertLink = db.prepare(`INSERT INTO viewpoint_securities(viewpoint_id,link_order,security_id,surface,code_hint,evidence,confidence,resolution_method,resolution_status,resolved_at)
    VALUES(?,?,?,?,?,?,?,?,?,?)`);
  const insertQueue = db.prepare(`INSERT INTO security_resolution_queue(viewpoint_id,surface,code_hint,evidence,status,candidate_security_ids_json,search_query,created_at,updated_at)
    VALUES(?,?,?,?,'pending',?,?,?,?)`);
  let success = 0;
  let failed = 0;
  let skipped = 0;
  let duration = 0;
  let viewpointCount = 0;
  const rebuild = option('rebuild', 'false') === 'true';
  const rebuildReviewed = option('rebuild-reviewed', 'false') === 'true';
  const onlyMissingAny = option('only-missing-any', 'false') === 'true';
  const pendingTopics = topics.filter((topic) => {
    if (!rebuild && onlyMissingAny && existingAnyModel.get(topic.topic_id, topic.content_hash)) {
      skipped++;
      return false;
    }
    if (!rebuild && existing.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion)) {
      skipped++;
      return false;
    }
    return true;
  });
  const processTopic = async (topic) => {
    let call = null;
    try {
      const replay = replayAttempt.get(topic.topic_id, settings.provider, settings.model, settings.promptVersion);
      call = replay
        ? { result: JSON.parse(replay.raw_result_json), raw: { replayed: true }, durationMs: 0 }
        : await callStructuredModel({ provider: settings.provider, model: settings.model, contextSize: settings.contextSize, temperature: settings.temperature,
          schema: extractionSchema, system: prompts.extraction.system,
          prompt: `${prompts.extraction.instruction}\n\n${modelInput(topic)}` });
      const value = normalizeExtraction(topic, call.result);
      db.exec('BEGIN IMMEDIATE');
      try {
        const result = insertResult.run(topic.topic_id, topic.classification_id, modelRunId, topic.content_hash, settings.provider, settings.model,
          settings.promptVersion, JSON.stringify(call.result), JSON.stringify({ invalid: value.invalid, abstain_reason: value.abstainReason }), Math.round(call.durationMs || 0), nowIso());
        const extractionId = Number(findResult.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion).id);
        const previousStatuses = new Map();
        if (rebuild) {
          const previous = db.prepare(`SELECT v.id viewpoint_id,v.direction,v.review_status,vs.security_id,vs.surface,vs.code_hint
            FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
            WHERE v.extraction_result_id=? AND v.review_status<>'rejected'`).all(extractionId);
          if (previous.some((row) => row.review_status === 'published')) throw new Error('已有发布观点，禁止重建缓存结果');
          if (!rebuildReviewed && previous.some((row) => row.review_status === 'reviewed')) throw new Error('已有人工接受观点，禁止重建缓存结果');
          const previousByViewpoint = new Map();
          for (const row of previous) {
            if (!previousByViewpoint.has(row.viewpoint_id)) previousByViewpoint.set(row.viewpoint_id, { ...row, securityIds: [] });
            if (row.security_id != null) previousByViewpoint.get(row.viewpoint_id).securityIds.push(Number(row.security_id));
          }
          for (const row of previousByViewpoint.values()) {
            const securityIds = [...new Set(row.securityIds)].sort((a, b) => a - b);
            const identity = securityIds.length
              ? `security:${securityIds.join(',')}`
              : `surface:${row.surface.trim().toLocaleLowerCase('zh-CN')}:${String(row.code_hint || '').toUpperCase()}`;
            if (!previousStatuses.has(identity)) previousStatuses.set(identity, []);
            previousStatuses.get(identity).push(row.review_status);
          }
          db.prepare("UPDATE viewpoints SET ordinal=-id WHERE extraction_result_id=? AND review_status='rejected'").run(extractionId);
          db.prepare("DELETE FROM viewpoints WHERE extraction_result_id=? AND review_status='candidate'").run(extractionId);
          if (rebuildReviewed) {
            const reviewed = db.prepare("SELECT * FROM viewpoints WHERE extraction_result_id=? AND review_status='reviewed'").all(extractionId);
            for (const row of reviewed) {
              db.prepare("UPDATE viewpoints SET review_status='rejected',ordinal=-id,updated_at=? WHERE id=?").run(nowIso(), row.id);
              db.prepare("INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)")
                .run(row.id, 'superseded_by_merge', JSON.stringify(row), JSON.stringify({ ...row, review_status: 'rejected' }), '同篇同标的同方向观点合并，保留原审核审计', nowIso());
            }
          }
        }
        const mergedViewpoints = mergeViewpointsBySecurity(value.viewpoints);
        mergedViewpoints.forEach((viewpoint, index) => {
          const resolution = viewpoint.resolution;
          const inserted = insertViewpoint.run(topic.topic_id, extractionId, index + 1, viewpoint.summary, viewpoint.direction, viewpoint.evidence,
            JSON.stringify(viewpoint.supportingEvidence),
            value.recommendation.broker, value.recommendation.group, JSON.stringify(value.recommendation.authors), value.recommendation.evidence,
            viewpoint.targetPrice, viewpoint.targetCurrency, viewpoint.targetRaw, viewpoint.targetEvidence, viewpoint.confidence, nowIso(), nowIso());
          const viewpointId = Number(inserted.lastInsertRowid);
          const priorStatuses = previousStatuses.get(viewpoint.mergeKey) || [];
          if (priorStatuses.length && priorStatuses.every((status) => status === 'reviewed')) {
            db.prepare("UPDATE viewpoints SET review_status='reviewed' WHERE id=?").run(viewpointId);
            db.prepare("INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at) VALUES(?,?,?,?,?,?)")
              .run(viewpointId, 'merge_preserve_review', JSON.stringify({ merged_statuses: priorStatuses }), JSON.stringify({ review_status: 'reviewed' }), '合并前该标的同方向观点均已接受', nowIso());
          }
          const resolvedSecurities = resolution.status === 'resolved' ? (resolution.securities || [resolution.security]).filter(Boolean) : [];
          const links = resolvedSecurities.length ? resolvedSecurities : [null];
          links.forEach((security, linkIndex) => insertLink.run(viewpointId, linkIndex + 1, security?.id || null, viewpoint.surface, viewpoint.codeHint,
            viewpoint.securityEvidence, viewpoint.confidence, resolution.method || null, resolution.status, resolution.status === 'resolved' ? nowIso() : null));
          if (resolution.status !== 'resolved') insertQueue.run(viewpointId, viewpoint.surface, viewpoint.codeHint, viewpoint.securityEvidence,
            JSON.stringify(resolution.candidates), `${viewpoint.surface} 股票 代码 Tushare`, nowIso(), nowIso());
          viewpointCount++;
        });
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      duration += Math.round(call.durationMs || 0);
      db.prepare("INSERT INTO model_attempts(model_run_id,topic_id,stage,status,raw_result_json,created_at) VALUES(?,?,'extraction','success',?,?)")
        .run(modelRunId, topic.topic_id, JSON.stringify(call.result), nowIso());
      success++;
    } catch (error) {
      failed++;
      db.prepare("INSERT INTO model_attempts(model_run_id,topic_id,stage,status,raw_result_json,error,created_at) VALUES(?,?,'extraction','failed',?,?,?)")
        .run(modelRunId, topic.topic_id, call ? JSON.stringify(call.result) : null, error.message, nowIso());
      db.prepare(`INSERT OR IGNORE INTO extraction_results(topic_id,classification_id,model_run_id,content_hash,provider,model_id,prompt_version,status,error,created_at)
        VALUES(?,?,?,?,?,?,?,'failed',?,?)`).run(topic.topic_id, topic.classification_id, modelRunId, topic.content_hash, settings.provider, settings.model, settings.promptVersion, error.message, nowIso());
      console.error(`抽取失败 ${topic.topic_id}: ${error.message}`);
    }
    db.prepare('UPDATE model_runs SET success_count=?,failed_count=?,total_duration_ms=? WHERE id=?').run(success, failed, duration, modelRunId);
  };
  const requestedConcurrency = Number(option('concurrency', '2'));
  const concurrency = Math.max(1, Math.min(3, Number.isFinite(requestedConcurrency) ? requestedConcurrency : 2, pendingTopics.length || 1));
  let nextTopic = 0;
  await Promise.all(Array.from({ length: concurrency }, async () => {
    while (nextTopic < pendingTopics.length) {
      const topic = pendingTopics[nextTopic++];
      await processTopic(topic);
    }
  }));
  db.prepare(`UPDATE model_runs SET status=?,finished_at=?,success_count=?,failed_count=?,total_duration_ms=?,error=? WHERE id=?`)
    .run(failed ? 'failed' : 'success', nowIso(), success, failed, duration, failed ? `${failed} 篇抽取失败` : null, modelRunId);
  if (failed) throw new Error(`${failed} 篇观点抽取失败，已保存成功进度，可重跑续接`);
  const cleanupAt = nowIso();
  db.prepare(`UPDATE security_resolution_queue SET status='dismissed',resolution_source='out_of_scope_or_rejected',updated_at=?
    WHERE status='pending' AND viewpoint_id IN (SELECT id FROM viewpoints WHERE review_status='rejected')`).run(cleanupAt);
  const outOfScopeRejected = db.prepare(`SELECT DISTINCT v.* FROM viewpoints v
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE v.review_status='rejected' AND vs.resolution_status<>'resolved'
      AND vs.surface NOT GLOB '*[^A-Za-z0-9 .&+_/-]*'
      AND coalesce((SELECT re.action FROM review_events re WHERE re.viewpoint_id=v.id ORDER BY re.id DESC LIMIT 1),'')<>'scope_excluded'`).all();
  const markScopeExcluded = db.prepare(`INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at)
    VALUES(?,'scope_excluded',?,?,?,?)`);
  for (const row of outOfScopeRejected) markScopeExcluded.run(row.id, JSON.stringify(row), JSON.stringify(row), '业务范围仅保留A/H股；范围外英文标的仅留原文审计', cleanupAt);
  return { input: topics.length, success, failed, skipped, viewpoints: viewpointCount, model_run_id: modelRunId, duration_ms: duration };
}

function currentTopicViewpoints(topicId) {
  const rows = db.prepare(`SELECT v.* FROM viewpoints v WHERE v.topic_id=? AND v.review_status<>'rejected' ORDER BY v.ordinal`).all(topicId);
  const links = db.prepare(`SELECT vs.*,s.ts_code,s.market FROM viewpoint_securities vs LEFT JOIN securities s ON s.id=vs.security_id
    WHERE vs.viewpoint_id=? ORDER BY vs.link_order`);
  return rows.map((row) => ({ ...row, links: links.all(row.id) }));
}

function approvalFingerprintFromCurrent(viewpoints) {
  return viewpoints.map((viewpoint) => {
    const securityIds = viewpoint.links.filter((link) => link.resolution_status === 'resolved' && link.security_id != null)
      .map((link) => Number(link.security_id)).sort((a, b) => a - b);
    return {
      viewpoint_id: viewpoint.id,
      key: `${securityIds.join(',')}:${viewpoint.direction}`,
      resolved: securityIds.length > 0 && securityIds.length === viewpoint.links.length,
      target: viewpoint.target_price == null ? null : Number(viewpoint.target_price),
      currency: viewpoint.target_currency || null,
      confidence: Number(viewpoint.confidence),
    };
  }).sort((left, right) => left.key.localeCompare(right.key));
}

function approvalFingerprintFromReviewer(viewpoints) {
  return viewpoints.map((viewpoint) => {
    const securities = viewpoint.resolution.status === 'resolved' ? (viewpoint.resolution.securities || [viewpoint.resolution.security]).filter(Boolean) : [];
    const securityIds = securities.map((security) => Number(security.id)).sort((a, b) => a - b);
    return {
      key: `${securityIds.join(',')}:${viewpoint.direction}`,
      resolved: viewpoint.resolution.status === 'resolved' && securityIds.length > 0,
      target: viewpoint.targetPrice == null ? null : Number(viewpoint.targetPrice),
      currency: viewpoint.targetCurrency || null,
      confidence: Number(viewpoint.confidence),
    };
  }).sort((left, right) => left.key.localeCompare(right.key));
}

function compareApproval(topic, reviewerResult) {
  const currentClassification = db.prepare(`SELECT * FROM topic_classifications WHERE topic_id=? ORDER BY created_at DESC,id DESC LIMIT 1`).get(topic.topic_id);
  const currentViewpoints = currentTopicViewpoints(topic.topic_id);
  const reviewerClassification = normalizeClassification(topic, {
    classification: reviewerResult.classification,
    confidence: reviewerResult.classification_confidence,
    evidence: reviewerResult.classification_evidence,
    reasons: [], negative_signals: [],
  });
  const reviewerExtraction = normalizeExtraction(topic, {
    recommendation: reviewerResult.recommendation,
    viewpoints: reviewerResult.viewpoints,
    abstain_reason: null,
  });
  const reviewerViewpoints = mergeViewpointsBySecurity(reviewerExtraction.viewpoints);
  const currentFingerprint = approvalFingerprintFromCurrent(currentViewpoints);
  const reviewerFingerprint = approvalFingerprintFromReviewer(reviewerViewpoints);
  const reasons = [];
  const classificationAgreement = currentClassification?.classification === reviewerClassification.classification;
  if (!classificationAgreement) reasons.push('classification_disagreement');
  if (currentClassification?.classification === 'uncertain' || reviewerClassification.classification === 'uncertain') reasons.push('uncertain_classification');
  if (Number(currentClassification?.confidence || 0) < 0.9 || reviewerClassification.confidence < 0.9) reasons.push('low_classification_confidence');
  if (Number(reviewerResult.overall_confidence || 0) < 0.9) reasons.push('low_overall_confidence');
  if ((reviewerResult.risk_flags || []).length) reasons.push('reviewer_risk_flags');
  if (JSON.stringify(currentFingerprint.map((row) => row.key)) !== JSON.stringify(reviewerFingerprint.map((row) => row.key))) reasons.push('viewpoint_set_disagreement');
  if (currentFingerprint.some((row) => !row.resolved || row.confidence < 0.85) || reviewerFingerprint.some((row) => !row.resolved || row.confidence < 0.85)) reasons.push('viewpoint_gate_failed');
  const currentTargets = currentFingerprint.map((row) => `${row.key}:${row.target ?? ''}:${row.currency ?? ''}`);
  const reviewerTargets = reviewerFingerprint.map((row) => `${row.key}:${row.target ?? ''}:${row.currency ?? ''}`);
  if (JSON.stringify(currentTargets) !== JSON.stringify(reviewerTargets)) reasons.push('target_disagreement');
  const currentBroker = currentViewpoints[0]?.recommendation_broker || null;
  const currentTeam = currentViewpoints[0]?.recommendation_team || '其他';
  if (currentViewpoints.length && (currentBroker !== reviewerExtraction.recommendation.broker
      || currentTeam !== reviewerExtraction.recommendation.group)) reasons.push('recommendation_source_disagreement');
  const reviewerByKey = new Map(reviewerFingerprint.map((row) => [row.key, row]));
  const sharedBlockers = [];
  if (currentViewpoints.length && (currentBroker !== reviewerExtraction.recommendation.broker
      || currentTeam !== reviewerExtraction.recommendation.group)) sharedBlockers.push('recommendation_source_disagreement');
  const viewpointReviews = currentFingerprint.map((current) => {
    const reviewer = reviewerByKey.get(current.key);
    const viewpointReasons = [...sharedBlockers];
    if (!current.resolved) viewpointReasons.push('current_viewpoint_unresolved');
    if (current.confidence < 0.85) viewpointReasons.push('current_viewpoint_low_confidence');
    if (!reviewer) viewpointReasons.push('reviewer_missing_viewpoint');
    else {
      if (!reviewer.resolved) viewpointReasons.push('reviewer_viewpoint_unresolved');
      if (reviewer.confidence < 0.85) viewpointReasons.push('reviewer_viewpoint_low_confidence');
      if (current.target !== reviewer.target || current.currency !== reviewer.currency) viewpointReasons.push('target_disagreement');
    }
    return {
      viewpoint_id: current.viewpoint_id,
      key: current.key,
      decision: viewpointReasons.length ? 'human_review' : 'auto_accept',
      confidence: reviewer ? Math.min(current.confidence, reviewer.confidence) : 0,
      reasons: [...new Set(viewpointReasons)],
    };
  });
  const currentKeys = new Set(currentFingerprint.map((row) => row.key));
  const reviewerOnlyViewpoints = reviewerFingerprint.filter((row) => !currentKeys.has(row.key));
  return {
    decision: reasons.length ? 'human_review' : 'auto_accept',
    confidence: Number(reviewerResult.overall_confidence || 0),
    reasons,
    comparison: {
      current_classification: currentClassification?.classification || null,
      reviewer_classification: reviewerClassification.classification,
      current_viewpoints: currentFingerprint,
      reviewer_viewpoints: reviewerFingerprint,
      current_broker: currentBroker,
      reviewer_broker: reviewerExtraction.recommendation.broker,
      reviewer_risk_flags: reviewerResult.risk_flags || [],
      invalid_reviewer_items: reviewerExtraction.invalid,
      reviewer_only_viewpoints: reviewerOnlyViewpoints,
    },
    currentViewpoints,
    viewpointReviews,
    reviewerOnlyViewpoints,
    currentClassificationId: currentClassification?.id || null,
  };
}

async function approvePilot(workflowRunId) {
  const settings = modelSettings('approval');
  const allTopics = db.prepare(`SELECT rt.*,ps.sample_order FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id ORDER BY ps.sample_order`).all();
  const afterOrder = Number(option('after-order', '0')) || 0;
  const eligibleTopics = allTopics.filter((topic) => topic.sample_order > afterOrder);
  const requestedLimit = Number(option('limit', '0'));
  const topics = requestedLimit > 0 ? eligibleTopics.slice(0, requestedLimit) : eligibleTopics;
  const modelRunId = createModelRun(workflowRunId, 'approval', settings, topics.length);
  const apply = option('apply', command === 'approve-pilot' ? 'true' : 'false') === 'true';
  const reuseOnly = option('reuse-only', 'false') === 'true';
  const existing = db.prepare(`SELECT * FROM ai_approval_results WHERE topic_id=? AND content_hash=? AND provider=? AND model_id=? AND prompt_version=? AND status='success'`);
  const upsert = db.prepare(`INSERT INTO ai_approval_results(topic_id,model_run_id,content_hash,provider,model_id,prompt_version,status,decision,confidence,reasons_json,comparison_json,raw_result_json,applied,error,duration_ms,created_at)
    VALUES(?,?,?,?,?,?,'success',?,?,?,?,?,?,NULL,?,?)
    ON CONFLICT(topic_id,content_hash,provider,model_id,prompt_version) DO UPDATE SET model_run_id=excluded.model_run_id,status='success',decision=excluded.decision,
      confidence=excluded.confidence,reasons_json=excluded.reasons_json,comparison_json=excluded.comparison_json,raw_result_json=excluded.raw_result_json,
      applied=max(ai_approval_results.applied,excluded.applied),error=NULL,duration_ms=excluded.duration_ms,created_at=excluded.created_at`);
  const approvalRow = db.prepare(`SELECT id FROM ai_approval_results WHERE topic_id=? AND content_hash=? AND provider=? AND model_id=? AND prompt_version=?`);
  const upsertViewpointApproval = db.prepare(`INSERT INTO ai_viewpoint_approval_results(approval_result_id,topic_id,viewpoint_id,decision,confidence,reasons_json,created_at)
    VALUES(?,?,?,?,?,?,?) ON CONFLICT(approval_result_id,viewpoint_id) DO UPDATE SET decision=excluded.decision,confidence=excluded.confidence,
      reasons_json=excluded.reasons_json,created_at=excluded.created_at`);
  let success = 0; let failed = 0; let skipped = 0; let duration = 0; let autoAccept = 0; let humanReview = 0;
  const recordFailure = (topic, error) => {
    failed++;
    db.prepare(`INSERT INTO ai_approval_results(topic_id,model_run_id,content_hash,provider,model_id,prompt_version,status,reasons_json,error,created_at)
      VALUES(?,?,?,?,?,?,'failed','[]',?,?) ON CONFLICT(topic_id,content_hash,provider,model_id,prompt_version)
      DO UPDATE SET status='failed',error=excluded.error,created_at=excluded.created_at`).run(topic.topic_id, modelRunId, topic.content_hash,
      settings.provider, settings.model, settings.promptVersion, error.message, nowIso());
  };
  const recordSuccess = (topic, rawResult, elapsed = 0) => {
    const review = compareApproval(topic, rawResult);
    if (review.decision === 'auto_accept') autoAccept++; else humanReview++;
    const when = nowIso();
    db.exec('BEGIN IMMEDIATE');
    try {
      const acceptedViewpointIds = new Set(review.viewpointReviews.filter((row) => row.decision === 'auto_accept').map((row) => row.viewpoint_id));
      upsert.run(topic.topic_id, modelRunId, topic.content_hash, settings.provider, settings.model, settings.promptVersion,
        review.decision, review.confidence, JSON.stringify(review.reasons), JSON.stringify(review.comparison), JSON.stringify(rawResult),
        Number(apply && acceptedViewpointIds.size > 0), elapsed, when);
      const approvalId = approvalRow.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion).id;
      for (const viewpointReview of review.viewpointReviews) {
        upsertViewpointApproval.run(approvalId, topic.topic_id, viewpointReview.viewpoint_id, viewpointReview.decision,
          viewpointReview.confidence, JSON.stringify(viewpointReview.reasons), when);
      }
      if (apply) {
        for (const viewpoint of review.currentViewpoints.filter((row) => row.review_status === 'candidate' && acceptedViewpointIds.has(row.id))) {
          const before = { ...viewpoint }; delete before.links;
          db.prepare("UPDATE viewpoints SET review_status='reviewed',updated_at=? WHERE id=?").run(when, viewpoint.id);
          db.prepare("INSERT INTO review_events(viewpoint_id,action,before_json,after_json,note,created_at) VALUES(?,'ai_accept',?,?,?,?)")
            .run(viewpoint.id, JSON.stringify(before), JSON.stringify({ ...before, review_status: 'reviewed' }), '该观点独立AI二审一致，逐条质量门禁通过', when);
        }
        const remainingCandidates = Number(db.prepare("SELECT count(*) n FROM viewpoints WHERE topic_id=? AND review_status='candidate'").get(topic.topic_id).n);
        if (remainingCandidates === 0 && review.reviewerOnlyViewpoints.length === 0 && review.currentClassificationId) {
          db.prepare("UPDATE topic_classifications SET review_status='reviewed' WHERE id=?").run(review.currentClassificationId);
        }
      }
      db.exec('COMMIT');
    } catch (error) { db.exec('ROLLBACK'); throw error; }
    success++;
  };
  const processTopic = async (topic) => {
    try {
      let stored = existing.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion);
      let rawResult; let elapsed = 0;
      if (stored) { rawResult = JSON.parse(stored.raw_result_json); skipped++; }
      else if (reuseOnly) { skipped++; return; }
      else {
        const call = await callStructuredModel({ provider: settings.provider, model: settings.model, contextSize: settings.contextSize,
          temperature: settings.temperature, schema: approvalSchema, system: prompts.approval.system,
          prompt: `${prompts.approval.instruction}\n\n${modelInput(topic)}` });
        rawResult = call.result; elapsed = Math.round(call.durationMs || 0); duration += elapsed;
      }
      recordSuccess(topic, rawResult, elapsed);
    } catch (error) {
      recordFailure(topic, error);
    }
    db.prepare('UPDATE model_runs SET success_count=?,failed_count=?,total_duration_ms=? WHERE id=?').run(success, failed, duration, modelRunId);
  };
  const concurrency = Math.max(1, Math.min(2, Number(option('concurrency', '2')) || 2));
  const batchSize = settings.provider === 'codex' ? Math.max(1, Math.min(12, Number(option('batch-size', '10')) || 10)) : 1;
  if (batchSize === 1) {
    let nextTopic = 0;
    await Promise.all(Array.from({ length: concurrency }, async () => {
      while (nextTopic < topics.length) await processTopic(topics[nextTopic++]);
    }));
  } else {
    const pending = [];
    for (const topic of topics) {
      const stored = existing.get(topic.topic_id, topic.content_hash, settings.provider, settings.model, settings.promptVersion);
      if (stored) {
        skipped++;
        try { recordSuccess(topic, JSON.parse(stored.raw_result_json), 0); } catch (error) { recordFailure(topic, error); }
      } else pending.push(topic);
    }
    if (reuseOnly && pending.length) {
      skipped += pending.length;
      pending.length = 0;
    }
    const batches = [];
    for (let index = 0; index < pending.length; index += batchSize) batches.push(pending.slice(index, index + batchSize));
    let nextBatch = 0;
    await Promise.all(Array.from({ length: concurrency }, async () => {
      while (nextBatch < batches.length) {
        const batch = batches[nextBatch++];
        const batchSchema = {
          type: 'object', additionalProperties: false, required: ['results'],
          properties: { results: { type: 'array', minItems: batch.length, maxItems: batch.length, items: {
            ...approvalSchema,
            required: ['topic_id', ...approvalSchema.required],
            properties: { topic_id: { type: 'string' }, ...approvalSchema.properties },
          } } },
        };
        try {
          const call = await callStructuredModel({ provider: settings.provider, model: settings.model, contextSize: settings.contextSize,
            temperature: settings.temperature, maxAttempts: 1, schema: batchSchema, system: prompts.approval.system,
            prompt: `${prompts.approval.instruction}\n\n请分别审核以下 ${batch.length} 篇文本，每篇必须返回且 topic_id 原样复制。所有 evidence 字段只能逐字复制对应原文中的连续片段，不得包含“标题：”“正文：”等输入标签，不得改写。\n\n${batch.map((topic) => `===== topic_id: ${topic.topic_id} =====\n${modelInput(topic)}`).join('\n\n')}` });
          duration += Math.round(call.durationMs || 0);
          const byId = new Map((call.result.results || []).map((result) => [String(result.topic_id), result]));
          for (const topic of batch) {
            const raw = byId.get(String(topic.topic_id));
            if (!raw) { recordFailure(topic, new Error('批量二审结果缺少 topic_id')); continue; }
            const { topic_id: ignored, ...rawResult } = raw;
            try { recordSuccess(topic, rawResult, Math.round((call.durationMs || 0) / batch.length)); }
            catch (error) { recordFailure(topic, error); }
          }
        } catch (error) {
          for (const topic of batch) recordFailure(topic, error);
        }
        db.prepare('UPDATE model_runs SET success_count=?,failed_count=?,total_duration_ms=? WHERE id=?').run(success, failed, duration, modelRunId);
      }
    }));
  }
  db.prepare(`UPDATE model_runs SET status=?,finished_at=?,success_count=?,failed_count=?,total_duration_ms=?,error=? WHERE id=?`)
    .run(failed ? 'failed' : 'success', nowIso(), success, failed, duration, failed ? `${failed} 篇AI二审失败` : null, modelRunId);
  return { input: topics.length, success, failed, skipped, auto_accept: autoAccept, human_review: humanReview, applied: apply, model_run_id: modelRunId, duration_ms: duration };
}

function validateUnifiedPilot() {
  const sampleCount = Number(db.prepare('SELECT count(*) n FROM pilot_samples').get().n);
  const classifications = Number(db.prepare(`SELECT count(*) n FROM pilot_samples ps WHERE EXISTS(
    SELECT 1 FROM topic_classifications tc JOIN raw_topics rt ON rt.topic_id=tc.topic_id WHERE tc.topic_id=ps.topic_id AND tc.content_hash=rt.content_hash)`).get().n);
  const requiredExtractions = Number(db.prepare(`SELECT count(*) n FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id
    JOIN topic_classifications tc ON tc.id=(SELECT id FROM topic_classifications t WHERE t.topic_id=rt.topic_id AND t.content_hash=rt.content_hash ORDER BY created_at DESC,id DESC LIMIT 1)
    WHERE tc.classification IN ('broker_note','uncertain')`).get().n);
  const successfulExtractions = Number(db.prepare(`SELECT count(*) n FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id
    JOIN topic_classifications tc ON tc.id=(SELECT id FROM topic_classifications t WHERE t.topic_id=rt.topic_id AND t.content_hash=rt.content_hash ORDER BY created_at DESC,id DESC LIMIT 1)
    WHERE tc.classification IN ('broker_note','uncertain') AND EXISTS(SELECT 1 FROM extraction_results er
      WHERE er.topic_id=rt.topic_id AND er.content_hash=rt.content_hash AND er.status='success')`).get().n);
  const activeScope = `FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id WHERE v.review_status<>'rejected'`;
  const viewpoints = Number(db.prepare(`SELECT count(*) n ${activeScope}`).get().n);
  const resolved = Number(db.prepare(`SELECT count(DISTINCT v.id) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id WHERE v.review_status<>'rejected' AND vs.resolution_status='resolved'`).get().n);
  const badEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    JOIN raw_topics rt ON rt.topic_id=v.topic_id WHERE v.review_status<>'rejected' AND instr(rt.title||char(10)||rt.content,v.evidence)=0`).get().n);
  const badSupportingEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    JOIN raw_topics rt ON rt.topic_id=v.topic_id JOIN json_each(v.supporting_evidence_json) je
    WHERE v.review_status<>'rejected' AND instr(rt.title||char(10)||rt.content,je.value)=0`).get().n);
  const badSecurityEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    JOIN raw_topics rt ON rt.topic_id=v.topic_id JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE v.review_status<>'rejected' AND instr(rt.title||char(10)||rt.content,vs.evidence)=0`).get().n);
  const duplicateAtomic = Number(db.prepare(`SELECT count(*) n FROM (SELECT v.topic_id,
    CASE WHEN vs.security_id IS NOT NULL THEN 'id:'||vs.security_id ELSE 'surface:'||lower(vs.surface)||':'||coalesce(vs.code_hint,'') END security_key,
    count(*) c FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE v.review_status<>'rejected' GROUP BY v.topic_id,security_key HAVING c>1)`).get().n);
  const illegalMarkets = Number(db.prepare(`SELECT count(*) n FROM viewpoint_securities vs JOIN viewpoints v ON v.id=vs.viewpoint_id
    JOIN pilot_samples ps ON ps.topic_id=v.topic_id JOIN securities s ON s.id=vs.security_id
    WHERE v.review_status<>'rejected' AND s.market NOT IN ('SH','SZ','BJ','HK')`).get().n);
  const pendingSecurity = Number(db.prepare(`SELECT count(*) n FROM security_resolution_queue q JOIN viewpoints v ON v.id=q.viewpoint_id
    JOIN pilot_samples ps ON ps.topic_id=v.topic_id WHERE q.status='pending' AND v.review_status<>'rejected'`).get().n);
  const approval = db.prepare(`SELECT
    sum(CASE WHEN ar.status='success' AND ar.decision='auto_accept' THEN 1 ELSE 0 END) auto_accept,
    sum(CASE WHEN ar.status='success' AND ar.decision='human_review' THEN 1 ELSE 0 END) human_review,
    sum(CASE WHEN ar.status='failed' THEN 1 ELSE 0 END) failed,
    count(ar.id) accounted
    FROM pilot_samples ps JOIN raw_topics rt ON rt.topic_id=ps.topic_id
    LEFT JOIN ai_approval_results ar ON ar.id=(SELECT a.id FROM ai_approval_results a
      WHERE a.topic_id=ps.topic_id AND a.content_hash=rt.content_hash ORDER BY a.id DESC LIMIT 1)
    WHERE ps.sample_order>30`).get();
  const candidateViewpoints = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    WHERE ps.sample_order>30 AND v.review_status='candidate'`).get().n);
  const reviewedViewpoints = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    WHERE ps.sample_order>30 AND v.review_status='reviewed'`).get().n);
  const viewpointApproval = db.prepare(`SELECT
    sum(CASE WHEN avr.decision='auto_accept' THEN 1 ELSE 0 END) auto_accept,
    sum(CASE WHEN avr.decision='human_review' THEN 1 ELSE 0 END) human_review
    FROM ai_viewpoint_approval_results avr JOIN pilot_samples ps ON ps.topic_id=avr.topic_id WHERE ps.sample_order>30`).get();
  const automatedPass = sampleCount === config.pilot.size && classifications === sampleCount
    && Number(approval.accounted || 0) === sampleCount - 30 && Number(approval.failed || 0) === 0
    && successfulExtractions === requiredExtractions && badEvidence === 0 && badSupportingEvidence === 0
    && badSecurityEvidence === 0 && duplicateAtomic === 0 && illegalMarkets === 0;
  const report = {
    generated_at: nowIso(), status: automatedPass ? 'awaiting_human_review' : 'failed',
    counts: {
      sample_count: sampleCount, classifications, required_extractions: requiredExtractions,
      successful_extractions: successfulExtractions, extraction_failures: requiredExtractions - successfulExtractions,
      viewpoints, resolved, unresolved: viewpoints - resolved, pending_security_resolution: pendingSecurity,
      ai_auto_accept_topics: Number(approval.auto_accept || 0), ai_human_review_topics: Number(approval.human_review || 0),
      ai_failed_topics: Number(approval.failed || 0), candidate_viewpoints: candidateViewpoints, reviewed_viewpoints: reviewedViewpoints,
      ai_auto_accept_viewpoints: Number(viewpointApproval.auto_accept || 0), ai_human_review_viewpoints: Number(viewpointApproval.human_review || 0),
    },
    automated_gates: {
      complete_classification: classifications === sampleCount,
      ai_all_accounted: Number(approval.accounted || 0) === sampleCount - 30,
      bad_evidence: badEvidence, bad_supporting_evidence: badSupportingEvidence, bad_security_evidence: badSecurityEvidence,
      duplicate_atomic: duplicateAtomic, illegal_markets: illegalMarkets,
    },
    human_gates: { approved: false, baseline_sample_size: 30, baseline_approved: true },
    next_action: automatedPass
      ? '扩展试跑自动门禁完成；请在入库管理页复核剩余候选观点、待解析证券和1篇AI二审失败文本。'
      : '自动门禁存在失败项，先修复后再进入人工审核。',
  };
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), 'utf8');
  writeFileSync(REPORT_MD, `# ${sampleCount}篇全自动扩展试跑报告\n\n生成时间：${report.generated_at}\n\n- 分类：${classifications}/${sampleCount}\n- 应提取文本：${requiredExtractions}\n- 提取成功：${successfulExtractions}\n- 提取失败并留人工：${requiredExtractions - successfulExtractions}\n- 有效观点：${viewpoints}\n- 已匹配证券：${resolved}\n- 待解析证券：${pendingSecurity}\n- AI自动接受主题：${report.counts.ai_auto_accept_topics}\n- AI转人工主题：${report.counts.ai_human_review_topics}\n- AI失败并转人工：${report.counts.ai_failed_topics}\n- 逐条AI自动接受观点：${report.counts.ai_auto_accept_viewpoints}\n- 逐条AI保留人工观点：${report.counts.ai_human_review_viewpoints}\n- 当前候选观点：${candidateViewpoints}\n- 当前已接受观点：${reviewedViewpoints}\n- 证据错误：${badEvidence + badSupportingEvidence + badSecurityEvidence}\n- 同篇重复标的：${duplicateAtomic}\n- 非法市场：${illegalMarkets}\n\n## 下一步\n\n${report.next_action}\n`, 'utf8');
  return report;
}

function validatePilot() {
  return validateUnifiedPilot();
  /* Legacy single-provider validation retained below for historical report compatibility. */
  const classificationSettings = modelSettings('classification');
  const extractionSettings = modelSettings('extraction');
  const sampleCount = Number(db.prepare('SELECT count(*) n FROM pilot_samples').get().n);
  const classifications = Number(db.prepare(`SELECT count(*) n FROM pilot_samples ps WHERE EXISTS(
    SELECT 1 FROM topic_classifications tc WHERE tc.topic_id=ps.topic_id AND tc.provider=? AND tc.model_id=? AND tc.prompt_version=?)`)
    .get(classificationSettings.provider, classificationSettings.model, classificationSettings.promptVersion).n);
  const extractionFailures = Number(db.prepare(`SELECT count(*) n FROM extraction_results er JOIN pilot_samples ps ON ps.topic_id=er.topic_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='failed'`)
    .get(extractionSettings.provider, extractionSettings.model, extractionSettings.promptVersion).n);
  const viewpointScope = `FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'`;
  const scopeParams = [extractionSettings.provider, extractionSettings.model, extractionSettings.promptVersion];
  const viewpoints = Number(db.prepare(`SELECT count(*) n ${viewpointScope}`).get(...scopeParams).n);
  const resolved = Number(db.prepare(`SELECT count(DISTINCT v.id) n FROM viewpoint_securities vs JOIN viewpoints v ON v.id=vs.viewpoint_id
    JOIN extraction_results er ON er.id=v.extraction_result_id JOIN pilot_samples ps ON ps.topic_id=v.topic_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected' AND vs.resolution_status='resolved'`)
    .get(...scopeParams).n);
  const unresolved = viewpoints - resolved;
  const badEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN raw_topics rt ON rt.topic_id=v.topic_id WHERE er.provider=? AND er.model_id=? AND er.prompt_version=?
    AND er.status='success' AND v.review_status<>'rejected' AND instr(rt.title||char(10)||rt.content,v.evidence)=0`).get(...scopeParams).n);
  const badSecurityEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoint_securities vs JOIN viewpoints v ON v.id=vs.viewpoint_id
    JOIN extraction_results er ON er.id=v.extraction_result_id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
    AND instr(rt.title||char(10)||rt.content,vs.evidence)=0`).get(...scopeParams).n);
  const badSupportingEvidence = Number(db.prepare(`SELECT count(*) n FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN raw_topics rt ON rt.topic_id=v.topic_id JOIN json_each(v.supporting_evidence_json) je
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
    AND instr(rt.title||char(10)||rt.content,je.value)=0`).get(...scopeParams).n);
  const duplicateAtomic = Number(db.prepare(`SELECT count(*) n FROM (SELECT v.topic_id,
    CASE WHEN vs.security_id IS NOT NULL THEN 'id:'||vs.security_id ELSE 'surface:'||lower(vs.surface)||':'||coalesce(vs.code_hint,'') END security_key,
    count(*) c FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id WHERE er.provider=? AND er.model_id=? AND er.prompt_version=?
    AND er.status='success' AND v.review_status<>'rejected' GROUP BY v.topic_id,security_key HAVING c>1)`).get(...scopeParams).n);
  const defaultAResolvableQueued = Number(db.prepare(`SELECT count(DISTINCT v.id) n
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND vs.resolution_status<>'resolved' AND vs.code_hint IS NULL
      AND (SELECT count(DISTINCT s.id) FROM security_aliases a JOIN securities s ON s.id=a.security_id
        WHERE a.alias=vs.surface AND s.market IN ('SH','SZ','BJ'))=1`).get(...scopeParams).n);
  const defaultAMarketViolations = Number(db.prepare(`SELECT count(DISTINCT v.id) n
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN securities s ON s.id=vs.security_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND s.market='HK' AND coalesce(upper(vs.code_hint),'') NOT LIKE '%.HK'
      AND coalesce(vs.resolution_method,'')<>'tushare_alias_hk_fallback'
      AND upper(trim(vs.surface)) NOT LIKE '%(H)' AND upper(trim(vs.surface)) NOT LIKE '%（H）'
      AND upper(trim(vs.surface)) NOT LIKE '% H'
      AND upper(trim(vs.surface)) NOT LIKE '% AH' AND upper(trim(vs.surface)) NOT LIKE '%(AH)' AND upper(trim(vs.surface)) NOT LIKE '%（AH）'`).get(...scopeParams).n);
  const genericSecuritySurfaces = Number(db.prepare(`SELECT count(DISTINCT v.id) n
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND vs.surface IN ('A股','港股','H股','美股','个股','公司','标的','受益标的','相关标的','板块','行业')`).get(...scopeParams).n);
  const securitySurfaceRows = db.prepare(`SELECT DISTINCT v.id,vs.surface,rt.title,rt.content
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id JOIN raw_topics rt ON rt.topic_id=v.topic_id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND instr(rt.title||char(10)||rt.content,vs.surface)=0`).all(...scopeParams);
  const securitySurfaceNotInOriginal = securitySurfaceRows.filter((row) => !securitySurfaceExists(row, row.surface)).length;
  const outOfScopeLatinUnresolved = Number(db.prepare(`SELECT count(DISTINCT v.id) n
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND vs.resolution_status<>'resolved' AND vs.surface NOT GLOB '*[^A-Za-z0-9 .&+_/-]*'`).get(...scopeParams).n);
  const rejectedPendingQueue = Number(db.prepare(`SELECT count(*) n FROM security_resolution_queue q
    JOIN viewpoints v ON v.id=q.viewpoint_id WHERE q.status='pending' AND v.review_status='rejected'`).get().n);
  const uniqueHkResolvableQueued = Number(db.prepare(`SELECT count(DISTINCT v.id) n
    FROM viewpoints v JOIN extraction_results er ON er.id=v.extraction_result_id
    JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
    WHERE er.provider=? AND er.model_id=? AND er.prompt_version=? AND er.status='success' AND v.review_status<>'rejected'
      AND vs.resolution_status<>'resolved' AND vs.code_hint IS NULL
      AND (SELECT count(DISTINCT s.id) FROM security_aliases a JOIN securities s ON s.id=a.security_id
        WHERE a.alias=vs.surface AND s.market IN ('SH','SZ','BJ'))=0
      AND (SELECT count(DISTINCT s.id) FROM security_aliases a JOIN securities s ON s.id=a.security_id
        WHERE a.alias=vs.surface AND s.market='HK')=1`).get(...scopeParams).n);
  const humanReviewRow = db.prepare("SELECT value FROM workflow_settings WHERE key='pilot_human_review'").get();
  let humanReview = null;
  try { humanReview = humanReviewRow ? JSON.parse(humanReviewRow.value) : null; } catch {}
  const humanApprovalCoversSample = Boolean(humanReview?.approved && Number(humanReview?.sample_size || 30) === sampleCount);
  const automatedPass = sampleCount === config.pilot.size && classifications === sampleCount && extractionFailures === 0
    && badEvidence === 0 && badSecurityEvidence === 0 && badSupportingEvidence === 0 && duplicateAtomic === 0
    && defaultAResolvableQueued === 0 && defaultAMarketViolations === 0 && genericSecuritySurfaces === 0
    && securitySurfaceNotInOriginal === 0 && outOfScopeLatinUnresolved === 0 && rejectedPendingQueue === 0
    && uniqueHkResolvableQueued === 0;
  const report = {
    generated_at: nowIso(),
    status: automatedPass ? (humanApprovalCoversSample ? 'approved' : 'awaiting_human_review') : 'failed',
    counts: { sample_count: sampleCount, classifications, viewpoints, resolved, unresolved, extraction_failures: extractionFailures },
    automated_gates: { complete_classification: classifications === sampleCount, extraction_failures: extractionFailures, bad_evidence: badEvidence, bad_supporting_evidence: badSupportingEvidence, bad_security_evidence: badSecurityEvidence, duplicate_atomic: duplicateAtomic, default_a_resolvable_queued: defaultAResolvableQueued, default_a_market_violations: defaultAMarketViolations, generic_security_surfaces: genericSecuritySurfaces, security_surface_not_in_original: securitySurfaceNotInOriginal, out_of_scope_latin_unresolved: outOfScopeLatinUnresolved, rejected_pending_queue: rejectedPendingQueue, unique_hk_resolvable_queued: uniqueHkResolvableQueued },
    human_gates: humanReview || { approved: false, obvious_broker_note_misses: null, security_accuracy: null, direction_accuracy: null, inferred_brokers: null },
    next_action: automatedPass && humanApprovalCoversSample
      ? `${sampleCount}篇人工验收已通过。`
      : '自动门禁完成；仅需在入库管理页复核AI标记的高度存疑项。',
  };
  mkdirSync(dirname(REPORT_JSON), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), 'utf8');
  writeFileSync(REPORT_MD, `# 30篇端到端试跑质量报告\n\n生成时间：${report.generated_at}\n\n## 自动门禁\n\n- 样本：${sampleCount}/${config.pilot.size}\n- 已分类：${classifications}\n- 合并后观点：${viewpoints}\n- 已匹配证券：${resolved}\n- 待解析证券：${unresolved}\n- 抽取失败：${extractionFailures}\n- 主证据错误：${badEvidence}\n- 支持证据错误：${badSupportingEvidence}\n- 标的证据错误：${badSecurityEvidence}\n- 同篇同标的同方向重复：${duplicateAtomic}\n- 可由唯一 A 股精确匹配却仍在待解析：${defaultAResolvableQueued}\n- 无 A/H 标记却错误匹配到港股：${defaultAMarketViolations}\n- 唯一港股可回退匹配却仍在待解析：${uniqueHkResolvableQueued}\n- 泛称被当成证券标的：${genericSecuritySurfaces}\n- 标的名称不在原文：${securitySurfaceNotInOriginal}\n- 范围外纯英文未解析标的：${outOfScopeLatinUnresolved}\n- 已驳回但仍占用待解析队列：${rejectedPendingQueue}\n\n## 人工验收\n\n- 状态：${humanReview?.approved ? '通过' : '待验收'}\n- 已接受观点：${humanReview?.accepted_viewpoints ?? '待填写'}\n- 合理不结构化的小作文：${humanReview?.acceptable_unstructured_topics ?? '待填写'}\n- 结论：${humanReview?.note || '待填写'}\n\n## 当前结论\n\n${report.next_action}\n`, 'utf8');
  return report;
}

function getStatus() {
  const one = (sql, ...params) => Number(db.prepare(sql).get(...params)?.n || 0);
  return {
    database: DB_PATH,
    schema_version: db.prepare("SELECT value FROM schema_meta WHERE key='schema_version'").get()?.value,
    raw_topics: one('SELECT count(*) n FROM raw_topics'),
    eligible_topics: one("SELECT count(*) n FROM raw_topics WHERE eligibility='eligible'"),
    excluded_topics: one("SELECT count(*) n FROM raw_topics WHERE eligibility='excluded'"),
    pilot_samples: one('SELECT count(*) n FROM pilot_samples'),
    classifications: one('SELECT count(DISTINCT topic_id) n FROM topic_classifications'),
    candidate_viewpoints: one("SELECT count(*) n FROM viewpoints WHERE review_status='candidate'"),
    reviewed_viewpoints: one("SELECT count(*) n FROM viewpoints WHERE review_status='reviewed'"),
    published_viewpoints: one("SELECT count(*) n FROM viewpoints WHERE review_status='published'"),
    pending_security_resolution: one("SELECT count(*) n FROM security_resolution_queue q JOIN viewpoints v ON v.id=q.viewpoint_id WHERE q.status='pending' AND v.review_status<>'rejected'"),
    ai_auto_accepted_topics: one("SELECT count(*) n FROM ai_approval_results WHERE status='success' AND decision='auto_accept'"),
    ai_human_review_topics: one("SELECT count(*) n FROM ai_approval_results WHERE status='success' AND decision='human_review'"),
    securities: one('SELECT count(*) n FROM securities'),
    latest_run: db.prepare('SELECT * FROM workflow_runs ORDER BY id DESC LIMIT 1').get() || null,
    latest_ingestion: db.prepare('SELECT * FROM ingestion_runs ORDER BY id DESC LIMIT 1').get() || null,
    latest_model_runs: db.prepare('SELECT * FROM model_runs ORDER BY id DESC LIMIT 4').all(),
    report: existsSync(REPORT_JSON) ? JSON.parse(readFileSync(REPORT_JSON, 'utf8')) : null,
  };
}

async function runPilot(workflowRunId) {
  const securityCount = Number(db.prepare('SELECT count(*) n FROM securities').get().n);
  const sync = securityCount ? { input: securityCount, success: securityCount, skipped: securityCount } : await syncSecurities();
  const classification = await classifyPilot(workflowRunId);
  const extraction = await extractPilot(workflowRunId);
  const report = validatePilot();
  return { input: config.pilot.size, success: classification.success + classification.skipped, failed: classification.failed + extraction.failed,
    securities: sync.success, classification, extraction, report };
}

let output;
try {
  if (command === 'init') output = { database: DB_PATH, schema: 'broker-note-v2' };
  else if (command === 'fetch') output = await withinRun('fetch', fetchAndIngest);
  else if (command === 'sync-securities') output = await withinRun('sync-securities', syncSecurities);
  else if (command === 'classify-pilot') output = await withinRun('classify-pilot', classifyPilot);
  else if (command === 'extract-pilot') output = await withinRun('extract-pilot', extractPilot);
  else if (command === 'approve-pilot') output = await withinRun('approve-pilot', approvePilot);
  else if (command === 'pilot') output = await withinRun('pilot', runPilot);
  else if (command === 'validate') output = validatePilot();
  else if (command === 'recover') {
    const when = nowIso();
    const workflows = db.prepare("UPDATE workflow_runs SET status='cancelled',finished_at=?,error=coalesce(error,'进程中断') WHERE status='running'").run(when).changes;
    const models = db.prepare("UPDATE model_runs SET status='cancelled',finished_at=?,error=coalesce(error,'进程中断') WHERE status='running'").run(when).changes;
    output = { recovered_workflow_runs: Number(workflows), recovered_model_runs: Number(models) };
  }
  else if (command === 'all') output = await withinRun('all', async (runId) => ({ fetch: await fetchAndIngest(runId), pilot: await runPilot(runId) }));
  else if (command === 'status') output = getStatus();
  else throw new Error(`未知命令: ${command}`);
  console.log(JSON.stringify(output, null, 2));
} finally {
  db.close();
}
