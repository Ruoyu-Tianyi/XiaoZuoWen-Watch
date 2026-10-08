import { createHash } from 'node:crypto';
import { execFileSync, spawn } from 'node:child_process';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { DatabaseSync } from 'node:sqlite';
import { loadPipelineConfig } from './lib/direction_rules.mjs';
import { pipelineVersion } from './lib/pipeline_version.mjs';
import {
  AI_PROMPT_VERSION,
  buildSecurityIndex,
  compareVerified,
  evidenceExists,
  normalizeBroker,
  securityKey,
  shouldAutoAcceptDirection,
  shouldAutoAcceptMention,
  shouldAutoAcceptSource,
  validateTopicResult,
} from './lib/ai_enrichment.mjs';
import {
  VIEWPOINT_PROMPT_VERSION,
  compareViewpointEntities,
  flattenViewpointEntities,
  splitViewpointBlocks,
  validateViewpointVerifierResult,
  validateViewpointTopicResult,
} from './lib/ai_viewpoint_enrichment.mjs';

const command = process.argv[2] || 'run';
const option = (name, fallback = null) => process.argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
const hasFlag = (name) => process.argv.includes(`--${name}`);
const ROOT = process.cwd();
const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const CANDIDATE_PATH = resolve(process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite');
const MANIFEST_PATH = resolve(process.env.PIPELINE_MANIFEST || 'data/pipeline_candidate.json');
const MASTER_PATH = resolve(process.env.SECURITY_MASTER || 'data/security_master.json');
const SCHEMA_PATH = resolve('config/ai_enrichment_schema.json');
const VIEWPOINT_SCHEMA_PATH = resolve('config/ai_viewpoint_schema.json');
const VIEWPOINT_VERIFY_SCHEMA_PATH = resolve('config/ai_viewpoint_verify_schema.json');
const REPORT_JSON = resolve('reports/ai_enrichment.json');
const REPORT_MD = resolve('reports/ai_enrichment.md');
const PROVIDER = option('provider', process.env.AI_ENRICHMENT_PROVIDER || 'codex');
const MODEL = option('model', PROVIDER === 'ollama' ? (process.env.OLLAMA_AI_MODEL || 'qwen3.5:9b') : (process.env.CODEX_AI_MODEL || 'gpt-5.6-terra'));
const REASONING_EFFORT = option('effort', process.env.CODEX_AI_EFFORT || 'low');
const VIEWPOINT_REQUESTED = hasFlag('viewpoints');
const MAX_TOPICS = Number(option('batch-topics', VIEWPOINT_REQUESTED ? '6' : '80'));
const MAX_CHARS = Number(option('batch-chars', VIEWPOINT_REQUESTED ? '16000' : '60000'));
const VERIFY_MAX_TOPICS = Number(option('verify-batch-topics', VIEWPOINT_REQUESTED ? '10' : String(MAX_TOPICS)));
const VERIFY_MAX_CHARS = Number(option('verify-batch-chars', VIEWPOINT_REQUESTED ? '25000' : String(MAX_CHARS)));
const LIMIT = Number(option('limit', '0'));
const PILOT = Number(option('pilot', '0'));
const TARGETS_ONLY = hasFlag('targets-only');
const MISSING_ONLY = hasFlag('missing-only');
const AUDIT_AI = hasFlag('audit-ai');
const ADJUDICATE_AI = hasFlag('adjudicate-ai');
const VIEWPOINT_MODE = hasFlag('viewpoints');
const AUTHORITATIVE = hasFlag('authoritative');
const CHANGED_ONLY = hasFlag('changed-only');
const PROMPT_VERSION = VIEWPOINT_MODE ? VIEWPOINT_PROMPT_VERSION : ADJUDICATE_AI ? 'gpt-target-adjudicate-v1' : TARGETS_ONLY ? 'gpt-targets-v1' : AI_PROMPT_VERSION;
const qualityAudit = ADJUDICATE_AI ? JSON.parse(readFileSync(resolve('reports/ai_quality_audit.json'), 'utf8')) : null;
const adjudicationCandidates = new Map();
for (const row of qualityAudit?.disagreements || []) {
  const topicId = String(row.topic_id);
  if (!adjudicationCandidates.has(topicId)) adjudicationCandidates.set(topicId, []);
  adjudicationCandidates.get(topicId).push(row);
}
const config = loadPipelineConfig();
const version = pipelineVersion(config);
const master = JSON.parse(readFileSync(MASTER_PATH, 'utf8'));
const securityIndex = buildSecurityIndex(master);
let isolatedCodexHome = null;
let codexAppServer = null;

function now() { return new Date().toISOString(); }
function sqlPath(path) { return path.replace(/'/g, "''"); }
function ensureColumn(db, table, name, definition) {
  const exists = db.prepare(`PRAGMA table_info(${table})`).all().some((column) => column.name === name);
  if (!exists) db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
}

export function ensureAiSchema(db) {
  ensureColumn(db, 'mentions', 'recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
  ensureColumn(db, 'mentions', 'recognition_model', "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'mentions', 'recognition_confidence', 'REAL');
  ensureColumn(db, 'mentions', 'is_mixed', 'INTEGER NOT NULL DEFAULT 0');
  ensureColumn(db, 'topics', 'source_recognition_method', "TEXT NOT NULL DEFAULT 'rule'");
  ensureColumn(db, 'topics', 'source_recognition_model', "TEXT NOT NULL DEFAULT ''");
  ensureColumn(db, 'topics', 'source_recognition_confidence', 'REAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS ai_enrichment_runs (
      id INTEGER PRIMARY KEY, provider TEXT NOT NULL, model TEXT NOT NULL, prompt_version TEXT NOT NULL,
      status TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT,
      total_topics INTEGER NOT NULL, completed_topics INTEGER NOT NULL DEFAULT 0,
      failed_topics INTEGER NOT NULL DEFAULT 0, batch_count INTEGER NOT NULL DEFAULT 0,
      error TEXT
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
    CREATE INDEX IF NOT EXISTS idx_ai_results_run ON ai_enrichment_results(run_id,status,phase);
    CREATE INDEX IF NOT EXISTS idx_model_viewpoints_topic ON model_viewpoints(topic_id,ordinal);
    CREATE INDEX IF NOT EXISTS idx_model_viewpoint_mentions_security ON model_viewpoint_mentions(security_id,direction);
    CREATE INDEX IF NOT EXISTS idx_security_resolution_status ON security_resolution_queue(status,confidence);
  `);
  ensureColumn(db, 'model_viewpoints', 'source_block_ordinals', "TEXT NOT NULL DEFAULT '[]'");
}

function locateCodex() {
  let pathEntries = [];
  try {
    pathEntries = execFileSync('where.exe', ['codex'], { encoding: 'utf8', windowsHide: true })
      .split(/\r?\n/).map((value) => value.trim()).filter(Boolean);
  } catch {}
  const candidates = [
    resolve(process.env.APPDATA || '', 'npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin/codex.exe'),
    resolve(process.env.APPDATA || '', 'npm/node_modules/@openai/codex/bin/codex.js'),
    process.env.CODEX_CLI_PATH,
    ...pathEntries,
  ].filter(Boolean);
  const found = candidates.find((path) => existsSync(path));
  if (!found) throw new Error('找不到可执行的 Codex CLI；请确认 CODEX_CLI_PATH 或官方 CLI 安装');
  return found;
}

function codexInvocation() {
  const path = locateCodex();
  return path.endsWith('.js') ? { executable: process.execPath, prefix: [path] } : { executable: path, prefix: [] };
}

function getIsolatedCodexHome() {
  if (isolatedCodexHome) return isolatedCodexHome;
  const sourceHome = process.env.CODEX_HOME || resolve(process.env.USERPROFILE || '', '.codex');
  const authPath = resolve(sourceHome, 'auth.json');
  if (!existsSync(authPath)) throw new Error(`找不到 Codex 登录凭证 ${authPath}`);
  isolatedCodexHome = mkdtempSync(resolve(tmpdir(), 'broker-note-codex-'));
  copyFileSync(authPath, resolve(isolatedCodexHome, 'auth.json'));
  writeFileSync(resolve(isolatedCodexHome, 'config.toml'), `model = "${MODEL}"\nmodel_reasoning_effort = "${REASONING_EFFORT}"\napproval_policy = "never"\nsandbox_mode = "read-only"\n[features]\nplugins = false\nskill_search = false\nmulti_agent = false\ncomputer_use = false\nbrowser_use = false\napps = false\nimage_generation = false\nshell_snapshot = false\n`, 'utf8');
  return isolatedCodexHome;
}

function cleanupIsolatedCodexHome() {
  if (!isolatedCodexHome) return;
  try { rmSync(isolatedCodexHome, { recursive: true, force: true }); } catch {}
  isolatedCodexHome = null;
}
process.on('exit', cleanupIsolatedCodexHome);
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { codexAppServer?.close(); cleanupIsolatedCodexHome(); process.exit(130); });

class CodexAppServer {
  constructor() {
    const { executable, prefix } = codexInvocation();
    const cleanHome = getIsolatedCodexHome();
    const args = [...prefix, 'app-server', '--listen', 'stdio://', '--disable', 'plugins', '--disable', 'skill_search', '--disable', 'multi_agent', '--disable', 'computer_use', '--disable', 'browser_use', '--disable', 'apps', '--disable', 'image_generation', '--disable', 'shell_snapshot'];
    this.child = spawn(executable, args, { cwd: ROOT, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true, env: { ...process.env, CODEX_HOME: cleanHome, USERPROFILE: cleanHome, HOME: cleanHome } });
    this.nextId = 1;
    this.pending = new Map();
    this.turns = new Map();
    this.turnTexts = new Map();
    this.completedTurns = new Map();
    this.ready = this.initialize();
    createInterface({ input: this.child.stdout }).on('line', (line) => this.onMessage(line));
    this.child.stderr.on('data', (chunk) => process.stderr.write(chunk));
    this.child.on('exit', (code) => {
      const error = new Error(`Codex app-server 已退出，code=${code}`);
      for (const pending of this.pending.values()) pending.reject(error);
      for (const turn of this.turns.values()) turn.reject(error);
    });
  }

  onMessage(line) {
    let message;
    try { message = JSON.parse(line); } catch { return; }
    if (message.id != null && this.pending.has(message.id)) {
      const pending = this.pending.get(message.id); this.pending.delete(message.id);
      if (message.error) pending.reject(new Error(JSON.stringify(message.error)));
      else pending.resolve(message.result);
      return;
    }
    const method = message.method;
    const params = message.params || {};
    const turnId = params.turnId || params.turn?.id;
    if (method === 'item/agentMessage/delta' && turnId) {
      this.turnTexts.set(turnId, (this.turnTexts.get(turnId) || '') + (params.delta || ''));
    }
    if (method === 'item/completed' && turnId && params.item?.type === 'agentMessage') {
      if (params.item.text) this.turnTexts.set(turnId, params.item.text);
    }
    if (method === 'turn/completed' && turnId) {
      const turn = this.turns.get(turnId);
      if (turn) { this.turns.delete(turnId); turn.resolve(this.turnTexts.get(turnId) || ''); }
      else this.completedTurns.set(turnId, params);
    }
  }

  request(method, params = {}, timeoutMs = 30000) {
    const id = this.nextId++;
    return new Promise((resolveRequest, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`${method} 超时`)); }, timeoutMs);
      this.pending.set(id, { resolve: (value) => { clearTimeout(timer); resolveRequest(value); }, reject: (error) => { clearTimeout(timer); reject(error); } });
      this.child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
    });
  }

  async initialize() {
    await this.request('initialize', { clientInfo: { name: 'broker_note_enrichment', title: 'Broker Note Enrichment', version: '1.0.0' }, capabilities: { experimentalApi: true, optOutNotificationMethods: ['item/agentMessage/delta'] } });
    this.child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'initialized', params: {} })}\n`);
  }

  async run(prompt, schema) {
    await this.ready;
    // Keep one authenticated app-server/network connection, but isolate every batch in
    // a fresh ephemeral thread. Long-lived threads may expire server-side after dozens
    // of rollbacks and then return an empty terminal message.
    const threadResult = await this.request('thread/start', { model: MODEL, cwd: ROOT, approvalPolicy: 'never', sandbox: 'read-only', ephemeral: true }, 60000);
    const threadId = threadResult.thread.id;
    const started = await this.request('turn/start', { threadId, input: [{ type: 'text', text: prompt }], model: MODEL, effort: REASONING_EFFORT, summary: 'none', outputSchema: schema, environments: [] }, 60000);
    const turnId = started.turn.id;
    let text;
    if (this.completedTurns.has(turnId)) { this.completedTurns.delete(turnId); text = this.turnTexts.get(turnId) || ''; }
    else text = await new Promise((resolveTurn, reject) => {
      const timer = setTimeout(() => { this.turns.delete(turnId); reject(new Error('Codex app-server turn 超时')); }, 8 * 60 * 1000);
      this.turns.set(turnId, { resolve: (value) => { clearTimeout(timer); resolveTurn(value); }, reject: (error) => { clearTimeout(timer); reject(error); } });
    });
    if (!String(text || '').trim()) throw new Error('Codex 返回空结构化结果');
    const parsed = JSON.parse(text);
    this.turnTexts.delete(turnId);
    return parsed;
  }

  close() { try { this.child.stdin.end(); } catch {} }
}

function getCodexAppServer() {
  if (!codexAppServer) codexAppServer = new CodexAppServer();
  return codexAppServer;
}

function cloneProduction(restart = false) {
  mkdirSync(dirname(CANDIDATE_PATH), { recursive: true });
  if (existsSync(CANDIDATE_PATH)) {
    if (!restart) return false;
    unlinkSync(CANDIDATE_PATH);
  }
  const production = new DatabaseSync(DB_PATH, { readOnly: true });
  production.exec(`VACUUM INTO '${sqlPath(CANDIDATE_PATH)}'`);
  production.close();
  const candidate = new DatabaseSync(CANDIDATE_PATH);
  ensureAiSchema(candidate);
  candidate.close();
  return true;
}

function existingState(db, topicId) {
  const topic = db.prepare('SELECT source_broker,source_team,source_raw FROM topics WHERE topic_id=?').get(topicId);
  const mentions = db.prepare(`SELECT s.code,s.market,s.name,m.direction,m.target_price,m.target_currency,
      m.entity_confidence,m.recognition_confidence,m.recognition_method
    FROM mentions m JOIN securities s ON s.id=m.security_id WHERE m.topic_id=?`).all(topicId);
  return { topic, mentions };
}

function makeBatches(rows, maxTopics = MAX_TOPICS, maxChars = MAX_CHARS) {
  const batches = [];
  let batch = [];
  let chars = 0;
  for (const row of rows) {
    const size = String(row.title || '').length + String(row.content || '').length;
    if (batch.length && (batch.length >= maxTopics || chars + size > maxChars)) {
      batches.push(batch); batch = []; chars = 0;
    }
    batch.push(row); chars += size;
  }
  if (batch.length) batches.push(batch);
  return batches;
}

function promptFor(rows, phase) {
  const payload = rows.map((row) => ({
    topic_id: String(row.topic_id), title: row.title || '', content: row.content || '',
    existing_mentions: row.existing.mentions,
    ...(ADJUDICATE_AI ? { candidates_to_judge: adjudicationCandidates.get(String(row.topic_id)) || [] } : {}),
  }));
  if (VIEWPOINT_MODE) {
    const viewpointPayload = rows.map((row) => ({
      topic_id: String(row.topic_id), title: row.title || '', blocks: row.viewpoint_blocks || splitViewpointBlocks(row.content),
    }));
    if (phase === 'verify') return `你是卖方小作文证券映射的独立复核员。忽略已有识别，重新阅读全文，只输出符合 schema 的 JSON。

规则：
1. entities 仅列出原文确实涉及的上市公司及标准代码；market 只能为 SH、SZ、BJ、HK、US，禁止 A，不确定代码不得猜。
2. 每个公司独立判断方向：推荐、关注、高弹性、看好、受益、超预期为 bullish；明确看空、卖出、减持、回避、下调评级为 bearish；纯事实为 neutral。
3. evidence 必须是能同时支持公司映射及方向的原文连续精确片段。
4. 原文明示具体公司但代码无法可靠确定时放入 unresolved_entities；泛称公司、厂商、客户不输出。
5. source 仅在标题、首尾行或落款明确署名券商/研究组时填写；没有署名为 null，禁止推测。
6. 每个 topic_id 必须且只能输出一次；没有实体和署名时填写 abstain_reason。

输入 JSON：
${JSON.stringify({ topics: viewpointPayload })}`;
    const role = '你是中国证券卖方小作文的观点结构化抽取员。';
    return `${role}逐篇覆盖原文中的全部实质观点，只输出符合 schema 的 JSON。

规则：
1. 先拆观点，再识别每个观点涉及的上市公司。宏观、行业或事件观点即使没有公司也必须保存，entities 可以为空。
2. summary 是忠实简述；viewpoint evidence 必须是能支撑该观点的原文连续精确片段，禁止改写成证据。
3. 每个观点可关联多个公司；公司 evidence 必须逐字来自原文并能支持该公司与该观点的关系。
4. 已确定代码时 market 只能为 SH、SZ、BJ、HK、US，禁止 A；代码必须是标准代码，不确定不得猜。
5. 原文明示或公认简称指向具体公司、但无法可靠确定标准代码时，放入 unresolved_entities，并给出适合联网核验的 search_query；“其他公司、多家客户、相关厂商”等泛称不是证券，不得输出。
6. 推荐、关注、高弹性、看好、受益、超预期为 bullish；明确看空、卖出、减持、回避、下调评级为 bearish；纯事实为 neutral。方向是“观点—公司”级。
7. 目标价只绑定同一观点中明确对应的公司；没有则为 null，币种未知为空字符串。
8. source 仅在标题、首尾行或落款明确署名券商/研究组时填写；没有署名为 null，禁止推测或填“其他”。
9. 每个输入 block 必须被处理：实质内容的 block ordinal 写入一个或多个观点的 source_block_ordinals；免责声明、联系方式、纯标题编号等写入 ignored_blocks 并说明原因。不得遗漏任何 block。
10. 观点 ordinal 从 1 连续递增；每个 topic_id 必须且只能输出一次。所有 block 均可忽略时 viewpoints=[] 并说明 abstain_reason。

输入 JSON：
${JSON.stringify({ topics: viewpointPayload })}`;
  }
  if (ADJUDICATE_AI) {
    return `你是上市公司标的准确性裁决员。逐条审查 candidates_to_judge，只返回原文确实支持的候选；不要自由补充其他标的，只输出 schema JSON。

裁决规则：
1. 候选公司必须在标题/正文中被明确点名、用公认简称指代，或由紧邻上下文唯一指向；不能仅凭泛化业务描述猜公司。
2. 行业、产品、人物、会议工具、发文券商署名不是标的。
3. 原文若把候选列入推荐、关注、弹性、受益、看好名单，方向为 bullish；明确看空为 bearish；仅陈述事实为 neutral。
4. 返回通过裁决的候选时，code/market 必须与输入候选一致，evidence 必须逐字来自原文；未通过的候选不要返回。
5. 每个 topic_id 只输出一次；全部不通过时 mentions=[]，并在 abstain_reason 说明。

输入 JSON：
${JSON.stringify({ topics: payload })}`;
  }
  if (TARGETS_ONLY) {
    const role = phase === 'verify' ? '你是上市公司标的独立复核员。' : '你是中国证券卖方文本的上市公司标的抽取员。';
    return `${role}逐篇识别正文真正讨论、推荐、看空或评价的上市公司，只输出 schema JSON。

规则：
1. 返回每篇原文中的全部真实标的；行业、产品、人物、会议工具以及仅作为署名出现的券商不是标的。
2. market 只能是 SH、SZ、BJ、HK、US；代码必须匹配标准证券代码，不确定就不猜。
3. 推荐、建议关注、重点关注、高弹性、看好、受益、上调评级为 bullish；明确看空、卖出、减持、回避为 bearish；纯事实为 neutral。
4. evidence 必须是原文逐字片段，并包含公司名、简称、代码或能唯一定位该公司的上下文。
5. 每个 topic_id 必须且只能输出一次；无标的时 mentions=[] 并说明 abstain_reason。

输入 JSON：
${JSON.stringify({ topics: payload })}`;
  }
  if (VIEWPOINT_MODE && CHANGED_ONLY) {
    const completed = db.prepare(`SELECT 1 FROM ai_enrichment_results WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase='extract' AND status='complete' LIMIT 1`);
    rows = rows.filter((row) => !completed.get(String(row.topic_id), row.content_hash, PROVIDER, MODEL, PROMPT_VERSION));
  }
  const role = phase === 'verify'
    ? '你是独立复核员。忽略既有识别结论，重新从原文抽取，然后仅输出符合 schema 的结果。'
    : '你是中国证券卖方小作文结构化抽取员。逐篇检查原文，仅输出符合 schema 的结果。';
  return `${role}

规则：
1. 找出文中真实被讨论、推荐、看空或给出目标价的上市公司；行业名、产品名、会议工具和作为署名出现的券商都不是标的。
2. market 只能是 SH、SZ、BJ、HK、US，绝不输出 A。代码必须是标准证券代码；不确定时不要猜。
3. 推荐、建议关注、重点关注、高弹性、看好、受益、上调评级通常是 bullish；明确看空、卖出、减持、回避是 bearish；纯事实且无方向是 neutral。
4. 每个标的 evidence 必须逐字摘自标题或正文，并包含标的名称/代码或足以唯一指向该标的的上下文。
5. source 只表示发文券商/研究组。只有标题、首尾行、落款等明确署名时才填写；正文引用某券商观点不等于发文方。无署名必须为 null，禁止归为“其他”。
6. suspected_false_positive_codes 只列出现有识别中明显属于行业词、工具名或署名券商而非讨论标的的代码；本任务不会据此自动删除。
7. 每个输入 topic_id 必须且只能输出一次；即便没有标的也输出空 mentions 和 abstain_reason。

输入 JSON：
${JSON.stringify({ topics: payload })}`;
}

function schemaForRun(phase) {
  if (VIEWPOINT_MODE && phase === 'verify') return JSON.parse(readFileSync(VIEWPOINT_VERIFY_SCHEMA_PATH, 'utf8'));
  if (VIEWPOINT_MODE) return JSON.parse(readFileSync(VIEWPOINT_SCHEMA_PATH, 'utf8'));
  if (!TARGETS_ONLY) return JSON.parse(readFileSync(SCHEMA_PATH, 'utf8'));
  return {
    type: 'object', additionalProperties: false, required: ['topics'], properties: {
      topics: { type: 'array', items: {
        type: 'object', additionalProperties: false, required: ['topic_id', 'mentions', 'abstain_reason'], properties: {
          topic_id: { type: 'string' }, abstain_reason: { type: 'string' }, mentions: { type: 'array', items: {
            type: 'object', additionalProperties: false,
            required: ['surface', 'canonical_name', 'code', 'market', 'direction', 'evidence', 'confidence'],
            properties: {
              surface: { type: 'string' }, canonical_name: { type: 'string' }, code: { type: 'string' },
              market: { type: 'string', enum: ['SH', 'SZ', 'BJ', 'HK', 'US'] },
              direction: { type: 'string', enum: ['bullish', 'bearish', 'neutral'] },
              evidence: { type: 'string' }, confidence: { type: 'number', minimum: 0, maximum: 1 },
            },
          } },
        },
      } },
    },
  };
}

function invokeCodex(rows, phase, attempt = 1) {
  const schema = schemaForRun(phase);
  return getCodexAppServer().run(promptFor(rows, phase), schema);
}

async function invokeOllama(rows, phase) {
  const endpoint = process.env.OLLAMA_API_URL || 'http://127.0.0.1:11434/api/chat';
  const schema = schemaForRun(phase);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 20 * 60 * 1000);
  try {
    const response = await fetch(endpoint, {
      method: 'POST', headers: { 'content-type': 'application/json' }, signal: controller.signal,
      body: JSON.stringify({ model: MODEL, stream: false, format: schema, keep_alive: '15m', options: { temperature: 0 }, messages: [{ role: 'user', content: promptFor(rows, phase) }] }),
    });
    if (!response.ok) throw new Error(`Ollama HTTP ${response.status}: ${(await response.text()).slice(0, 1000)}`);
    const body = await response.json();
    return JSON.parse(body.message?.content || '');
  } finally { clearTimeout(timer); }
}

async function invokeWithRetry(rows, phase) {
  let lastError;
  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try { return PROVIDER === 'ollama' ? await invokeOllama(rows, phase) : await invokeCodex(rows, phase, attempt); }
    catch (error) {
      lastError = error;
      // A timed-out/disconnected app-server turn cannot safely be reused: the old
      // turn may still be alive and will make later turns hang behind it. Recreate
      // the isolated Codex session before retrying this same checkpointed batch.
      if (PROVIDER === 'codex' && codexAppServer) {
        codexAppServer.close();
        codexAppServer = null;
      }
      if (attempt < 3) await new Promise((resolveWait) => setTimeout(resolveWait, [3000, 10000][attempt - 1]));
    }
  }
  throw lastError;
}

function savePhase(db, runId, rows, phase, payload, batchError = null) {
  const returned = new Map((payload?.topics || []).map((result) => [String(result.topic_id), result]));
  const upsert = db.prepare(`INSERT INTO ai_enrichment_results(topic_id,content_hash,provider,model,prompt_version,phase,status,result_json,validation_json,error,processed_at,run_id)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(topic_id,content_hash,provider,model,prompt_version,phase) DO UPDATE SET
    status=excluded.status,result_json=excluded.result_json,validation_json=excluded.validation_json,error=excluded.error,processed_at=excluded.processed_at,run_id=excluded.run_id`);
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const row of rows) {
      const result = returned.get(String(row.topic_id));
      const validation = result
        ? (VIEWPOINT_MODE
          ? (phase === 'verify' && Array.isArray(result.entities)
            ? validateViewpointVerifierResult(row, result, securityIndex)
            : validateViewpointTopicResult(row, result, securityIndex))
          : validateTopicResult(row, result, securityIndex))
        : VIEWPOINT_MODE
          ? { valid: false, errors: [batchError || 'missing_topic_result'], viewpoints: [], source: null }
          : { valid: false, errors: [batchError || 'missing_topic_result'], mentions: [], source: null };
      upsert.run(String(row.topic_id), row.content_hash, PROVIDER, MODEL, PROMPT_VERSION, phase,
        validation.valid ? 'complete' : 'failed', result ? JSON.stringify(result) : null, JSON.stringify(validation), validation.valid ? null : validation.errors.join(';'), now(), runId);
    }
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
}

function resultRow(db, topic, phase) {
  return db.prepare(`SELECT * FROM ai_enrichment_results WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase=?`)
    .get(String(topic.topic_id), topic.content_hash, PROVIDER, MODEL, PROMPT_VERSION, phase);
}

function revalidateCachedViewpointResults(db, rows) {
  if (!VIEWPOINT_MODE) return 0;
  const select = db.prepare(`SELECT * FROM ai_enrichment_results WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase=?`);
  const update = db.prepare('UPDATE ai_enrichment_results SET status=?,validation_json=?,error=?,processed_at=? WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase=?');
  let changed = 0;
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const topic of rows) for (const phase of ['extract', 'verify']) {
      const stored = select.get(String(topic.topic_id), topic.content_hash, PROVIDER, MODEL, PROMPT_VERSION, phase);
      if (!stored?.result_json) continue;
      let result;
      try { result = JSON.parse(stored.result_json); } catch { continue; }
      const validation = phase === 'verify' && Array.isArray(result.entities)
        ? validateViewpointVerifierResult(topic, result, securityIndex)
        : validateViewpointTopicResult(topic, result, securityIndex);
      const status = validation.valid ? 'complete' : 'failed';
      const error = validation.valid ? null : validation.errors.join(';');
      if (stored.status !== status || stored.validation_json !== JSON.stringify(validation) || stored.error !== error) changed += 1;
      update.run(status, JSON.stringify(validation), error, now(), String(topic.topic_id), topic.content_hash, PROVIDER, MODEL, PROMPT_VERSION, phase);
    }
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
  return changed;
}

function hasPotentialDelta(row, validation) {
  if (VIEWPOINT_MODE) return flattenViewpointEntities(validation).length > 0
    || (validation.viewpoints || []).some((viewpoint) => (viewpoint.unresolved_entities || []).length > 0)
    || Boolean(validation.source);
  const existingKeys = new Set(row.existing.mentions.map((mention) => securityKey(mention.market, mention.code)));
  if (validation.mentions.some((mention) => !existingKeys.has(securityKey(mention.market, mention.code)))) return true;
  for (const mention of validation.mentions) {
    const old = row.existing.mentions.find((item) => securityKey(item.market, item.code) === securityKey(mention.market, mention.code));
    if (old && (old.direction !== mention.direction || Number(old.target_price || 0) !== Number(mention.target_price || 0))) return true;
  }
  return Boolean(validation.source && normalizeBroker(validation.source.broker) !== normalizeBroker(row.existing.topic.source_broker));
}

function loadRows(db) {
  let rows = db.prepare('SELECT topic_id,title,content,content_hash FROM topics ORDER BY published_at_utc,topic_id').all();
  rows = rows.map((row) => ({ ...row, viewpoint_blocks: splitViewpointBlocks(row.content), existing: existingState(db, row.topic_id) }));
  if (ADJUDICATE_AI) rows = rows.filter((row) => adjudicationCandidates.has(String(row.topic_id)));
  if (AUDIT_AI) {
    const auditedTopicIds = new Set(db.prepare("SELECT DISTINCT topic_id FROM mentions WHERE recognition_method='gpt_verified'").all().map((row) => String(row.topic_id)));
    rows = rows.filter((row) => auditedTopicIds.has(String(row.topic_id)));
  }
  if (TARGETS_ONLY) {
    rows = rows.filter((row) => AUDIT_AI || ADJUDICATE_AI || row.existing.mentions.length === 0 || (!MISSING_ONLY && row.existing.mentions.some((mention) => {
      const confidence = mention.recognition_confidence ?? mention.entity_confidence;
      return confidence == null || Number(confidence) < 0.92;
    })));
  }
  if (PILOT > 0) rows = stratifiedPilot(rows, PILOT);
  else if (LIMIT > 0) rows = rows.slice(0, LIMIT);
  return rows;
}

function evenlySpaced(rows, count) {
  if (rows.length <= count) return rows;
  const selected = [];
  for (let index = 0; index < count; index += 1) selected.push(rows[Math.floor(index * rows.length / count)]);
  return selected;
}

function stratifiedPilot(rows, count) {
  const perGroup = Math.max(1, Math.ceil(count / 5));
  const groups = [
    rows.filter((row) => row.existing.mentions.length === 0),
    rows.filter((row) => row.existing.mentions.length >= 2),
    rows.filter((row) => row.existing.topic.source_broker),
    rows.filter((row) => String(row.content || '').length >= 1200),
    rows,
  ];
  const byId = new Map();
  for (const group of groups) for (const row of evenlySpaced(group, perGroup)) byId.set(String(row.topic_id), row);
  if (byId.size < count) for (const row of evenlySpaced(rows, count * 2)) { byId.set(String(row.topic_id), row); if (byId.size >= count) break; }
  return [...byId.values()].slice(0, count).sort((a, b) => String(a.topic_id).localeCompare(String(b.topic_id)));
}

function getOrCreateRun(db, total) {
  const active = db.prepare(`SELECT * FROM ai_enrichment_runs WHERE provider=? AND model=? AND prompt_version=? AND status IN ('running','paused') ORDER BY id DESC LIMIT 1`).get(PROVIDER, MODEL, PROMPT_VERSION);
  if (active) { db.prepare("UPDATE ai_enrichment_runs SET status='running',error=NULL WHERE id=?").run(active.id); return Number(active.id); }
  return Number(db.prepare(`INSERT INTO ai_enrichment_runs(provider,model,prompt_version,status,started_at,total_topics) VALUES(?,?,?,?,?,?)`)
    .run(PROVIDER, MODEL, PROMPT_VERSION, 'running', now(), total).lastInsertRowid);
}

function refreshRunMetrics(db, runId, total, status = 'running', error = null) {
  const completed = Number(db.prepare(`SELECT count(*) n FROM ai_enrichment_results WHERE run_id=? AND phase='extract' AND status='complete'`).get(runId).n);
  const failed = Number(db.prepare(`SELECT count(*) n FROM ai_enrichment_results WHERE run_id=? AND status='failed'`).get(runId).n);
  db.prepare('UPDATE ai_enrichment_runs SET status=?,completed_topics=?,failed_topics=?,total_topics=?,error=?,finished_at=? WHERE id=?')
    .run(status, completed, failed, total, error, ['complete', 'failed'].includes(status) ? now() : null, runId);
  return { completed, failed };
}

function parseValidation(row) { return JSON.parse(row.validation_json); }

function mergeVerified(db, rows, runId) {
  const getSecurity = db.prepare('SELECT id,name FROM securities WHERE code=? AND market=?');
  const upsertSecurity = db.prepare('INSERT INTO securities(code,market,name) VALUES(?,?,?) ON CONFLICT(code,market) DO UPDATE SET name=excluded.name');
  const insertMention = db.prepare(`INSERT INTO mentions(topic_id,security_id,direction,direction_confidence,entity_confidence,target_price,target_currency,target_price_text,evidence,recognition_method,recognition_model,recognition_confidence)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`);
  const updateMention = db.prepare(`UPDATE mentions SET direction=?,direction_confidence=?,target_price=?,target_currency=?,target_price_text=?,evidence=?,recognition_method='gpt_verified',recognition_model=?,recognition_confidence=? WHERE id=?`);
  const findMention = db.prepare('SELECT m.* FROM mentions m JOIN securities s ON s.id=m.security_id WHERE m.topic_id=? AND s.code=? AND s.market=?');
  const updateSource = db.prepare(`UPDATE topics SET source_broker=?,source_team=?,source_raw=?,source_recognition_method='gpt_verified',source_recognition_model=?,source_recognition_confidence=? WHERE topic_id=?`);
  const updateResult = db.prepare(`UPDATE ai_enrichment_results SET merge_status=?,validation_json=? WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase='extract'`);
  const summary = { topics: rows.length, added_mentions: 0, updated_mentions: 0, updated_sources: 0, review_items: 0, unchanged: 0 };
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const topic of rows) {
      const primaryRow = resultRow(db, topic, 'extract');
      const verifierRow = resultRow(db, topic, 'verify');
      if (!primaryRow || primaryRow.status !== 'complete') continue;
      const primary = parseValidation(primaryRow);
      const verifier = verifierRow?.status === 'complete' ? parseValidation(verifierRow) : { mentions: [], source: null };
      const agreed = compareVerified(primary, verifier);
      const decisions = { added: [], updated: [], source: null, review: [...(primary.suspected_false_positive_codes || [])] };
      for (const mention of agreed.mentions) {
        const security = securityIndex.byKey.get(securityKey(mention.market, mention.code));
        if (!security || !evidenceExists(topic, mention.evidence)) continue;
        let existing = findMention.get(String(topic.topic_id), security.code, security.market);
        if (!existing && shouldAutoAcceptMention(mention)) {
          upsertSecurity.run(security.code, security.market, security.name);
          const securityId = getSecurity.get(security.code, security.market).id;
          insertMention.run(String(topic.topic_id), securityId, mention.direction, mention.confidence, mention.confidence,
            mention.target_price, mention.target_currency || null, mention.target_price == null ? null : String(mention.target_price), mention.evidence.slice(0, 1000),
            'gpt_verified', MODEL, Math.min(mention.confidence, mention.verifier_confidence));
          summary.added_mentions += 1; decisions.added.push(securityKey(security.market, security.code));
          continue;
        }
        if (existing && shouldAutoAcceptDirection(mention)) {
          const changed = existing.direction !== mention.direction || Number(existing.target_price || 0) !== Number(mention.target_price || 0);
          if (changed) {
            updateMention.run(mention.direction, mention.confidence, mention.target_price, mention.target_currency || null,
              mention.target_price == null ? null : String(mention.target_price), mention.evidence.slice(0, 1000), MODEL,
              Math.min(mention.confidence, mention.verifier_confidence), existing.id);
            summary.updated_mentions += 1; decisions.updated.push(securityKey(security.market, security.code));
          }
        }
      }
      if (shouldAutoAcceptSource(agreed.source) && evidenceExists(topic, agreed.source.evidence)) {
        const oldBroker = normalizeBroker(topic.existing.topic.source_broker);
        const newBroker = normalizeBroker(agreed.source.broker);
        if (newBroker && oldBroker !== newBroker) {
          updateSource.run(agreed.source.broker, agreed.source.team || '', agreed.source.evidence.slice(0, 120), MODEL,
            Math.min(agreed.source.confidence, agreed.source.verifier_confidence), String(topic.topic_id));
          summary.updated_sources += 1; decisions.source = agreed.source.broker;
        }
      }
      summary.review_items += decisions.review.length;
      if (!decisions.added.length && !decisions.updated.length && !decisions.source && !decisions.review.length) summary.unchanged += 1;
      updateResult.run(decisions.added.length || decisions.updated.length || decisions.source ? 'merged' : decisions.review.length ? 'review' : 'unchanged',
        JSON.stringify({ ...primary, agreement: agreed, decisions }), String(topic.topic_id), topic.content_hash, PROVIDER, MODEL, PROMPT_VERSION);
    }
    db.exec('COMMIT');
  } catch (error) { db.exec('ROLLBACK'); throw error; }
  db.prepare("UPDATE ai_enrichment_runs SET status='complete',finished_at=? WHERE id=?").run(now(), runId);
  return summary;
}

function mergeViewpoints(db, rows, runId) {
  const deleteViewpoints = db.prepare('DELETE FROM model_viewpoints WHERE topic_id=? AND recognition_model=? AND prompt_version=?');
  const deleteResolutionQueue = db.prepare('DELETE FROM security_resolution_queue WHERE topic_id=? AND recognition_model=? AND prompt_version=?');
  const insertViewpoint = db.prepare(`INSERT INTO model_viewpoints(topic_id,content_hash,ordinal,source_block_ordinals,summary,evidence,viewpoint_type,confidence,recognition_model,prompt_version,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
  const insertViewpointMention = db.prepare(`INSERT INTO model_viewpoint_mentions(viewpoint_id,security_id,surface,direction,target_price,target_currency,evidence,confidence,verification_status)
    VALUES(?,?,?,?,?,?,?,?,?)`);
  const insertQueue = db.prepare(`INSERT INTO security_resolution_queue(topic_id,content_hash,viewpoint_ordinal,surface,canonical_name,direction,evidence,confidence,search_query,reason,status,recognition_model,prompt_version,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?,?,?,?)`);
  const upsertSecurity = db.prepare('INSERT INTO securities(code,market,name) VALUES(?,?,?) ON CONFLICT(code,market) DO UPDATE SET name=excluded.name');
  const getSecurity = db.prepare('SELECT id FROM securities WHERE code=? AND market=?');
  const deleteTopicMentions = db.prepare('DELETE FROM mentions WHERE topic_id=?');
  const findMention = db.prepare('SELECT m.id FROM mentions m JOIN securities s ON s.id=m.security_id WHERE m.topic_id=? AND s.code=? AND s.market=?');
  const insertMention = db.prepare(`INSERT INTO mentions(topic_id,security_id,direction,direction_confidence,entity_confidence,target_price,target_currency,target_price_text,evidence,recognition_method,recognition_model,recognition_confidence,is_mixed)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`);
  const updateMention = db.prepare(`UPDATE mentions SET direction=?,direction_confidence=?,entity_confidence=?,target_price=?,target_currency=?,target_price_text=?,evidence=?,recognition_method='gpt_viewpoint_verified',recognition_model=?,recognition_confidence=?,is_mixed=? WHERE id=?`);
  const updateSource = db.prepare(`UPDATE topics SET source_broker=?,source_team=?,source_raw=?,source_recognition_method='gpt_viewpoint_verified',source_recognition_model=?,source_recognition_confidence=? WHERE topic_id=?`);
  const updateResult = db.prepare(`UPDATE ai_enrichment_results SET merge_status=?,validation_json=? WHERE topic_id=? AND content_hash=? AND provider=? AND model=? AND prompt_version=? AND phase='extract'`);
  const summary = { topics: rows.length, viewpoints: 0, verified_viewpoint_mentions: 0, unresolved_entities: 0, materialized_mentions: 0, mixed_mentions: 0, updated_sources: 0, authoritative: AUTHORITATIVE };
  db.exec('BEGIN IMMEDIATE');
  try {
    for (const topic of rows) {
      const primaryRow = resultRow(db, topic, 'extract');
      const verifierRow = resultRow(db, topic, 'verify');
      if (!primaryRow || primaryRow.status !== 'complete') continue;
      const primary = parseValidation(primaryRow);
      const verifier = verifierRow?.status === 'complete' ? parseValidation(verifierRow) : { viewpoints: [], source: null };
      const compared = compareViewpointEntities(primary, verifier);
      const acceptedByOrdinal = new Map();
      for (const entity of compared) {
        if (!entity.verified || entity.confidence < 0.92 || entity.verifier_confidence < 0.92) continue;
        if (!acceptedByOrdinal.has(entity.viewpoint_ordinal)) acceptedByOrdinal.set(entity.viewpoint_ordinal, []);
        acceptedByOrdinal.get(entity.viewpoint_ordinal).push(entity);
      }
      const verifierUnresolved = new Set((verifier.viewpoints || []).flatMap((viewpoint) => (viewpoint.unresolved_entities || []).map((entity) => String(entity.surface || entity.canonical_name || '').trim().toLowerCase())));
      deleteViewpoints.run(String(topic.topic_id), MODEL, PROMPT_VERSION);
      deleteResolutionQueue.run(String(topic.topic_id), MODEL, PROMPT_VERSION);
      const aggregated = new Map();
      for (const viewpoint of primary.viewpoints || []) {
        const inserted = insertViewpoint.run(String(topic.topic_id), topic.content_hash, viewpoint.ordinal, JSON.stringify(viewpoint.source_block_ordinals || []), viewpoint.summary,
          viewpoint.evidence, viewpoint.viewpoint_type, viewpoint.confidence, MODEL, PROMPT_VERSION, now());
        const viewpointId = Number(inserted.lastInsertRowid);
        summary.viewpoints += 1;
        for (const entity of acceptedByOrdinal.get(viewpoint.ordinal) || []) {
          const security = securityIndex.byKey.get(securityKey(entity.market, entity.code));
          if (!security) continue;
          upsertSecurity.run(security.code, security.market, security.name);
          const securityId = Number(getSecurity.get(security.code, security.market).id);
          const confidence = Math.min(entity.confidence, entity.verifier_confidence);
          insertViewpointMention.run(viewpointId, securityId, entity.surface || '', entity.direction, entity.target_price,
            entity.target_currency || null, entity.evidence, confidence, 'double_verified');
          summary.verified_viewpoint_mentions += 1;
          const key = securityKey(security.market, security.code);
          if (!aggregated.has(key)) aggregated.set(key, { security, securityId, rows: [] });
          aggregated.get(key).rows.push({ ...entity, confidence });
        }
        for (const unresolved of viewpoint.unresolved_entities || []) {
          const unresolvedKey = String(unresolved.surface || unresolved.canonical_name || '').trim().toLowerCase();
          if (!unresolvedKey || !verifierUnresolved.has(unresolvedKey) || unresolved.confidence < 0.75) continue;
          insertQueue.run(String(topic.topic_id), topic.content_hash, viewpoint.ordinal, unresolved.surface,
            unresolved.canonical_name || '', unresolved.direction, unresolved.evidence, unresolved.confidence,
            unresolved.search_query || `${unresolved.canonical_name || unresolved.surface} 股票 代码`, unresolved.reason || '',
            MODEL, PROMPT_VERSION, now(), now());
          summary.unresolved_entities += 1;
        }
      }
      if (AUTHORITATIVE) deleteTopicMentions.run(String(topic.topic_id));
      for (const group of aggregated.values()) {
        const directions = new Set(group.rows.map((row) => row.direction));
        const mixed = directions.has('bullish') && directions.has('bearish');
        const direction = mixed ? 'neutral' : directions.has('bullish') ? 'bullish' : directions.has('bearish') ? 'bearish' : 'neutral';
        const targetRow = group.rows.filter((row) => row.target_price != null).sort((a, b) => b.confidence - a.confidence)[0] || null;
        const confidence = Math.max(...group.rows.map((row) => row.confidence));
        const evidence = [...new Set(group.rows.map((row) => row.evidence))].join(' | ').slice(0, 2000);
        const existing = findMention.get(String(topic.topic_id), group.security.code, group.security.market);
        if (existing) {
          updateMention.run(direction, confidence, confidence, targetRow?.target_price ?? null, targetRow?.target_currency || null,
            targetRow?.target_price == null ? null : String(targetRow.target_price), evidence, MODEL, confidence, mixed ? 1 : 0, existing.id);
        } else {
          insertMention.run(String(topic.topic_id), group.securityId, direction, confidence, confidence,
            targetRow?.target_price ?? null, targetRow?.target_currency || null,
            targetRow?.target_price == null ? null : String(targetRow.target_price), evidence,
            'gpt_viewpoint_verified', MODEL, confidence, mixed ? 1 : 0);
        }
        summary.materialized_mentions += 1;
        if (mixed) summary.mixed_mentions += 1;
      }
      const primarySource = primary.source;
      const verifierSource = verifier.source;
      if (primarySource && verifierSource
        && normalizeBroker(primarySource.broker) === normalizeBroker(verifierSource.broker)
        && Number(primarySource.confidence) >= 0.95 && Number(verifierSource.confidence) >= 0.95
        && evidenceExists(topic, primarySource.evidence) && evidenceExists(topic, verifierSource.evidence)) {
        const confidence = Math.min(Number(primarySource.confidence), Number(verifierSource.confidence));
        updateSource.run(primarySource.broker, primarySource.team || verifierSource.team || '', primarySource.evidence.slice(0, 300), MODEL, confidence, String(topic.topic_id));
        summary.updated_sources += 1;
      }
      updateResult.run('merged', JSON.stringify({ ...primary, verified_entities: compared, materialized: aggregated.size }),
        String(topic.topic_id), topic.content_hash, PROVIDER, MODEL, PROMPT_VERSION);
    }
    db.exec('COMMIT');
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
  db.prepare("UPDATE ai_enrichment_runs SET status='complete',finished_at=? WHERE id=?").run(now(), runId);
  return summary;
}

function writeReports(db, runId, summary = null) {
  const run = db.prepare('SELECT * FROM ai_enrichment_runs WHERE id=?').get(runId);
  const review = db.prepare(`SELECT r.topic_id,t.published_at,t.title,r.validation_json FROM ai_enrichment_results r JOIN topics t USING(topic_id)
    WHERE r.run_id=? AND r.phase='extract' AND r.merge_status='review' LIMIT 300`).all(runId);
  const payload = { generated_at: now(), candidate: CANDIDATE_PATH, run, summary, review };
  writeFileSync(REPORT_JSON, JSON.stringify(payload, null, 2), 'utf8');
  const lines = ['# GPT 历史识别强化', '', `- 模型：${run.model}`, `- 提示词版本：${run.prompt_version}`, `- 状态：${run.status}`,
    `- 已处理：${run.completed_topics}/${run.total_topics}`, `- 失败：${run.failed_topics}`, `- 批次：${run.batch_count}`];
  if (summary && VIEWPOINT_MODE) lines.push('', '## 观点合并结果', '', `- 观点：${summary.viewpoints}`, `- 双重验证观点-证券：${summary.verified_viewpoint_mentions}`, `- 主题级标的：${summary.materialized_mentions}`, `- 待联网解析：${summary.unresolved_entities}`, `- 更新券商来源：${summary.updated_sources}`);
  else if (summary) lines.push('', '## 合并结果', '', `- 新增标的：${summary.added_mentions}`, `- 更新方向/目标价：${summary.updated_mentions}`, `- 更新券商来源：${summary.updated_sources}`, `- 待复核：${summary.review_items}`);
  writeFileSync(REPORT_MD, lines.join('\n'), 'utf8');
}

function registerCandidate(runId, summary) {
  const production = new DatabaseSync(DB_PATH);
  const configHash = createHash('sha256').update(JSON.stringify(config)).digest('hex').slice(0, 12);
  const result = production.prepare('INSERT INTO pipeline_runs(command,status,rules_version,started_at,finished_at,candidate_db,metrics_json) VALUES(?,?,?,?,?,?,?)')
    .run('ai-enrich', 'candidate', version, now(), now(), CANDIDATE_PATH, JSON.stringify(summary));
  production.close();
  writeFileSync(MANIFEST_PATH, JSON.stringify({ run_id: Number(result.lastInsertRowid), version, config_hash: configHash, created_at: now(), candidate_db: CANDIDATE_PATH, source_json: '', metrics: { ai_enrichment: summary }, status: 'candidate', ai_enrichment_run_id: runId }, null, 2), 'utf8');
}

async function run() {
  cloneProduction(hasFlag('restart'));
  const db = new DatabaseSync(CANDIDATE_PATH);
  ensureAiSchema(db);
  const allRows = loadRows(db);
  const revalidated = revalidateCachedViewpointResults(db, allRows);
  const scopedRun = PILOT > 0 || LIMIT > 0;
  const runId = getOrCreateRun(db, allRows.length);
  const pending = allRows.filter((topic) => !resultRow(db, topic, 'extract') || resultRow(db, topic, 'extract').status !== 'complete');
  const batches = makeBatches(pending);
  console.log(JSON.stringify({ phase: 'start', provider: PROVIDER, model: MODEL, prompt_version: PROMPT_VERSION, selected: allRows.length, pending: pending.length, batches: batches.length, revalidated, candidate: CANDIDATE_PATH }));
  try {
    for (let index = 0; index < batches.length; index += 1) {
      const batch = batches[index];
      console.log(JSON.stringify({ phase: 'extract', batch: index + 1, total_batches: batches.length, topics: batch.length }));
      const primaryPayload = await invokeWithRetry(batch, 'extract');
      savePhase(db, runId, batch, 'extract', primaryPayload);
      // Structured output can occasionally omit one item from a large batch. Retry
      // only the omitted/invalid item in the same persistent session instead of
      // rerunning the successful part of the batch.
      const retryRows = batch.filter((topic) => resultRow(db, topic, 'extract')?.status !== 'complete');
      for (const retryRow of retryRows) {
        console.log(JSON.stringify({ phase: 'extract_retry', topic_id: String(retryRow.topic_id) }));
        const retryPayload = await invokeWithRetry([retryRow], 'extract');
        savePhase(db, runId, [retryRow], 'extract', retryPayload);
      }
      db.prepare('UPDATE ai_enrichment_runs SET batch_count=batch_count+1 WHERE id=?').run(runId);
      const metrics = refreshRunMetrics(db, runId, allRows.length);
      console.log(JSON.stringify({ phase: 'progress', ...metrics, total: allRows.length }));
    }
    const verifyRows = allRows.filter((row) => {
      const primary = resultRow(db, row, 'extract');
      const verifier = resultRow(db, row, 'verify');
      return primary?.status === 'complete' && hasPotentialDelta(row, parseValidation(primary)) && verifier?.status !== 'complete';
    });
    const verifyBatches = makeBatches(verifyRows, VERIFY_MAX_TOPICS, VERIFY_MAX_CHARS);
    for (let index = 0; index < verifyBatches.length; index += 1) {
      const batch = verifyBatches[index];
      console.log(JSON.stringify({ phase: 'verify', batch: index + 1, total_batches: verifyBatches.length, topics: batch.length }));
      const verifyPayload = await invokeWithRetry(batch, 'verify');
      savePhase(db, runId, batch, 'verify', verifyPayload);
      const retryRows = batch.filter((topic) => resultRow(db, topic, 'verify')?.status !== 'complete');
      for (const retryRow of retryRows) {
        console.log(JSON.stringify({ phase: 'verify_retry', topic_id: String(retryRow.topic_id) }));
        const retryPayload = await invokeWithRetry([retryRow], 'verify');
        savePhase(db, runId, [retryRow], 'verify', retryPayload);
      }
      db.prepare('UPDATE ai_enrichment_runs SET batch_count=batch_count+1 WHERE id=?').run(runId);
    }
    const missing = allRows.filter((topic) => !resultRow(db, topic, 'extract') || resultRow(db, topic, 'extract').status !== 'complete').length;
    const unverified = allRows.filter((topic) => {
      const primary = resultRow(db, topic, 'extract');
      return primary?.status === 'complete' && hasPotentialDelta(topic, parseValidation(primary)) && resultRow(db, topic, 'verify')?.status !== 'complete';
    }).length;
    if (missing || unverified) throw new Error(`历史识别未完整：missing=${missing}, unverified=${unverified}`);
    if (scopedRun) {
      const pilotSummary = VIEWPOINT_MODE ? mergeViewpoints(db, allRows, runId) : null;
      refreshRunMetrics(db, runId, allRows.length, 'paused');
      writeReports(db, runId, pilotSummary);
      console.log(JSON.stringify({ status: 'pilot_complete', run_id: runId, topics: allRows.length, summary: pilotSummary }));
      db.close(); return;
    }
    const summary = VIEWPOINT_MODE ? mergeViewpoints(db, allRows, runId) : mergeVerified(db, allRows, runId);
    writeReports(db, runId, summary);
    db.close();
    registerCandidate(runId, summary);
    console.log(JSON.stringify({ status: 'candidate', run_id: runId, summary, next: ['npm run validate', 'npm run diff', 'npm run publish'] }, null, 2));
  } catch (error) {
    refreshRunMetrics(db, runId, allRows.length, 'paused', error.message);
    writeReports(db, runId);
    db.close();
    throw error;
  }
}

function prepare() {
  const created = cloneProduction(hasFlag('restart'));
  const db = new DatabaseSync(CANDIDATE_PATH); ensureAiSchema(db); const topics = db.prepare('SELECT count(*) n FROM topics').get().n; db.close();
  console.log(JSON.stringify({ status: created ? 'created' : 'reused', candidate: CANDIDATE_PATH, topics, provider: PROVIDER, model: MODEL, prompt_version: PROMPT_VERSION }, null, 2));
}

function status() {
  if (!existsSync(CANDIDATE_PATH)) return console.log(JSON.stringify({ status: 'not_prepared' }));
  const db = new DatabaseSync(CANDIDATE_PATH, { readOnly: true });
  const runs = db.prepare("SELECT * FROM ai_enrichment_runs ORDER BY id DESC LIMIT 5").all();
  const latest = runs[0];
  const phases = latest ? db.prepare('SELECT phase,status,count(*) n FROM ai_enrichment_results WHERE run_id=? GROUP BY phase,status').all(latest.id) : [];
  db.close(); console.log(JSON.stringify({ candidate: CANDIDATE_PATH, runs, phases }, null, 2));
}

if (command === 'run') {
  try { await run(); } finally { codexAppServer?.close(); cleanupIsolatedCodexHome(); }
}
else if (command === 'prepare') prepare();
else if (command === 'status') status();
else throw new Error(`未知命令 ${command}`);
