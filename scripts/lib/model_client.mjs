import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve as resolvePath } from 'node:path';
import { homedir, tmpdir } from 'node:os';

export const classificationSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['classification', 'confidence', 'evidence', 'reasons', 'negative_signals'],
  properties: {
    classification: { type: 'string', enum: ['broker_note', 'not_broker_note', 'uncertain'] },
    confidence: { type: 'number', minimum: 0, maximum: 1 },
    evidence: { type: 'string' },
    reasons: { type: 'array', items: { type: 'string' }, maxItems: 8 },
    negative_signals: { type: 'array', items: { type: 'string' }, maxItems: 8 },
  },
};

export const extractionSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['recommendation', 'viewpoints', 'abstain_reason'],
  properties: {
    recommendation: {
      type: 'object', additionalProperties: false,
      required: ['broker', 'research_group', 'authors', 'evidence'],
      properties: {
        broker: { anyOf: [{ type: 'string' }, { type: 'null' }] },
        research_group: { anyOf: [{ type: 'string' }, { type: 'null' }] },
        authors: { type: 'array', items: { type: 'string' }, maxItems: 12 },
        evidence: { anyOf: [{ type: 'string' }, { type: 'null' }] },
      },
    },
    viewpoints: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        required: ['summary', 'direction', 'security_surface', 'security_code_hint', 'security_evidence', 'evidence', 'target_price', 'target_currency', 'target_price_raw', 'target_price_evidence', 'confidence'],
        properties: {
          summary: { type: 'string' },
          direction: { type: 'string', enum: ['bullish', 'bearish'] },
          security_surface: { type: 'string' },
          security_code_hint: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          security_evidence: { type: 'string' },
          evidence: { type: 'string' },
          target_price: { anyOf: [{ type: 'number' }, { type: 'null' }] },
          target_currency: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          target_price_raw: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          target_price_evidence: { anyOf: [{ type: 'string' }, { type: 'null' }] },
          confidence: { type: 'number', minimum: 0, maximum: 1 },
        },
      },
    },
    abstain_reason: { anyOf: [{ type: 'string' }, { type: 'null' }] },
  },
};

export const approvalSchema = {
  type: 'object',
  additionalProperties: false,
  required: ['classification', 'classification_confidence', 'classification_evidence', 'recommendation', 'viewpoints', 'overall_confidence', 'risk_flags'],
  properties: {
    classification: { type: 'string', enum: ['broker_note', 'not_broker_note', 'uncertain'] },
    classification_confidence: { type: 'number', minimum: 0, maximum: 1 },
    classification_evidence: { type: 'string' },
    recommendation: extractionSchema.properties.recommendation,
    viewpoints: extractionSchema.properties.viewpoints,
    overall_confidence: { type: 'number', minimum: 0, maximum: 1 },
    risk_flags: { type: 'array', items: { type: 'string' }, maxItems: 12 },
  },
};

function modelError(message, code, extra = {}) {
  return Object.assign(new Error(message), { code, ...extra });
}

// Validate locally too: CLI schema constraints are not a replacement for the
// boundary check before a provider response is admitted to the database.
export function validateStructuredResult(value, schema, path = '$') {
  const fail = (message) => { throw modelError(`${path}: ${message}`, 'MODEL_SCHEMA_INVALID'); };
  if (!schema || typeof schema !== 'object') fail('缺少 JSON Schema');
  if (schema.anyOf) {
    if (!schema.anyOf.some((candidate) => {
      try { validateStructuredResult(value, candidate, path); return true; } catch { return false; }
    })) fail('不符合任何允许的类型');
  }
  const type = schema.type;
  if (type === 'object' && (value === null || typeof value !== 'object' || Array.isArray(value))) fail('应为对象');
  if (type === 'array' && !Array.isArray(value)) fail('应为数组');
  if (type === 'string' && typeof value !== 'string') fail('应为字符串');
  if (type === 'number' && (typeof value !== 'number' || !Number.isFinite(value))) fail('应为有限数值');
  if (type === 'integer' && !Number.isSafeInteger(value)) fail('应为安全整数');
  if (type === 'boolean' && typeof value !== 'boolean') fail('应为布尔值');
  if (type === 'null' && value !== null) fail('应为空值');
  if (schema.enum && !schema.enum.some((entry) => Object.is(entry, value))) fail('枚举值非法');
  if (Object.hasOwn(schema, 'const') && !Object.is(schema.const, value)) fail('固定值不匹配');
  if (typeof value === 'number') {
    if (schema.minimum !== undefined && value < schema.minimum) fail(`小于最小值 ${schema.minimum}`);
    if (schema.maximum !== undefined && value > schema.maximum) fail(`大于最大值 ${schema.maximum}`);
  }
  if (typeof value === 'string') {
    if (schema.minLength !== undefined && value.length < schema.minLength) fail('字符串过短');
    if (schema.maxLength !== undefined && value.length > schema.maxLength) fail('字符串过长');
    if (schema.pattern && !new RegExp(schema.pattern, 'u').test(value)) fail('字符串格式不符');
  }
  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) fail('数组项不足');
    if (schema.maxItems !== undefined && value.length > schema.maxItems) fail('数组项过多');
    if (schema.uniqueItems && new Set(value.map((item) => JSON.stringify(item))).size !== value.length) fail('数组项重复');
    if (schema.items) value.forEach((item, index) => validateStructuredResult(item, schema.items, `${path}[${index}]`));
  } else if (value !== null && typeof value === 'object') {
    for (const key of schema.required || []) if (!Object.hasOwn(value, key)) fail(`缺少必填字段 ${key}`);
    for (const [key, item] of Object.entries(value)) {
      if (Object.hasOwn(schema.properties || {}, key)) validateStructuredResult(item, schema.properties[key], `${path}.${key}`);
      else if (schema.additionalProperties === false) fail(`含未允许的字段 ${key}`);
      else if (schema.additionalProperties && typeof schema.additionalProperties === 'object') validateStructuredResult(item, schema.additionalProperties, `${path}.${key}`);
    }
  }
  return value;
}

export function classifyModelError(error) {
  const code = String(error?.code || '');
  const text = `${code} ${error?.message || error || ''}`;
  const status = Number(error?.status || error?.statusCode || 0);
  if (code === 'MODEL_BUDGET_EXCEEDED') return { category: 'budget', retryable: false, stopRun: true };
  if (['MODEL_COMPLETION_UNCONFIRMED', 'MODEL_OUTPUT_MISSING'].includes(code)
      || error?.completed && (error?.terminalFailure || code === 'MODEL_PROCESS_ERROR')) {
    return { category: 'protocol', retryable: false, stopRun: true };
  }
  if (status === 401 || status === 403 || /\b40[13]\b|unauthori[sz]ed|authentication|not logged in|login required|invalid.api.key|未登录|认证失败|登录失效/i.test(text)) return { category: 'authentication', retryable: false, stopRun: true };
  if (status === 402 || /\b402\b|insufficient.?balance|insufficient.?quota|quota.{0,20}(?:exceed|exhaust)|usage limit|credit.{0,12}(?:exhaust|insufficient)|余额不足|额度.{0,8}(?:耗尽|不足|用完)/i.test(text)) return { category: 'quota', retryable: false, stopRun: true };
  if (status === 429 || /\b429\b|rate.?limit|too many requests|速率限制/i.test(text)) return { category: 'rate_limit', retryable: true, stopRun: false };
  if (code === 'MODEL_TIMEOUT' || error?.name === 'AbortError' || /timed? ?out|timeout|超时/i.test(text)) return { category: 'timeout', retryable: true, stopRun: false };
  if (code === 'MODEL_SCHEMA_INVALID' || error instanceof SyntaxError || /可解析 JSON|JSON.*(?:parse|invalid)/i.test(text)) return { category: 'schema', retryable: false, stopRun: false };
  if (status >= 500 || /\b50[0234]\b|ECONNRESET|ECONNREFUSED|EAI_AGAIN|ETIMEDOUT|fetch failed|temporar|overloaded|service unavailable/i.test(text)) return { category: 'temporary', retryable: true, stopRun: false };
  if (['MODEL_PREFLIGHT_FAILED', 'MODEL_AUDIT_FAILED', 'MODEL_TOOL_USE'].includes(code) || /model.{0,30}(?:not found|not available|unsupported)|unrecognized model|ENOENT|Error loading config|reserved built-in provider|cannot be overridden|unexpected argument|unknown (?:option|flag)|模型.{0,10}(?:不可用|不存在)/i.test(text)) return { category: 'configuration', retryable: false, stopRun: true };
  return { category: 'unknown', retryable: false, stopRun: false };
}

export function batchModelInputs(items, { maxItems = 10, maxChars = 20000, measure = (item) => typeof item === 'string' ? item.length : JSON.stringify(item).length } = {}) {
  if (!Number.isSafeInteger(maxItems) || maxItems < 1 || !Number.isSafeInteger(maxChars) || maxChars < 1) throw new Error('批次篇数和字符上限必须是正整数');
  const batches = [];
  let batch = []; let chars = 0;
  const flush = () => { if (batch.length) batches.push({ items: batch, chars, oversized: chars > maxChars }); batch = []; chars = 0; };
  for (const item of items) {
    const size = measure(item);
    if (!Number.isSafeInteger(size) || size < 0) throw new Error('文本字符数必须是非负整数');
    if (batch.length && (batch.length >= maxItems || chars + size > maxChars)) flush();
    batch.push(item); chars += size;
    if (chars > maxChars) flush();
  }
  flush();
  return batches;
}

function parseJsonPayload(value) {
  const text = String(value || '').trim();
  try { return JSON.parse(text); } catch {}
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/i)?.[1];
  if (fenced) return JSON.parse(fenced);
  // Some CLI-backed models append a short explanation after an otherwise valid
  // JSON object. Extract only the first complete object while respecting quoted
  // braces, rather than slicing through the final brace in the explanation.
  const start = text.indexOf('{');
  if (start >= 0) {
    let depth = 0;
    let quoted = false;
    let escaped = false;
    for (let index = start; index < text.length; index++) {
      const char = text[index];
      if (quoted) {
        if (escaped) escaped = false;
        else if (char === '\\') escaped = true;
        else if (char === '"') quoted = false;
        continue;
      }
      if (char === '"') quoted = true;
      else if (char === '{') depth++;
      else if (char === '}' && --depth === 0) return JSON.parse(text.slice(start, index + 1));
    }
  }
  throw new Error('模型未返回可解析 JSON');
}

async function callOllama({ model, system, prompt, schema, contextSize = 16384, temperature = 0, timeoutMs = 300000 }) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(process.env.OLLAMA_URL || 'http://127.0.0.1:11434/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model,
        stream: false,
        think: false,
        keep_alive: '30m',
        format: schema,
        options: { temperature, num_ctx: contextSize },
        messages: [
          { role: 'system', content: system },
          { role: 'user', content: prompt },
        ],
      }),
      signal: controller.signal,
    });
    if (!response.ok) throw modelError(`Ollama HTTP ${response.status}: ${(await response.text()).slice(0, 500)}`, 'MODEL_HTTP_ERROR', {
      status: response.status, retryAfterMs: retryAfterMillis(response.headers.get('retry-after')),
    });
    const payload = await response.json();
    return { result: parseJsonPayload(payload.message?.content), raw: payload, durationMs: Number(payload.total_duration || 0) / 1e6,
      actualModel: payload.model || null, usage: { input_tokens: payload.prompt_eval_count ?? null, output_tokens: payload.eval_count ?? null } };
  } finally {
    clearTimeout(timeout);
  }
}

function retryAfterMillis(value) {
  if (!value) return 0;
  const seconds = Number(value);
  return Number.isFinite(seconds) ? Math.max(0, seconds * 1000) : Math.max(0, Date.parse(value) - Date.now()) || 0;
}

function safeErrorText(value) {
  return String(value || '').replace(/Bearer\s+[^\s"']+/gi, 'Bearer [redacted]')
    .replace(/((?:api[_-]?key|access[_-]?token|authorization)["']?\s*[:=]\s*["']?)[^\s,"'}]+/gi, '$1[redacted]').slice(-2000);
}

async function stopChildTree(child) {
  if (!child.pid) return;
  if (process.platform !== 'win32') {
    try { process.kill(-child.pid, 'SIGKILL'); } catch { try { child.kill('SIGKILL'); } catch {} }
    return;
  }
  // Only the process created above and its descendants are targeted. Killing
  // the wrapper alone can leave a CLI worker using quota after a timeout.
  await new Promise((resolve) => {
    const killer = spawn('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true, shell: false, stdio: 'ignore' });
    const timer = setTimeout(() => { try { killer.kill(); child.kill(); } catch {} resolve(); }, 5000);
    const done = () => { clearTimeout(timer); resolve(); };
    killer.once('error', () => { try { child.kill(); } catch {} done(); });
    killer.once('close', done);
  });
}

function spawnCapture(command, args, { timeoutMs = 300000, input = '', captureBoth = false, env = process.env, cwd } = {}) {
  return new Promise((resolve, reject) => {
    const isWindowsScript = process.platform === 'win32' && /\.(?:cmd|bat)$/i.test(command);
    const nativeClaude = isWindowsScript ? resolvePath(dirname(command), 'node_modules/@anthropic-ai/claude-code/bin/claude.exe') : '';
    const executable = nativeClaude && existsSync(nativeClaude) ? nativeClaude : command;
    const childArgs = args;
    const child = spawn(executable, childArgs, { cwd, env, windowsHide: true, shell: false, detached: process.platform !== 'win32', stdio: ['pipe', 'pipe', 'pipe'] });
    let stdout = '';
    let stderr = '';
    let terminating = false;
    const terminate = async (error) => {
      if (terminating) return;
      terminating = true;
      clearTimeout(timer);
      await stopChildTree(child);
      reject(error);
    };
    const timer = setTimeout(() => { void terminate(modelError('模型子进程调用超时，已清理本次进程树', 'MODEL_TIMEOUT')); }, timeoutMs);
    const append = (key, chunk) => {
      if (terminating) return;
      if (key === 'stdout') stdout += chunk.toString(); else stderr += chunk.toString();
      if (stdout.length + stderr.length > 8 * 1024 * 1024) void terminate(modelError('模型子进程输出超过 8MB 上限', 'MODEL_OUTPUT_TOO_LARGE'));
    };
    child.stdout.on('data', (chunk) => append('stdout', chunk));
    child.stderr.on('data', (chunk) => append('stderr', chunk));
    child.stdin.on('error', () => {});
    child.on('error', (error) => { clearTimeout(timer); if (!terminating) reject(error); });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (terminating) return;
      if (code !== 0) reject(modelError(safeErrorText(stderr || stdout || `模型子进程退出码 ${code}`), 'MODEL_PROCESS_ERROR', { stdout, stderr, exitCode: code }));
      else resolve(captureBoth ? { stdout, stderr } : stdout);
    });
    child.stdin.end(input);
  });
}

async function callClaude({ model, system, prompt, schema, timeoutMs = 300000 }) {
  const combined = `${system}\n\n${prompt}\n\n只输出符合以下 JSON Schema 的 JSON，不要输出 Markdown：\n${JSON.stringify(schema)}`;
  const executable = process.env.CLAUDE_CLI || 'claude';
  const args = [
    '-p', '--output-format', 'json', '--no-session-persistence', '--name', 'broker-note-worker', '--prompt-suggestions', 'false',
  ];
  if (model && model !== 'claude-cli-default') args.push('--model', model);
  const stdout = await spawnCapture(executable, args, { timeoutMs, input: combined });
  const envelope = parseJsonPayload(stdout);
  if (envelope.is_error) throw modelError(safeErrorText(envelope.result || envelope.error || 'Claude 返回错误'), 'MODEL_PROVIDER_ERROR');
  const content = envelope.result ?? envelope.content ?? envelope.message ?? envelope;
  const usedModels = Object.keys(envelope.modelUsage || {});
  return { result: typeof content === 'string' ? parseJsonPayload(content) : content, raw: envelope, durationMs: Number(envelope.duration_ms || envelope.durationMs || 0),
    actualModel: envelope.model || (usedModels.length === 1 ? usedModels[0] : null), usage: envelope.usage || null };
}

function codexEnvironment(codexHome) {
  return { ...process.env, CODEX_HOME: resolvePath(codexHome || process.env.CODEX_HOME || join(homedir(), '.codex')) };
}

export function parseCodexEvents(stdout) {
  let actualModel = null; let usage = null; let error = null; let toolUsed = false;
  let terminalFailure = false; let completed = false;
  const warnings = [];
  for (const line of String(stdout).split(/\r?\n/)) {
    let event;
    try { event = JSON.parse(line); } catch { continue; }
    if (typeof event.model === 'string') actualModel = event.model;
    else if (typeof event.model_id === 'string') actualModel = event.model_id;
    else if (typeof event.session?.model === 'string') actualModel = event.session.model;
    if (event.type === 'turn.completed') {
      completed = true;
      if (event.usage) usage = event.usage;
      // Reconnection errors are progress events, not a failed final turn.
      // A real turn.failed is never forgiven, even if later output is malformed.
      if (!terminalFailure) error = null;
    }
    if (event.type === 'turn.failed') {
      terminalFailure = true;
      error = safeErrorText(event.error?.message || event.message || 'Codex 返回失败事件');
    } else if (event.type === 'error') {
      const message = safeErrorText(event.error?.message || event.message || 'Codex 返回错误事件');
      warnings.push(message);
      if (!completed && !terminalFailure) error = message;
    }
    if (['command_execution', 'mcp_tool_call', 'web_search', 'file_change', 'collab_tool_call', 'computer_use'].includes(event.item?.type)) toolUsed = true;
  }
  return { actualModel, usage, error, toolUsed, completed, terminalFailure, warnings };
}

export async function preflightModel({ provider, model, codexHome, codexCliPath, timeoutMs = 15000, probe = spawnCapture, readCatalog } = {}) {
  if (provider !== 'codex') throw modelError(`当前成本可控入口仅配置 Codex 预检；${provider || '未配置提供方'} 请显式配置后再运行`, 'MODEL_PREFLIGHT_FAILED');
  if (!model || model === 'codex-cli-default') throw modelError('预检要求明确的 Codex 模型 ID，不能使用浮动默认模型', 'MODEL_PREFLIGHT_FAILED');
  const env = codexEnvironment(codexHome);
  const codexJs = resolvePath('node_modules/@openai/codex/bin/codex.js');
  const command=codexCliPath||process.execPath,prefix=codexCliPath?[]:[codexJs];
  const options = { env, timeoutMs, captureBoth: true };
  let help; let login;
  try {
    help = await probe(command, [...prefix, 'exec', '--help'], options);
    login = await probe(command, [...prefix, 'login', 'status'], options);
  } catch (error) {
    throw modelError(`Codex 本地预检未通过：${safeErrorText(error.message)}`, 'MODEL_PREFLIGHT_FAILED');
  }
  const helpText = typeof help === 'string' ? help : `${help.stdout}\n${help.stderr}`;
  const loginText = typeof login === 'string' ? login : `${login.stdout}\n${login.stderr}`;
  const flags = ['--json', '--output-schema', '--ephemeral', '--ignore-user-config', '--cd'];
  if (flags.some((flag) => !helpText.includes(flag))) throw modelError('本机 Codex CLI 缺少结构化隔离运行所需参数，请更新 CLI', 'MODEL_PREFLIGHT_FAILED');
  if (!/logged in using chatgpt/i.test(loginText) || /not logged in/i.test(loginText)) throw modelError('本机 Codex 未确认 ChatGPT 登录，请在配置的 Codex 用户目录完成登录', 'MODEL_PREFLIGHT_FAILED');
  let catalog;
  try { catalog = readCatalog ? await readCatalog() : JSON.parse(readFileSync(join(env.CODEX_HOME, 'models_cache.json'), 'utf8')); }
  catch { throw modelError('未找到可读的本地模型目录缓存，无法在零模型调用下确认模型可用性', 'MODEL_PREFLIGHT_FAILED'); }
  const models = Array.isArray(catalog) ? catalog : catalog.models || [];
  const catalogModelListed=models.some((entry) => (typeof entry === 'string' ? entry : entry.slug || entry.id || entry.model) === model);
  if (!catalogModelListed && !codexCliPath) {
    throw modelError(`本地模型目录未列出 ${model}，本次任务未发起模型调用`, 'MODEL_PREFLIGHT_FAILED');
  }
  return { verified: true, verification: catalogModelListed?'local_cli_login_and_cached_catalog':'explicit_cli_login_only', catalogModelListed, provider, requestedModel: model,
    actualModel: null, loggedIn: true, catalogFetchedAt: catalog.fetched_at || null,
    reason: catalogModelListed?'本地 CLI、ChatGPT 登录及缓存目录已确认；实际服务返回的模型和用量在运行时记录。':'显式指定客户端及登录已确认，跨版本共享目录未列出该模型；不保证模型可用，实际调用失败即停止。' };
}

// Only the process runner is injectable. Tests therefore exercise real argv,
// temporary output files, JSONL admission and cleanup, not a bypassed adapter.
export async function callCodex({ model, system, prompt, schema, codexHome, codexCliPath, timeoutMs = 300000, codexRunner = spawnCapture }) {
  const tempDir = mkdtempSync(join(tmpdir(), 'broker-note-codex-'));
  const schemaPath = join(tempDir, 'schema.json');
  const outputPath = join(tempDir, 'output.json');
  const codexJs = resolvePath('node_modules/@openai/codex/bin/codex.js');
  const command=codexCliPath||process.execPath;
  const args = [...(codexCliPath?[]:[codexJs]), 'exec', '--ephemeral', '--ignore-user-config', '--skip-git-repo-check', '--sandbox', 'read-only', '--cd', tempDir,
    '--output-schema', schemaPath, '--output-last-message', outputPath, '--color', 'never', '--json',
    '-c', 'model_reasoning_effort="low"'];
  for (const feature of ['shell_tool', 'unified_exec', 'apps', 'plugins', 'browser_use', 'browser_use_external', 'computer_use', 'in_app_browser', 'multi_agent', ...(codexCliPath?['code_mode','code_mode_only']:['code_mode_host']), 'hooks', 'image_generation', 'view_image', 'skill_search', 'unbounded_connection_retries']) args.push('--disable', feature);
  if (model && model !== 'codex-cli-default') args.push('--model', model);
  args.push('-');
  const started = Date.now();
  let response = null; let primaryFailure = null;
  try {
    writeFileSync(schemaPath, JSON.stringify(schema), 'utf8');
    let stdout; let processFailure = null;
    try {
      const captured = await codexRunner(command, args, { cwd: tempDir, env: codexEnvironment(codexHome), timeoutMs,
        input: `${system}\n\n${prompt}\n\n不要调用任何工具；不要读取目录或文件；只根据以上文本输出符合 Schema 的最终 JSON。` });
      stdout = typeof captured === 'string' ? captured : captured?.stdout || '';
    } catch (error) { processFailure = error; stdout = error.stdout || ''; }
    const metadata = parseCodexEvents(stdout);
    let result = null; let outputFailure = null;
    try { result = parseJsonPayload(readFileSync(outputPath, 'utf8')); }
    catch (error) { outputFailure = error; }
    // Rejected payloads remain available to failure audit, never as an accepted
    // return value. Paid usage and raw structured data must not disappear.
    const diagnostic = { ...metadata, rawResult: result };
    if (metadata.toolUsed) throw modelError('Codex 返回了工具调用，本次结果未接收入库', 'MODEL_TOOL_USE', diagnostic);
    if (processFailure) throw Object.assign(processFailure, diagnostic);
    if (metadata.error) throw modelError(metadata.error, 'MODEL_PROVIDER_ERROR', diagnostic);
    if (!metadata.completed) throw modelError('Codex 未确认本轮完成，结果未接收', 'MODEL_COMPLETION_UNCONFIRMED', diagnostic);
    if (outputFailure) {
      if (outputFailure.code === 'ENOENT') throw modelError('Codex 已完成但没有生成结构化输出文件', 'MODEL_OUTPUT_MISSING', diagnostic);
      throw Object.assign(outputFailure, diagnostic);
    }
    response = { result, raw: null, durationMs: Date.now() - started, actualModel: metadata.actualModel,
      usage: metadata.usage, requestedModel: model || null, warnings: metadata.warnings };
    return response;
  } catch (error) {
    primaryFailure = error;
    throw error;
  } finally {
    try {
      // tempDir was uniquely allocated above and must remain a direct child of
      // the system temp directory before a recursive cleanup is allowed.
      if (dirname(resolvePath(tempDir)) !== resolvePath(tmpdir())) throw new Error('临时目录超出预期范围');
      rmSync(tempDir, { recursive: true, force: true });
    } catch (error) {
      const warning = `临时文件清理失败：${safeErrorText(error.message)}`;
      if (response) response.warnings.push(warning);
      else if (primaryFailure) primaryFailure.cleanupWarnings = [warning];
      // Cleanup cannot turn a successful, paid model result into a retryable call.
    }
  }
}

export async function callStructuredModel(options) {
  const invoke = options.invoke || (options.provider === 'ollama'
    ? () => callOllama(options)
    : options.provider === 'claude'
      ? () => callClaude(options)
      : options.provider === 'codex'
        ? () => callCodex(options)
      : null);
  if (!invoke) throw new Error(`不支持的模型提供方: ${options.provider}`);
  const maxAttempts = Math.max(1, Math.min(3, Math.floor(Number(options.maxAttempts) || 3)));
  const sleep = options.sleep || ((ms) => new Promise((resolve) => setTimeout(resolve, ms)));
  const baseDelayMs = Math.max(0, options.baseDelayMs ?? 1000);
  const maxDelayMs = Math.max(baseDelayMs, options.maxDelayMs ?? 30000);
  let totalDurationMs = 0;
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    const context = { attempt, provider: options.provider, model: options.model, requestedModel: options.model,
      promptChars: String(options.prompt || '').length + String(options.system || '').length };
    try {
      if (await options.beforeAttempt?.(context) === false) throw modelError('本轮模型调用预算已用完', 'MODEL_BUDGET_EXCEEDED');
    } catch (error) {
      if (!error.code) error.code = 'MODEL_BUDGET_EXCEEDED';
      error.errorClass = classifyModelError(error).category; error.stopRun = true;
      throw error;
    }
    const started = Date.now();
    let call = null; let failure = null;
    try {
      call = await invoke({ ...options, attempt });
      validateStructuredResult(call?.result, options.schema);
    } catch (error) {
      failure = error;
    }
    const elapsed = Math.max(0, Math.round(call?.durationMs || Date.now() - started));
    totalDurationMs += elapsed;
    const kind = failure ? classifyModelError(failure) : null;
    // Do not retry earlier than a provider's Retry-After. Long cooldowns are
    // returned to the durable runner instead of blocking this worker forever.
    if (kind?.category === 'rate_limit' && Number(failure.retryAfterMs || 0) > maxDelayMs) {
      kind.retryable = false; kind.stopRun = true;
    }
    // Smaller input cannot repair a transport outage. After bounded retries,
    // return to the durable runner rather than splitting into more paid calls.
    if (failure && attempt === maxAttempts && ['timeout', 'temporary', 'rate_limit'].includes(kind?.category)) kind.stopRun = true;
    const event = { ...context, status: failure ? 'failed' : 'success', durationMs: elapsed,
      actualModel: call?.actualModel || failure?.actualModel || null, usage: call?.usage || failure?.usage || null,
      rawResult: call?.result ?? failure?.rawResult ?? null, error: failure ? safeErrorText(failure.message) : null,
      warnings: call?.warnings || failure?.warnings || [],
      errorClass: kind?.category || null, retryable: kind?.retryable || false, stopRun: kind?.stopRun || false };
    try { await options.onAttempt?.(event); }
    catch (error) { throw modelError(`模型调用审计写入失败，任务已暂停：${safeErrorText(error.message)}`, 'MODEL_AUDIT_FAILED', {
      stopRun: true, retryable: false, rawResult: event.rawResult, actualModel: event.actualModel, usage: event.usage,
      attempts: attempt, providerWarnings: event.warnings,
    }); }
    if (!failure) return { ...call, requestedModel: options.model || null, actualModel: call.actualModel || null,
      usage: call.usage || null, attempts: attempt, durationMs: totalDurationMs };
    Object.assign(failure, { errorClass: kind.category, retryable: kind.retryable, stopRun: kind.stopRun, attempts: attempt });
    if (!kind.retryable || attempt === maxAttempts) throw failure;
    const delay = Math.min(maxDelayMs, Math.max(baseDelayMs * 2 ** (attempt - 1), Number(failure.retryAfterMs || 0)));
    if (delay > 0) await sleep(delay);
  }
}

export function exactEvidenceExists(topic, evidence) {
  const value = String(evidence || '').trim();
  if (!value) return false;
  return `${topic.title || ''}\n${topic.content || ''}`.includes(value);
}

const IGNORABLE_EVIDENCE_CHAR = /[\s，。；：！？、‘’“”（）()【】\[\]]/u;

export function repairExactEvidence(topic, evidence) {
  const source = `${topic.title || ''}\n${topic.content || ''}`;
  const value = String(evidence || '').trim();
  if (!value) return null;
  const candidates = [...new Set([
    value,
    value.replace(/^(?:标题|正文|分类证据|证据)[：:]\s*/u, '').trim(),
    value.replace(/^[“”"']+|[“”"']+$/gu, '').trim(),
    value.replace(/^(?:标题|正文|分类证据|证据)[：:]\s*/u, '').replace(/^[“”"']+|[“”"']+$/gu, '').trim(),
  ].filter(Boolean))];
  for (const candidate of candidates) if (source.includes(candidate)) return candidate;
  const normalizedSource = [];
  const sourceIndex = [];
  for (let index = 0; index < source.length; index++) {
    if (IGNORABLE_EVIDENCE_CHAR.test(source[index])) continue;
    normalizedSource.push(source[index]);
    sourceIndex.push(index);
  }
  for (const candidate of candidates) {
    const normalizedValue = [...candidate].filter((char) => !IGNORABLE_EVIDENCE_CHAR.test(char)).join('');
    if (!normalizedValue) continue;
    const position = normalizedSource.join('').indexOf(normalizedValue);
    if (position < 0) continue;
    const start = sourceIndex[position];
    const end = sourceIndex[position + normalizedValue.length - 1] + 1;
    return source.slice(start, end);
  }
  return null;
}
