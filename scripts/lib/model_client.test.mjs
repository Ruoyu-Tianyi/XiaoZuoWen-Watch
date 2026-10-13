import test from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { batchModelInputs, callStructuredModel, classificationSchema, classifyModelError, extractionSchema,
  parseCodexEvents, preflightModel, validateStructuredResult } from './model_client.mjs';

const valid = () => ({ classification: 'broker_note', confidence: 0.94, evidence: '推荐关注', reasons: [], negative_signals: [] });
const base = { provider: 'codex', model: 'fixed-model', schema: classificationSchema, prompt: '正文', system: '只提取原文' };

test('explicit new CLI may validate login despite stale shared catalog without claiming catalog availability',async()=>{
  const result=await preflightModel({provider:'codex',model:'new-fixture-model',codexCliPath:'C:/fixture/codex.exe',
    readCatalog:async()=>({models:['old-fixture-model']}),probe:async(command,args)=>{
      assert.equal(command,'C:/fixture/codex.exe');
      return {stdout:args.includes('--help')?'--json --output-schema --ephemeral --ignore-user-config --cd':'Logged in using ChatGPT',stderr:''};
    }});
  assert.equal(result.verified,true);assert.equal(result.catalogModelListed,false);
  assert.equal(result.actualModel,null);assert.equal(result.verification,'explicit_cli_login_only');
});

test('explicit newer Codex binary uses isolated invocation and rejects tool use exactly like default client',async()=>{
  const cli=fakeCodexRunner();
  const result=await callStructuredModel({...base,codexCliPath:'C:/fixture/codex.exe',codexRunner:cli.runner});
  assert.equal(result.result.classification,'broker_note');
  assert.equal(cli.calls[0].command,'C:/fixture/codex.exe');
  assert.equal(cli.calls[0].args[0],'exec');
  assert.ok(cli.calls[0].args.includes('code_mode'));cli.assertClean();
  const bad=fakeCodexRunner({events:[{type:'item.completed',item:{type:'mcp_tool_call'}},{type:'turn.completed'}]});
  await assert.rejects(callStructuredModel({...base,codexCliPath:'C:/fixture/codex.exe',codexRunner:bad.runner}),/工具调用/);
  bad.assertClean();
});

test('strict schema checks required fields, types, enum, finite numbers, bounds and extra fields', () => {
  assert.deepEqual(validateStructuredResult(valid(), classificationSchema), valid());
  for (const mutate of [
    (row) => delete row.evidence,
    (row) => { row.confidence = '0.94'; },
    (row) => { row.confidence = NaN; },
    (row) => { row.confidence = 1.1; },
    (row) => { row.classification = 'neutral'; },
    (row) => { row.reasons = Array(9).fill('理由'); },
    (row) => { row.extra = 'unsupported'; },
  ]) {
    const value = valid(); mutate(value);
    assert.throws(() => validateStructuredResult(value, classificationSchema), { code: 'MODEL_SCHEMA_INVALID' });
  }
});

test('strict schema validates nested nullable extraction fields', () => {
  const value = { recommendation: { broker: null, research_group: null, authors: [], evidence: null }, viewpoints: [], abstain_reason: null };
  validateStructuredResult(value, extractionSchema);
  value.recommendation.broker = 123;
  assert.throws(() => validateStructuredResult(value, extractionSchema), /recommendation.broker/);
});

test('every real invocation reserves budget before running and records one outcome', async () => {
  const events = [];
  const result = await callStructuredModel({ ...base,
    beforeAttempt: ({ attempt }) => { events.push(`reserve:${attempt}`); },
    invoke: ({ attempt }) => { events.push(`invoke:${attempt}`); return { result: valid(), durationMs: 12, usage: { input_tokens: 20 }, actualModel: 'fixed-model' }; },
    onAttempt: (event) => { events.push(`audit:${event.attempt}:${event.status}`); assert.equal(event.rawResult.evidence, '推荐关注'); },
  });
  assert.deepEqual(events, ['reserve:1', 'invoke:1', 'audit:1:success']);
  assert.equal(result.attempts, 1);
  assert.equal(result.actualModel, 'fixed-model');
  assert.equal(result.usage.input_tokens, 20);
});

test('budget exhaustion prevents any invocation and is a run-stopping condition', async () => {
  let calls = 0;
  await assert.rejects(callStructuredModel({ ...base, beforeAttempt: () => false,
    invoke: () => { calls++; return { result: valid() }; } }),
  (error) => error.code === 'MODEL_BUDGET_EXCEEDED' && error.stopRun === true);
  assert.equal(calls, 0);
});

test('rate limit retries with bounded delay, and retry consumes its own budget', async () => {
  const events = []; const delays = []; let calls = 0;
  const result = await callStructuredModel({ ...base, baseDelayMs: 10, maxDelayMs: 25,
    beforeAttempt: ({ attempt }) => events.push(`reserve:${attempt}`),
    invoke: () => { calls++; if (calls < 3) throw Object.assign(new Error('HTTP 429'), { retryAfterMs: 20 }); return { result: valid() }; },
    onAttempt: (event) => events.push(`${event.status}:${event.errorClass}`), sleep: (delay) => delays.push(delay),
  });
  assert.equal(result.attempts, 3);
  assert.deepEqual(delays, [20, 20]);
  assert.deepEqual(events, ['reserve:1', 'failed:rate_limit', 'reserve:2', 'failed:rate_limit', 'reserve:3', 'success:null']);
});

test('long Retry-After pauses the run instead of retrying before provider cooldown', async () => {
  let calls = 0;
  await assert.rejects(callStructuredModel({ ...base, maxDelayMs: 30,
    invoke: () => { calls++; throw Object.assign(new Error('HTTP 429'), { retryAfterMs: 60000 }); },
    sleep: () => assert.fail('must return to durable runner') }),
  (error) => error.stopRun === true && error.retryAfterMs === 60000);
  assert.equal(calls, 1);
});

test('retry cannot bypass remaining budget', async () => {
  let calls = 0; let reservations = 0;
  await assert.rejects(callStructuredModel({ ...base, beforeAttempt: () => ++reservations === 1,
    invoke: () => { calls++; throw new Error('HTTP 503'); }, sleep: () => {} }),
  { code: 'MODEL_BUDGET_EXCEEDED' });
  assert.equal(calls, 1);
  assert.equal(reservations, 2);
});

test('authentication and quota failures stop without retries', async () => {
  for (const message of ['HTTP 401 unauthorized', 'HTTP 403', 'HTTP 402 Insufficient Balance', 'insufficient_quota', 'Usage limit reached']) {
    let calls = 0;
    await assert.rejects(callStructuredModel({ ...base,
      invoke: () => { calls++; throw new Error(message); }, sleep: () => assert.fail('must not sleep') }),
    (error) => error.stopRun === true && error.retryable === false);
    assert.equal(calls, 1, message);
  }
});

test('invalid structured results are audited once without blind model retries', async () => {
  const events = []; let calls = 0;
  await assert.rejects(callStructuredModel({ ...base,
    invoke: () => { calls++; return { result: { ...valid(), confidence: 'high' }, usage: { output_tokens: 8 } }; },
    onAttempt: (event) => events.push(event) }), { code: 'MODEL_SCHEMA_INVALID' });
  assert.equal(calls, 1);
  assert.equal(events[0].errorClass, 'schema');
  assert.equal(events[0].usage.output_tokens, 8);
  assert.equal(events[0].rawResult.confidence, 'high');
});

test('audit failure stops instead of reissuing a successful paid call', async () => {
  let calls = 0;
  await assert.rejects(callStructuredModel({ ...base, invoke: () => { calls++; return { result: valid() }; },
    onAttempt: () => { throw new Error('database busy'); } }),
  (error) => error.code === 'MODEL_AUDIT_FAILED' && error.stopRun);
  assert.equal(calls, 1);
});

test('response without reported actual model does not claim requested model was verified', async () => {
  const result = await callStructuredModel({ ...base, invoke: () => ({ result: valid() }) });
  assert.equal(result.actualModel, null);
  assert.equal(result.requestedModel, 'fixed-model');
});

test('error classification distinguishes temporary, schema and unknown failures', () => {
  assert.equal(classifyModelError(new Error('ECONNRESET')).retryable, true);
  assert.equal(classifyModelError(Object.assign(new Error('timeout'), { code: 'MODEL_TIMEOUT' })).category, 'timeout');
  assert.equal(classifyModelError(new SyntaxError('Bad JSON')).retryable, false);
  assert.equal(classifyModelError(new Error('unknown failure')).retryable, false);
  assert.equal(classifyModelError(new Error('Error loading config.toml: model_providers contains reserved built-in provider IDs')).stopRun, true);
});

test('configuration failures consume one attempt and stop the runner', async () => {
  let calls = 0;
  await assert.rejects(callStructuredModel({ ...base,
    invoke: () => { calls++; throw new Error('Error loading config.toml: built-in providers cannot be overridden'); },
    sleep: () => assert.fail('configuration is not transient') }),
  error => error.stopRun === true && error.errorClass === 'configuration');
  assert.equal(calls, 1);
});

test('batch splitting enforces both limits, keeps order and marks long singleton', () => {
  const input = ['123', '45', '678', 'x'.repeat(9), '0', '1', '2'];
  const batches = batchModelInputs(input, { maxItems: 2, maxChars: 6 });
  assert.deepEqual(batches.map((row) => row.items), [['123', '45'], ['678'], ['x'.repeat(9)], ['0', '1'], ['2']]);
  assert.deepEqual(batches.flatMap((row) => row.items), input);
  assert.equal(batches[2].oversized, true);
  assert.equal(batches[2].items.length, 1);
  assert.equal(batches.filter((row) => row.oversized).length, 1);
  assert.throws(() => batchModelInputs(input, { maxItems: 0 }), /正整数/);
});

test('Codex JSONL metadata reads usage without treating requested model as actual', () => {
  const result = parseCodexEvents('noise\n{"type":"thread.started","thread_id":"abc"}\n{"type":"turn.completed","usage":{"input_tokens":30,"output_tokens":10}}');
  assert.equal(result.actualModel, null);
  assert.equal(result.usage.output_tokens, 10);
  assert.equal(result.toolUsed, false);
  assert.equal(parseCodexEvents('{"type":"session.started","session":{"model":"actual"}}').actualModel, 'actual');
  assert.equal(parseCodexEvents('{"type":"item.completed","item":{"type":"command_execution"}}').toolUsed, true);
});

test('Codex recovered connection warnings do not discard a completed paid result', () => {
  const events = [
    { type: 'error', message: 'Reconnecting... 5/5 (request timed out)' },
    { type: 'item.completed', item: { type: 'agent_message', text: '{}' } },
    { type: 'turn.completed', usage: { input_tokens: 12000, output_tokens: 1500 } },
  ];
  const metadata = parseCodexEvents(events.map(JSON.stringify).join('\n'));
  assert.equal(metadata.completed, true);
  assert.equal(metadata.error, null);
  assert.equal(metadata.warnings.length, 1);
  assert.equal(metadata.usage.output_tokens, 1500);
  const failed = parseCodexEvents([...events.slice(0, 1), { type: 'turn.failed', error: { message: 'terminal failure' } }, events.at(-1)].map(JSON.stringify).join('\n'));
  assert.equal(failed.error, 'terminal failure');
  assert.match(parseCodexEvents(JSON.stringify(events[0])).error, /timed out/);
});

test('exhausted transport retries pause instead of allowing smaller paid batches', async () => {
  let calls = 0;
  await assert.rejects(callStructuredModel({ ...base, maxAttempts: 2,
    invoke: () => { calls++; throw new Error('request timed out'); }, sleep: () => {} }),
  error => error.stopRun === true && error.attempts === 2);
  assert.equal(calls, 2);
});

function fakePreflight(overrides = {}) {
  const probes = [];
  return { probes, options: { provider: 'codex', model: 'fixed-model', codexHome: 'C:/mock/codex',
    probe: async (command, args, options) => {
      probes.push({ args, codexHome: options.env.CODEX_HOME });
      return args.includes('--help') ? { stdout: '--json --output-schema --ephemeral --ignore-user-config --cd', stderr: '' }
        : { stdout: '', stderr: 'Logged in using ChatGPT' };
    }, readCatalog: () => ({ models: [{ slug: 'fixed-model' }], fetched_at: '2026-09-12T00:00:00Z' }), ...overrides } };
}

test('preflight uses only local help/login plus model cache and records verification limits', async () => {
  const { probes, options } = fakePreflight();
  const result = await preflightModel(options);
  assert.equal(result.verified, true);
  assert.equal(result.actualModel, null);
  assert.equal(probes.length, 2);
  assert.deepEqual(probes.map((row) => row.args.slice(1)), [['exec', '--help'], ['login', 'status']]);
  assert.ok(probes.every((row) => row.codexHome.replaceAll('\\', '/').endsWith('C:/mock/codex')));
});

test('preflight fails explicitly for missing model or login rather than making a model request', async () => {
  const missing = fakePreflight({ readCatalog: () => ({ models: [] }) });
  await assert.rejects(preflightModel(missing.options), { code: 'MODEL_PREFLIGHT_FAILED' });
  const loggedOut = fakePreflight({ probe: async (command, args) => args.includes('--help')
    ? '--json --output-schema --ephemeral --ignore-user-config --cd' : 'Not logged in' });
  await assert.rejects(preflightModel(loggedOut.options), /未确认 ChatGPT 登录/);
  await assert.rejects(preflightModel({ provider: 'ollama', model: 'anything' }), { code: 'MODEL_PREFLIGHT_FAILED' });
});

function fakeCodexRunner({ events, output = valid(), omitOutput = false, processError = null, auxiliaryFile = false } = {}) {
  const calls = [];
  const jsonl = (events || [
    { type: 'session.started', session: { model: 'actual-model' } },
    { type: 'turn.completed', usage: { input_tokens: 12000, output_tokens: 1500 } },
  ]).map(JSON.stringify).join('\n');
  const runner = async (command, args, options) => {
    const outputPath = args[args.indexOf('--output-last-message') + 1];
    const schemaPath = args[args.indexOf('--output-schema') + 1];
    calls.push({ command, args, options, outputPath, schemaPath, schema: JSON.parse(readFileSync(schemaPath, 'utf8')) });
    if (!omitOutput) writeFileSync(outputPath, typeof output === 'string' ? output : JSON.stringify(output), 'utf8');
    if (auxiliaryFile) writeFileSync(join(options.cwd, 'cli-runtime.log'), 'test runtime artifact', 'utf8');
    if (processError) throw Object.assign(new Error(processError), { code: 'MODEL_PROCESS_ERROR', stdout: jsonl, stderr: processError, exitCode: 1 });
    return jsonl;
  };
  return { calls, runner, assertClean() {
    assert.ok(calls.length > 0);
    for (const call of calls) assert.equal(existsSync(call.options.cwd), false, 'adapter must remove its own temporary directory');
  } };
}

test('real Codex adapter admits recovered final JSON exactly once and persists structured result plus usage', async t => {
  const cli = fakeCodexRunner({ events: [
    { type: 'session.started', session: { model: 'actual-model' } },
    { type: 'error', message: 'Reconnecting... 5/5 (request timed out)' },
    { type: 'item.completed', item: { type: 'agent_message', text: JSON.stringify(valid()) } },
    { type: 'turn.completed', usage: { input_tokens: 12000, output_tokens: 1500 } },
  ], auxiliaryFile: true });
  const db = new DatabaseSync(':memory:'); t.after(() => db.close());
  db.exec('CREATE TABLE audit(status TEXT,raw_json TEXT,usage_json TEXT,model TEXT,warnings_json TEXT); CREATE TABLE accepted(payload TEXT);');
  let reservations = 0;
  const result = await callStructuredModel({ ...base, codexRunner: cli.runner,
    beforeAttempt: () => { reservations++; },
    onAttempt: event => db.prepare('INSERT INTO audit VALUES(?,?,?,?,?)').run(event.status, JSON.stringify(event.rawResult),
      JSON.stringify(event.usage), event.actualModel, JSON.stringify(event.warnings)),
    sleep: () => assert.fail('recovered paid response must not trigger a second CLI request'),
  });
  db.prepare('INSERT INTO accepted VALUES(?)').run(JSON.stringify(result.result));
  assert.equal(cli.calls.length, 1); assert.equal(reservations, 1); assert.equal(result.attempts, 1);
  assert.equal(result.actualModel, 'actual-model'); assert.equal(result.usage.output_tokens, 1500);
  assert.equal(result.warnings.length, 1);
  assert.deepEqual(JSON.parse(db.prepare('SELECT payload FROM accepted').get().payload), valid());
  const audit = db.prepare('SELECT * FROM audit').get();
  assert.equal(audit.status, 'success'); assert.deepEqual(JSON.parse(audit.raw_json), valid());
  assert.equal(JSON.parse(audit.usage_json).input_tokens, 12000); assert.equal(JSON.parse(audit.warnings_json).length, 1);
  const invocation = cli.calls[0];
  assert.equal(invocation.command, process.execPath);
  assert.deepEqual(invocation.schema, classificationSchema);
  assert.equal(invocation.args[invocation.args.indexOf('--cd') + 1], invocation.options.cwd);
  assert.equal(invocation.args[invocation.args.indexOf('--model') + 1], base.model);
  assert.ok(invocation.args.includes('--ignore-user-config')); assert.ok(invocation.args.includes('--json'));
  assert.equal(invocation.args.some(arg => arg.includes('model_providers.')), false);
  assert.match(invocation.options.input, /只提取原文/); assert.match(invocation.options.input, /正文/);
  cli.assertClean();
});

test('real Codex adapter rejects terminal failure despite output file and a conflicting completion event', async () => {
  const cli = fakeCodexRunner({ events: [
    { type: 'turn.failed', error: { message: 'request timed out: final failure' } },
    { type: 'turn.completed', usage: { input_tokens: 20, output_tokens: 10 } },
  ] });
  const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event),
    sleep: () => assert.fail('conflicting terminal events cannot cause paid retries') }),
  error => error.code === 'MODEL_PROVIDER_ERROR' && error.stopRun && !error.retryable);
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].status, 'failed');
  assert.deepEqual(audits[0].rawResult, valid()); assert.equal(audits[0].usage.output_tokens, 10);
  cli.assertClean();
});

test('real Codex adapter does not treat output file alone as completion or repeat the call', async () => {
  const cli = fakeCodexRunner({ events: [{ type: 'item.completed', item: { type: 'agent_message', text: JSON.stringify(valid()) } }] });
  const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event),
    sleep: () => assert.fail('unconfirmed response requires investigation, not repetition') }),
  error => error.code === 'MODEL_COMPLETION_UNCONFIRMED' && error.stopRun);
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].status, 'failed'); assert.deepEqual(audits[0].rawResult, valid());
  cli.assertClean();
});

test('real Codex adapter rejects every forbidden tool category even when final JSON is valid', async () => {
  for (const tool of ['command_execution', 'mcp_tool_call', 'web_search', 'file_change', 'collab_tool_call', 'computer_use']) {
    const cli = fakeCodexRunner({ events: [
      { type: 'item.completed', item: { type: tool } },
      { type: 'turn.completed', usage: { input_tokens: 12, output_tokens: 7 } },
    ] });
    const audits = [];
    await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event) }),
      error => error.code === 'MODEL_TOOL_USE' && error.stopRun && !error.retryable, tool);
    assert.equal(cli.calls.length, 1); assert.equal(audits[0].status, 'failed'); assert.deepEqual(audits[0].rawResult, valid());
    assert.equal(audits[0].usage.output_tokens, 7); cli.assertClean();
  }
});

test('real Codex adapter schema rejection keeps parsed paid output in audit without retry', async () => {
  const invalid = { ...valid(), confidence: 'high' };
  const cli = fakeCodexRunner({ output: invalid }); const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event) }), { code: 'MODEL_SCHEMA_INVALID' });
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].status, 'failed'); assert.deepEqual(audits[0].rawResult, invalid);
  assert.equal(audits[0].usage.output_tokens, 1500); cli.assertClean();
});

test('real Codex adapter missing final file preserves completion usage and stops without retry', async () => {
  const cli = fakeCodexRunner({ omitOutput: true }); const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event) }),
    error => error.code === 'MODEL_OUTPUT_MISSING' && error.stopRun);
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].usage.output_tokens, 1500); assert.equal(audits[0].rawResult, null);
  cli.assertClean();
});

test('real Codex adapter malformed final JSON preserves usage and does not retry paid completion', async () => {
  const cli = fakeCodexRunner({ output: '{ invalid-json' }); const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event) }),
    error => error.errorClass === 'schema' && !error.retryable);
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].usage.output_tokens, 1500); cli.assertClean();
});

test('real Codex adapter nonzero process after completion retains diagnostics but cannot auto-retry', async () => {
  const cli = fakeCodexRunner({ processError: 'timeout during CLI shutdown' }); const audits = [];
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner, onAttempt: event => audits.push(event) }),
    error => error.code === 'MODEL_PROCESS_ERROR' && error.stopRun && !error.retryable);
  assert.equal(cli.calls.length, 1); assert.equal(audits[0].status, 'failed'); assert.deepEqual(audits[0].rawResult, valid());
  assert.equal(audits[0].usage.output_tokens, 1500); cli.assertClean();
});

test('real Codex adapter audit failure carries recoverable paid result and never invokes again', async () => {
  const cli = fakeCodexRunner();
  await assert.rejects(callStructuredModel({ ...base, codexRunner: cli.runner,
    onAttempt: () => { throw new Error('SQLite audit unavailable'); } }),
  error => error.code === 'MODEL_AUDIT_FAILED' && error.stopRun && error.rawResult.confidence === 0.94
    && error.usage.output_tokens === 1500 && error.actualModel === 'actual-model');
  assert.equal(cli.calls.length, 1); cli.assertClean();
});
