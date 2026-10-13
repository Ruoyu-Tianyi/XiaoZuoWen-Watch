import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { ensureWorkflowSchema } from './workflow_db.mjs';
import { runAutomatedPipeline, createBatch } from './automated_pipeline.mjs';
import { runRecentPipeline } from './recent_pipeline.mjs';
import { autoPublishReady, ensureAutoPublishSchema } from './auto_publish.mjs';
import { acquireBatch, reserveCall, ensureAutomationSchema, hash, dailyBudget, DAILY_LIMITS } from './automation_state.mjs';
import { callStructuredModel } from './model_client.mjs';
import { recordApproval, validatePublishBatch, publishAccepted } from './approval_guard.mjs';

const WINDOW_END = '2026-09-12T12:00:00.000Z';
const BODY = '推荐关注宁德时代，目标价300元。';
const TITLE = '【长江证券电新】';
const source = (overrides = {}) => ({ topic_id: '123456789', create_time: '2026-09-12T11:00:00.000Z',
  title: TITLE, content: BODY, images: [], files: [], group: { group_id: '100000001', name: '测试星球' }, ...overrides });
const recommendation = () => ({ broker: '长江证券', research_group: '电新', authors: [], evidence: '长江证券电新' });
const unsigned = () => ({ broker: null, research_group: null, authors: [], evidence: null });
const viewpoint = () => ({ summary: '推荐关注宁德时代，目标价300元', direction: 'bullish', security_surface: '宁德时代',
  security_code_hint: null, security_evidence: '宁德时代', evidence: BODY,
  target_price: 300, target_currency: 'CNY', target_price_raw: '300元', target_price_evidence: '目标价300元', confidence: 0.98 });
const opts = (overrides = {}) => ({ windowEnd: WINDOW_END, hours: 168, maxCalls: 20, maxChars: 500000,
  config: { group_id: '100000001', automation: { model: { provider: 'codex', model: 'fixture-model' },
    review_model: { provider: 'codex', model: 'fixture-model' }, batch_size: 3, batch_chars: 12000, max_attempts: 1 } }, ...overrides });

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'zsxq-automated-pipeline-test-'));
  const db = new DatabaseSync(join(directory, 'fixture.sqlite'));
  db.exec('PRAGMA foreign_keys=ON');
  ensureWorkflowSchema(db);
  ensureAutomationSchema(db);
  db.exec("INSERT INTO securities(id,ts_code,symbol,market,name,synced_at) VALUES(1,'300750.SZ','300750','SZ','宁德时代','2026-09-12')");
  t.after(() => { db.close(); rmSync(directory, { recursive: true, force: true }); });
  return { db, directory };
}

function ordinaryResult(item, stage) {
  const isNews = item.text.includes('快讯：');
  const evidence = isNews ? '快讯：今日市场成交额公布。' : BODY;
  if (stage === 'primary') return { classification: isNews ? 'not_broker_note' : 'broker_note', confidence: 0.98,
    evidence, reasons: ['原文分类证据明确'], negative_signals: [], recommendation: isNews ? unsigned() : recommendation(),
    viewpoints: isNews ? [] : [viewpoint()], abstain_reason: isNews ? '纯快讯，无个股推荐' : null };
  return { classification: item.primary.classification, classification_evidence: evidence, reason: '已逐条核对原文',
    recommendation: item.primary.recommendation,
    decisions: item.candidates.map(candidate => ({ candidate_id: candidate.candidate_id, decision: 'accept',
      reason: '原文推荐、名称及目标价明确', confidence: 0.98, viewpoint: null })), additional_viewpoints: [], uncertainties: [] };
}

function fakeDependencies({ topics = [source()], transform, fetch } = {}) {
  const calls = []; const fetches = []; const preflights = [];
  const dependencies = {
    preflight: async settings => { preflights.push(settings); return { verified: true, actualModel: null }; },
    fetchPage: async (groupId, cursor) => {
      fetches.push({ groupId, cursor });
      return fetch ? fetch({ groupId, cursor, number: fetches.length }) : { topics_brief: topics, has_more: false, next_end_time: null };
    },
    model: options => callStructuredModel({ ...options, sleep: () => assert.fail('fixtures must not wait or retry'),
      invoke: async () => {
        const inputs = JSON.parse(options.prompt.split('以下JSON是待处理数据：\n')[1]);
        const stage = Object.hasOwn(inputs[0] || {}, 'primary') ? 'review' : 'primary';
        calls.push({ stage, topicIds: inputs.map(item => item.topic_id) });
        let result = { items: inputs.map(item => ({ topic_id: item.topic_id, result: ordinaryResult(item, stage) })) };
        if (transform) result = await transform({ stage, inputs, result, callNumber: calls.length });
        return { result, actualModel: 'fixture-model', usage: { input_tokens: 100, output_tokens: 50 }, durationMs: 2 };
      },
    }),
  };
  return { dependencies, calls, fetches, preflights };
}
const count = (db, table) => Number(db.prepare(`SELECT count(*) n FROM ${table}`).get().n);
const batchRow = (db, id) => db.prepare('SELECT * FROM workflow_batches WHERE id=?').get(id);
const topicRow = (db, id) => db.prepare('SELECT * FROM workflow_batch_topics WHERE batch_id=?').get(id);

test('end-to-end fetch, primary, explicit review, accepted state and guarded fixture publication', async t => {
  const { db, directory } = fixture(t), fake = fakeDependencies();
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed', JSON.stringify(run));
  assert.deepEqual(fake.calls.map(call => call.stage), ['primary', 'review']);
  assert.equal(count(db, 'raw_topics'), 1);
  assert.equal(count(db, 'raw_topic_versions'), 1);
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status, 'reviewed');
  assert.equal(db.prepare('SELECT decision,method FROM viewpoint_approval_audit').get().decision, 'accept');
  assert.equal(topicRow(db, run.batch_id).status, 'accepted');
  assert.equal(batchRow(db, run.batch_id).lease_token, null);
  assert.equal(batchRow(db, run.batch_id).calls_used, 2);
  const gate = validatePublishBatch(db);
  assert.equal(gate.ok, true, JSON.stringify(gate.issues));
  const published = await publishAccepted(db, { backupDir: join(directory, 'backups') });
  assert.equal(published.published, 1);
  assert.ok(existsSync(published.backup));
});

test('same-window rerun fetches safely but performs zero new model invocations', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  await runAutomatedPipeline(db, opts(), fake.dependencies);
  const previousCalls = fake.calls.length;
  const rerun = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(rerun.status, 'completed');
  assert.equal(fake.calls.length, previousCalls);
  assert.equal(batchRow(db, rerun.batch_id).calls_used, 0);
  assert.equal(batchRow(db, rerun.batch_id).selected_count, 0);
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(count(db, 'raw_topics'), 1);
});

test('insufficient paired-call budget stops before spending and explicit resume keeps cumulative limits', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  const first = await runAutomatedPipeline(db, opts({ maxCalls: 1 }), fake.dependencies);
  assert.equal(first.status, 'paused');
  assert.equal(topicRow(db, first.batch_id).status, 'queued');
  assert.equal(batchRow(db, first.batch_id).calls_used, 0);
  assert.equal(batchRow(db, first.batch_id).lease_token, null);
  const stillCapped = await runAutomatedPipeline(db, { resume: first.batch_id }, fake.dependencies);
  assert.equal(stillCapped.status, 'paused');
  assert.equal(fake.calls.length, 0);
  const resumed = await runAutomatedPipeline(db, { resume: first.batch_id, maxCalls: 2 }, fake.dependencies);
  assert.equal(resumed.status, 'completed', JSON.stringify(resumed));
  assert.deepEqual(fake.calls.map(call => call.stage), ['primary', 'review']);
  assert.equal(batchRow(db, first.batch_id).calls_used, 2);
});

test('review evidence failure is quarantined for humans without repeating paid primary or review', async t => {
  const { db } = fixture(t); let broken = true;
  const fake = fakeDependencies({ transform: ({ stage, result }) => {
    if (stage === 'review' && broken) result.items[0].result.classification_evidence = '原文没有这句';
    return result;
  } });
  const first = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(first.status, 'completed_with_issues');
  assert.equal(topicRow(db, first.batch_id).status, 'uncertain');
  assert.equal(count(db, 'viewpoints'), 0);
  assert.equal(count(db, 'topic_processing_state'), 1);
  broken = false;
  const resumed = await runAutomatedPipeline(db, { resume: first.batch_id }, fake.dependencies);
  assert.equal(resumed.status, 'completed_with_issues', JSON.stringify(resumed));
  assert.deepEqual(fake.calls.map(call => call.stage), ['primary', 'review']);
  assert.equal(count(db,'workflow_topic_issues'),1);
});

test('non-broker text retains original but ends excluded with no structured viewpoints', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ topics: [source({ title: '快讯', content: '快讯：今日市场成交额公布。' })] });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed', JSON.stringify(run));
  assert.equal(count(db, 'raw_topics'), 1);
  assert.equal(count(db, 'viewpoints'), 0);
  assert.equal(topicRow(db, run.batch_id).status, 'excluded');
  assert.equal(db.prepare('SELECT classification FROM topic_classifications').get().classification, 'not_broker_note');
});

test('empty and attachment topics retain metadata without consuming model calls', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ topics: [source({ content: '' }), source({ topic_id: '987654321', images: [{}] })] });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed');
  assert.equal(fake.calls.length, 0);
  assert.equal(fake.preflights.length, 0);
  assert.equal(count(db, 'raw_topics'), 2);
  assert.equal(db.prepare("SELECT count(*) n FROM raw_topics WHERE eligibility='excluded'").get().n, 2);
});

test('missing and mismatched topic IDs never count as completed topic processing', async t => {
  for (const mode of ['missing', 'mismatch']) {
    await t.test(mode, async child => {
      const { db } = fixture(child), fake = fakeDependencies({ transform: ({ result }) => {
        if (mode === 'missing') delete result.items[0].topic_id; else result.items[0].topic_id = '999999';
        return result;
      } });
      const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
      assert.equal(run.status, 'paused');
      assert.equal(topicRow(db, run.batch_id).status, 'failed');
      assert.equal(count(db, 'viewpoints'), 0);
      assert.equal(count(db, 'topic_processing_state'), 0);
      assert.equal(count(db, 'workflow_model_cache'), 0);
      if (mode === 'missing') assert.equal(db.prepare('SELECT status,error_class FROM model_call_ledger').get().error_class, 'schema');
    });
  }
});

test('batch response IDs and item count are constrained before a model is invoked', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ topics: [source(), source({ topic_id: '999888777', content: `${BODY}第二篇。` })] });
  const original = fake.dependencies.model;
  let checked = 0;
  fake.dependencies.model = options => {
    const inputs = JSON.parse(options.prompt.split('以下JSON是待处理数据：\n')[1]);
    const items = options.schema.properties.items;
    assert.equal(items.minItems, inputs.length);
    assert.equal(items.maxItems, inputs.length);
    assert.deepEqual(items.items.properties.topic_id.enum, inputs.map(x => x.topic_id));
    checked++;
    return original(options);
  };
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed', JSON.stringify(run));
  assert.equal(checked, 2);
});

test('batch structural failures pause after one paid call without single-topic fanout', async t => {
  for (const mode of ['wrong-id', 'duplicate-id', 'missing-item', 'schema']) {
    await t.test(mode, async child => {
      const { db } = fixture(child); let broken = true;
      const fake = fakeDependencies({ topics: [source(), source({ topic_id: '999888777', content: `${BODY}第二篇。` })],
        transform: ({ result }) => {
          if (!broken) return result;
          if (mode === 'wrong-id') result.items[1].topic_id = '999888778';
          if (mode === 'duplicate-id') result.items[1].topic_id = result.items[0].topic_id;
          if (mode === 'missing-item') result.items.pop();
          if (mode === 'schema') result.items[1].result.confidence = 'high';
          return result;
        } });
      const failed = await runAutomatedPipeline(db, opts(), fake.dependencies);
      assert.equal(failed.status, 'paused');
      assert.equal(fake.calls.length, 1);
      assert.equal(batchRow(db, failed.batch_id).calls_used, 1);
      assert.equal(count(db, 'workflow_model_cache'), 0);
      assert.equal(count(db, 'viewpoints'), 0);
      assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='failed'").get().n, 2);
      broken = false;
      const resumed = await runAutomatedPipeline(db, { resume: failed.batch_id }, fake.dependencies);
      assert.equal(resumed.status, 'completed', JSON.stringify(resumed));
      assert.deepEqual(fake.calls.map(x => x.stage), ['primary', 'primary', 'review']);
      assert.equal(batchRow(db, failed.batch_id).calls_used, 3);
    });
  }
});

test('malformed primary schema and fabricated classification evidence fail closed', async t => {
  for (const mode of ['schema', 'evidence']) {
    await t.test(mode, async child => {
      const { db } = fixture(child), fake = fakeDependencies({ transform: ({ result }) => {
        if (mode === 'schema') result.items[0].result.confidence = 'high'; else result.items[0].result.evidence = '不存在的分析';
        return result;
      } });
      const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
      assert.equal(run.status, mode==='schema'?'paused':'completed_with_issues');
      assert.equal(topicRow(db, run.batch_id).status, mode==='schema'?'failed':'uncertain');
      assert.equal(count(db, 'viewpoints'), 0);
      assert.equal(count(db, 'topic_processing_state'), mode==='schema'?0:1);
      assert.equal(count(db, 'workflow_model_cache'), 0);
    });
  }
});

test('existing human acceptance is preserved and reconciliation creates an issue instead of replacing it', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  await runAutomatedPipeline(db, opts(), fake.dependencies);
  const before = db.prepare('SELECT * FROM viewpoints').get();
  recordApproval(db, { viewpointId: before.id, decision: 'accept', method: 'human', scopeDecision: 'broker_note', reason: '人工确认保留原观点' });
  // Recreate legacy state: a human-approved topic without the new processing checkpoint.
  db.exec('DELETE FROM topic_processing_state');
  const rerun = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(rerun.status, 'completed_with_issues', JSON.stringify(rerun));
  assert.deepEqual(db.prepare('SELECT * FROM viewpoints').get(), before);
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(topicRow(db, rerun.batch_id).status, 'uncertain');
  assert.match(db.prepare('SELECT reason FROM workflow_topic_issues').get().reason, /已有人工/);
});

test('fetch failure resumes its saved cursor and retains previously committed raw pages', async t => {
  const { db } = fixture(t); let failSecondPage = true;
  const fake = fakeDependencies({ fetch: ({ cursor }) => {
    if (!cursor) return { topics_brief: [source()], has_more: true, next_end_time: 'page-2' };
    if (failSecondPage) throw new Error('network fixture stopped');
    return { topics_brief: [], has_more: false, next_end_time: null };
  } });
  const first = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(first.status, 'paused');
  assert.equal(count(db, 'raw_topics'), 1);
  assert.equal(batchRow(db, first.batch_id).fetch_cursor, 'page-2');
  assert.equal(fake.calls.length, 0);
  failSecondPage = false;
  const second = await runAutomatedPipeline(db, { resume: first.batch_id }, fake.dependencies);
  assert.equal(second.status, 'completed', JSON.stringify(second));
  assert.deepEqual(fake.fetches.map(item => item.cursor), [null, 'page-2', 'page-2']);
  assert.equal(count(db, 'raw_topics'), 1);
});

test('completed batch resume is a no-op and terminal runs release their lease', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  const first = await runAutomatedPipeline(db, opts(), fake.dependencies);
  const calls = fake.calls.length, fetches = fake.fetches.length;
  const resumed = await runAutomatedPipeline(db, { resume: first.batch_id }, fake.dependencies);
  assert.equal(resumed.reused, true);
  assert.equal(fake.calls.length, calls);
  assert.equal(fake.fetches.length, fetches);
  assert.equal(batchRow(db, first.batch_id).lease_token, null);
  assert.equal(batchRow(db, first.batch_id).lease_pid, null);
});

test('resume can adjust grouping without changing frozen topics, model, identity or budget', async t => {
  const { db } = fixture(t);
  const fake = fakeDependencies({ topics: Array.from({ length: 6 }, (_, i) => source({ topic_id: String(123456780 + i) })) });
  const first = await runAutomatedPipeline(db, opts({ maxCalls: 2 }), {
    ...fake.dependencies, preflight: async () => ({ verified: false, reason: 'fixture pause before paid call' }),
  });
  assert.equal(first.status, 'paused');
  const before = batchRow(db, first.batch_id);
  assert.equal(before.selected_count, 6);
  const result = await runAutomatedPipeline(db, { resume: first.batch_id, batchSize: 6 }, fake.dependencies);
  assert.equal(result.status, 'completed', JSON.stringify(result));
  const after = batchRow(db, first.batch_id);
  assert.deepEqual(fake.calls.map(call => call.topicIds.length), [6, 6]);
  assert.equal(after.calls_used, 2);
  assert.equal(after.max_calls, before.max_calls);
  assert.equal(after.selected_count, before.selected_count);
  assert.equal(after.config_hash, before.config_hash);
  assert.equal(after.error, null);
});

test('live lease blocks concurrent worker; dead worker recovery marks reserved call interrupted', t => {
  const { db } = fixture(t), first = createBatch(db, opts()), second = createBatch(db, opts());
  const lease = acquireBatch(db, first.id, { isAlive: () => true });
  const callId = reserveCall(db, first.id, { stage: 'primary', provider: 'codex', model: 'fixture-model', chars: 100 });
  assert.throws(() => acquireBatch(db, second.id, { isAlive: () => true }), /仍在运行/);
  assert.equal(batchRow(db, first.id).lease_token, lease);
  const secondLease = acquireBatch(db, second.id, { isAlive: () => false });
  assert.ok(secondLease);
  assert.equal(batchRow(db, first.id).status, 'paused');
  assert.equal(batchRow(db, first.id).lease_token, null);
  assert.equal(db.prepare('SELECT status FROM model_call_ledger WHERE id=?').get(callId).status, 'interrupted');
  assert.equal(batchRow(db, first.id).calls_used, 1);
});

test('invalid resume limits leave paused batch unleased and preserve its existing budget', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  const batch = createBatch(db, opts());
  await assert.rejects(runAutomatedPipeline(db, { resume: batch.id, maxCalls: 0 }, fake.dependencies), /参数必须/);
  const after = batchRow(db, batch.id);
  assert.equal(after.status, 'paused');
  assert.equal(after.lease_token, null);
  assert.equal(after.lease_pid, null);
  assert.equal(after.max_calls, batch.max_calls);
  assert.equal(fake.calls.length, 0);
});

test('concurrent resume cannot alter a live worker budget before lock rejection', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  const batch = createBatch(db, opts()), lease = acquireBatch(db, batch.id);
  await assert.rejects(runAutomatedPipeline(db, { resume: batch.id, maxCalls: 100 }, fake.dependencies), /仍在运行/);
  assert.equal(batchRow(db, batch.id).lease_token, lease);
  assert.equal(batchRow(db, batch.id).max_calls, batch.max_calls);
  assert.equal(fake.calls.length, 0);
});

test('excluding a non-security candidate does not block a valid viewpoint in the same note', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ topics: [source({ content: `${BODY}黄金价格上涨。` })],
    transform: ({ stage, result }) => {
      const value = result.items[0].result;
      if (stage === 'primary') value.viewpoints.push({ ...viewpoint(), summary: '黄金价格上涨', security_surface: '黄金',
        security_evidence: '黄金', evidence: '黄金价格上涨。', target_price: null, target_currency: null,
        target_price_raw: null, target_price_evidence: null });
      else value.decisions[1] = { candidate_id: 'p1', decision: 'exclude', reason: '黄金属于商品不是证券', confidence: 0.99, viewpoint: null };
      return result;
    } });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed', JSON.stringify(run));
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status, 'reviewed');
  assert.equal(validatePublishBatch(db).ok, true);
  const audit = JSON.parse(db.prepare('SELECT raw_result_json FROM extraction_results').get().raw_result_json);
  assert.equal(audit.review.decisions[1].decision, 'exclude');
});

test('merging duplicate company candidates must preserve a reviewer uncertain decision', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ transform: ({ stage, result }) => {
    if (stage === 'primary') result.items[0].result.viewpoints.push({ ...viewpoint(), summary: '关于宁德时代的第二段意见' });
    else result.items[0].result.decisions[1] = { candidate_id: 'p1', decision: 'uncertain',
      reason: '两段建议是否同一推荐方存在疑问', confidence: 0.97, viewpoint: null };
    return result;
  } });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed_with_issues', JSON.stringify(run));
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status, 'candidate');
  assert.equal(db.prepare('SELECT decision FROM viewpoint_approval_audit ORDER BY id DESC LIMIT 1').get().decision, 'uncertain');
});

test('same-company same-direction target conflict cannot be hidden by accepting the first candidate', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ topics: [source({ content: `${BODY}另一段目标价350元。` })],
    transform: ({ stage, result }) => {
      if (stage === 'primary') result.items[0].result.viewpoints.push({ ...viewpoint(), summary: '宁德时代另一段目标价350元',
        target_price: 350, target_price_raw: '350元', target_price_evidence: '目标价350元' });
      return result;
    } });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'completed_with_issues', JSON.stringify(run));
  assert.equal(count(db, 'viewpoints'), 1);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status, 'candidate');
  assert.equal(db.prepare('SELECT decision FROM viewpoint_approval_audit ORDER BY id DESC LIMIT 1').get().decision, 'uncertain');
});

function mutateOriginal(db, content) {
  db.prepare('UPDATE raw_topics SET content=?,content_hash=? WHERE topic_id=?').run(content, hash(`${TITLE}\n${content}`), source().topic_id);
}

test('resume rejects a changed current original before spending on a stale-version review', async t => {
  const { db } = fixture(t), fake = fakeDependencies();
  const first = await runAutomatedPipeline(db, opts(), {...fake.dependencies,log:event=>{
    if(event.stage==='primary')db.prepare('UPDATE workflow_batches SET stop_requested=1').run();
  }});
  assert.equal(first.status, 'paused');
  mutateOriginal(db, `${BODY}后续说明：推荐逻辑已经变化。`);
  const resumed = await runAutomatedPipeline(db, { resume: first.batch_id, maxCalls: 3 }, fake.dependencies);
  assert.equal(resumed.status, 'paused', JSON.stringify(resumed));
  assert.equal(fake.calls.length, 1, 'stale input should be rejected before another paid call');
  assert.equal(topicRow(db, first.batch_id).status, 'failed');
  assert.equal(count(db, 'viewpoints'), 0);
  assert.equal(count(db, 'topic_processing_state'), 0);
});

test('small chunks finish review before new primary work and reserve the last call', async t => {
  const { db } = fixture(t), fake = fakeDependencies({topics:Array.from({length:7},(_,i)=>source({
    topic_id:String(700000000+i),content:`${BODY}样本${i}。`} ))});
  const first=await runAutomatedPipeline(db,opts({maxCalls:3}),fake.dependencies);
  assert.equal(first.status,'paused');
  assert.deepEqual(fake.calls.map(c=>c.stage),['primary','review']);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='accepted'").get().n,3);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='extracted'").get().n,0);
  const resumed=await runAutomatedPipeline(db,{resume:first.batch_id,maxCalls:6},fake.dependencies);
  assert.equal(resumed.status,'completed',JSON.stringify(resumed));
  assert.deepEqual(fake.calls.map(c=>c.stage),['primary','review','primary','review','primary','review']);
  assert.equal(count(db,'viewpoints'),7);
});

test('prepare-only saves the full window without probing or calling a model',async t=>{
  const {db}=fixture(t),fake=fakeDependencies();
  const prepared=await runAutomatedPipeline(db,opts({prepareOnly:true}),fake.dependencies);
  assert.equal(prepared.status,'prepared');assert.equal(prepared.selected_count,1);
  assert.equal(fake.calls.length,0);assert.equal(fake.preflights.length,0);
  const resumed=await runAutomatedPipeline(db,{resume:prepared.batch_id},fake.dependencies);
  assert.equal(resumed.status,'completed');assert.equal(fake.fetches.length,1);
});

test('temporary fetch errors retry the same cursor, but authentication failure never retries',async t=>{
  const {db}=fixture(t);
  const fake=fakeDependencies({fetch:({number})=>{
    if(number<3)throw new Error('connectex: connection attempt timed out');
    return {topics_brief:[source()],has_more:false,next_end_time:null};
  }});
  const delays=[];
  const result=await runAutomatedPipeline(db,opts({prepareOnly:true}),{...fake.dependencies,sleep:async ms=>delays.push(ms)});
  assert.equal(result.status,'prepared');assert.deepEqual(delays,[1000,2000]);
  assert.deepEqual(fake.fetches.map(f=>f.cursor),[null,null,null]);assert.equal(fake.calls.length,0);
  const auth=fakeDependencies({fetch:()=>{throw new Error('401 unauthorized connection timeout');}});
  const rejected=await runAutomatedPipeline(db,opts(),{...auth.dependencies,sleep:()=>assert.fail('no auth retries')});
  assert.equal(rejected.status,'paused');assert.equal(auth.fetches.length,1);assert.equal(auth.calls.length,0);
});

test('one invalid evidence topic is isolated while unrelated chunks continue automatically',async t=>{
  const {db}=fixture(t),fake=fakeDependencies({topics:Array.from({length:4},(_,i)=>source({topic_id:String(800000000+i),content:`${BODY}样本${i}`})),
    transform:({stage,result})=>{if(stage==='review'&&result.items.some(i=>i.topic_id==='800000000'))
      result.items.find(i=>i.topic_id==='800000000').result.classification_evidence='伪造原文证据';return result;}});
  const result=await runAutomatedPipeline(db,opts(),fake.dependencies);
  assert.equal(result.status,'completed_with_issues');
  assert.deepEqual(fake.calls.map(c=>c.stage),['primary','review','primary','review']);
  assert.equal(count(db,'viewpoints'),3);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='uncertain'").get().n,1);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_model_cache WHERE stage='review'").get().n,3);
});

test('legacy failed evidence output is quarantined on resume without paying again',async t=>{
  const {db}=fixture(t),fake=fakeDependencies();
  const first=await runAutomatedPipeline(db,opts({prepareOnly:true}),fake.dependencies);
  db.prepare("UPDATE workflow_batch_topics SET status='failed',error='审核分类证据不在原文' WHERE batch_id=?").run(first.batch_id);
  const result=await runAutomatedPipeline(db,{resume:first.batch_id},fake.dependencies);
  assert.equal(result.status,'completed_with_issues');
  assert.equal(fake.calls.length,0);assert.equal(count(db,'workflow_topic_issues'),1);
});

test('compact review keeps complete primary viewpoints without duplicated candidate copies',async t=>{
  const {db}=fixture(t),fake=fakeDependencies({transform:({stage,inputs,result})=>{
    if(stage==='review')for(const item of inputs){
      assert.equal(item.primary.viewpoints.length,1);
      assert.equal(Object.hasOwn(item.candidates[0],'viewpoint'),false);
      assert.equal(item.candidates[0].candidate_id,'p0');
    }
    return result;
  }});
  const result=await runAutomatedPipeline(db,opts(),fake.dependencies);
  assert.equal(result.status,'completed');
  assert.match(db.prepare("SELECT prompt_version FROM workflow_model_cache WHERE stage='review'").get().prompt_version,/compact-input-v1/);
});

test('review uses its own character grouping instead of fragmenting on primary input limits',async t=>{
  const {db}=fixture(t),fake=fakeDependencies({topics:Array.from({length:3},(_,i)=>source({topic_id:String(900000000+i),content:`${BODY}样本${i}`}))});
  const options=opts();options.config.automation.batch_chars=500;
  options.config.automation.review_batch_chars=40000;
  const result=await runAutomatedPipeline(db,options,fake.dependencies);
  assert.equal(result.status,'completed',JSON.stringify(result));
  assert.deepEqual(fake.calls.map(c=>[c.stage,c.topicIds.length]),[['primary',3],['review',3]]);
});

test('enabled automation publishes reviewed closed topics with backup and atomic audit',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies();
  const result=await runAutomatedPipeline(db,opts({autoPublish:true}),{...fake.dependencies,backupDir:join(directory,'backups')});
  assert.equal(result.status,'completed',JSON.stringify(result));
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'published');
  const event=db.prepare("SELECT * FROM workflow_auto_publish_events WHERE status='published'").get();
  assert.equal(event.published_count,1);assert.ok(existsSync(event.backup_path));
  assert.equal(count(db,'viewpoint_publish_batches'),1);
  const rerun=await autoPublishReady(db,result.batch_id,{backupDir:join(directory,'backups')});
  assert.equal(rerun.published,0);assert.equal(count(db,'viewpoint_publish_batches'),1);
  assert.equal(fake.calls.length,2,'publication must not make a third model call');
});

test('completed small chunk can publish while other queued topics remain unprocessed',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies({topics:[source(),source({topic_id:'987654321',content:`${BODY}第二篇`})]});
  const run=await runAutomatedPipeline(db,opts({autoPublish:true,batchSize:1,maxCalls:2}),{...fake.dependencies,backupDir:join(directory,'backups')});
  assert.equal(run.status,'paused');assert.equal(fake.calls.length,2);
  assert.equal(db.prepare("SELECT count(*) n FROM viewpoints WHERE review_status='published'").get().n,1);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='queued'").get().n,1);
});

test('automatic publisher never replaces a human approval or publishes it as AI-reviewed',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies();
  const run=await runAutomatedPipeline(db,opts(),fake.dependencies);
  const id=db.prepare('SELECT id FROM viewpoints').get().id;
  recordApproval(db,{viewpointId:id,decision:'accept',method:'human',scopeDecision:'broker_note',reason:'人工决定'});
  const result=await autoPublishReady(db,run.batch_id,{backupDir:join(directory,'backups')});
  assert.equal(result.published,0);assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'reviewed');
  assert.equal(db.prepare('SELECT method FROM viewpoint_approval_audit ORDER BY id DESC').get().method,'human');
});

test('automatic publication holds lower confidence and topic uncertainty for humans',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies({transform:({stage,result})=>{
    if(stage==='review')result.items[0].result.decisions[0].confidence=.92;return result;
  }});
  const result=await runAutomatedPipeline(db,opts(),fake.dependencies);
  let published=await autoPublishReady(db,result.batch_id,{backupDir:join(directory,'backups')});
  assert.equal(published.published,0);assert.match(published.blocked[0].reason,/置信度/);
  const id=db.prepare('SELECT id FROM viewpoints').get().id;
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'candidate');
  recordApproval(db,{viewpointId:id,decision:'accept',method:'ai',scopeDecision:'broker_note',reason:'fixture',modelId:'fixture-model',metadata:{confidence:.99}});
  db.prepare("UPDATE viewpoints SET review_status='reviewed' WHERE id=?").run(id);
  db.prepare("INSERT INTO workflow_topic_issues(topic_id,batch_id,content_hash,reason,created_at) SELECT topic_id,?,content_hash,'fixture uncertainty','2026-09-12' FROM raw_topics").run(result.batch_id);
  published=await autoPublishReady(db,result.batch_id,{backupDir:join(directory,'backups')});
  assert.equal(published.published,0);assert.match(published.blocked[0].reason,/主题级疑点/);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'candidate');
});

test('publication never sweeps legacy reviewed rows outside the specified workflow batch',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies();
  const first=await runAutomatedPipeline(db,opts(),fake.dependencies);
  const second=await runAutomatedPipeline(db,opts(),fake.dependencies);
  const result=await autoPublishReady(db,second.batch_id,{backupDir:join(directory,'backups')});
  assert.equal(result.published,0);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'reviewed');
  assert.equal(batchRow(db,first.batch_id).calls_used,2);
});

test('automatic publication aborts without changing views if backup or atomic audit fails',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies();
  const run=await runAutomatedPipeline(db,opts(),fake.dependencies);
  await assert.rejects(autoPublishReady(db,run.batch_id,{backupDir:join(directory,'fixture.sqlite')}));
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'reviewed');
  ensureAutoPublishSchema(db);
  db.exec("CREATE TRIGGER fixture_fail_auto_audit BEFORE INSERT ON workflow_auto_publish_events BEGIN SELECT RAISE(ABORT,'fixture audit failed'); END");
  await assert.rejects(autoPublishReady(db,run.batch_id,{backupDir:join(directory,'backups')}),/fixture audit failed/);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'reviewed');
  assert.equal(count(db,'viewpoint_publish_batches'),0);
});

test('topic doubt introduced during backup blocks publication even with unchanged viewpoint fingerprints',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies();
  const run=await runAutomatedPipeline(db,opts(),fake.dependencies);
  const publishing=autoPublishReady(db,run.batch_id,{backupDir:join(directory,'backups')});
  queueMicrotask(()=>db.prepare("INSERT INTO workflow_topic_issues(topic_id,batch_id,content_hash,reason,created_at) SELECT topic_id,?,content_hash,'new doubt','2026-09-12' FROM raw_topics").run(run.batch_id));
  await assert.rejects(publishing,/疑点状态已变化/);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints').get().review_status,'reviewed');
  assert.equal(count(db,'viewpoint_publish_batches'),0);
});

test('single-topic incomplete adjudication is quarantined without publishing that topic',async t=>{
  const {db,directory}=fixture(t),fake=fakeDependencies({transform:({stage,result})=>{
    if(stage==='review')result.items[0].result.decisions=[];return result;
  }});
  const result=await runAutomatedPipeline(db,opts({autoPublish:true}),{...fake.dependencies,backupDir:join(directory,'backups')});
  assert.equal(result.status,'completed_with_issues');
  assert.equal(topicRow(db,result.batch_id).status,'uncertain');
  assert.equal(count(db,'viewpoints'),0);assert.equal(count(db,'viewpoint_publish_batches'),0);
  assert.equal(fake.calls.length,2);
});

test('Shanghai daily budget counts failed calls across batches and cannot be reset by a new batch',t=>{
  const {db}=fixture(t),first=createBatch(db,opts()),second=createBatch(db,opts());
  db.prepare('UPDATE workflow_batches SET max_calls=100').run();
  for(let i=0;i<DAILY_LIMITS.max_calls;i++){
    const id=reserveCall(db,i%2?first.id:second.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:100});
    db.prepare("UPDATE model_call_ledger SET status='failed' WHERE id=?").run(id);
  }
  const third=createBatch(db,opts());
  assert.equal(dailyBudget(db).calls_remaining,0);
  assert.throws(()=>reserveCall(db,third.id,{stage:'review',provider:'codex',model:'fixture-model',chars:1}),/跨批次模型调用上限/);
  assert.equal(batchRow(db,third.id).calls_used,0);
  const boundary=Date.parse('2026-10-12T16:00:00Z');
  assert.equal(dailyBudget(db,boundary-1).day,'2026-10-12');
  assert.equal(dailyBudget(db,boundary).day,'2026-10-13');
});

test('explicit unlimited batch bypasses both call caps but does not exempt other batches',t=>{
  const {db}=fixture(t),normal=createBatch(db,opts({maxCalls:100}));
  for(let i=0;i<40;i++)reserveCall(db,normal.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:1});
  const unlimited=createBatch(db,opts({unlimitedCalls:true}));
  assert.equal(unlimited.max_calls,0);
  for(let i=0;i<45;i++)reserveCall(db,unlimited.id,{stage:'review',provider:'codex',model:'fixture-model',chars:1});
  assert.equal(batchRow(db,unlimited.id).calls_used,45);
  assert.throws(()=>reserveCall(db,normal.id,{stage:'review',provider:'codex',model:'fixture-model',chars:1}),/跨批次模型调用上限/);
  assert.throws(()=>reserveCall(db,unlimited.id,{stage:'review',provider:'codex',model:'fixture-model',chars:600000}),/字符上限/);
});

test('recent entry can explicitly resume an unlimited batch after the usual daily call cap',async t=>{
  const {db}=fixture(t),fake=fakeDependencies();
  const prior=createBatch(db,opts({maxCalls:100}));
  for(let i=0;i<40;i++)reserveCall(db,prior.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:1});
  const prepared=await runAutomatedPipeline(db,opts({prepareOnly:true}),fake.dependencies);
  const result=await runRecentPipeline(db,{windowEnd:WINDOW_END,unlimitedCalls:true},fake.dependencies);
  assert.equal(result.status,'completed',JSON.stringify(result));assert.equal(result.batch_id,prepared.batch_id);
  assert.equal(result.daily_budget.max_calls,null);assert.equal(fake.calls.length,2);
  assert.equal(batchRow(db,result.batch_id).calls_used,2);
});

test('raising explicit daily character budget keeps previous usage and does not change other batches',t=>{
  const {db}=fixture(t),prior=createBatch(db,opts({maxChars:1000000}));
  reserveCall(db,prior.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:600000});
  const current=createBatch(db,opts({unlimitedCalls:true,maxChars:1000000,dailyMaxChars:1000000}));
  reserveCall(db,current.id,{stage:'review',provider:'codex',model:'fixture-model',chars:100000});
  assert.equal(dailyBudget(db).input_chars_used,700000);
  assert.throws(()=>reserveCall(db,prior.id,{stage:'review',provider:'codex',model:'fixture-model',chars:1}),/字符上限/);
});

test('two fast workers process distinct chunks and serialize publication without duplicate paid calls',async t=>{
  const {db,directory}=fixture(t);let active=0,maxActive=0;
  const fake=fakeDependencies({topics:Array.from({length:4},(_,i)=>source({topic_id:String(600000000+i),content:`${BODY}并发样本${i}`})),
    transform:async({result})=>{active++;maxActive=Math.max(maxActive,active);await new Promise(r=>setTimeout(r,15));active--;return result;}});
  const result=await runAutomatedPipeline(db,opts({unlimitedCalls:true,concurrency:2,batchSize:1,autoPublish:true}),{...fake.dependencies,backupDir:join(directory,'backups')});
  assert.equal(result.status,'completed',JSON.stringify(result));assert.equal(maxActive,2);
  assert.equal(fake.calls.length,8);
  assert.equal(new Set(fake.calls.map(c=>c.stage+':'+c.topicIds.join(','))).size,8);
  assert.equal(db.prepare("SELECT count(*) n FROM viewpoints WHERE review_status='published'").get().n,4);
  assert.equal(db.prepare("SELECT count(*) n FROM model_call_ledger WHERE status='running'").get().n,0);
});

test('fast worker failure waits for in-flight calls before releasing lease or closing the run',async t=>{
  const {db}=fixture(t);
  const fake=fakeDependencies({topics:[source({topic_id:'600000001'}),source({topic_id:'600000002',content:`${BODY}另一篇`})],
    transform:async({inputs,result})=>{
      await new Promise(r=>setTimeout(r,inputs[0].topic_id==='600000001'?5:40));
      if(inputs[0].topic_id==='600000001')result.items[0].topic_id='invalid';return result;
    }});
  const result=await runAutomatedPipeline(db,opts({unlimitedCalls:true,concurrency:2,batchSize:1}),fake.dependencies);
  assert.equal(result.status,'paused');assert.equal(fake.calls.length,2);
  assert.equal(batchRow(db,result.batch_id).lease_token,null);
  assert.equal(db.prepare("SELECT count(*) n FROM model_call_ledger WHERE status='running'").get().n,0);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_batch_topics WHERE status='extracted'").get().n,1);
});

test('daily input character cap is atomic and independent of per-batch caps',t=>{
  const {db}=fixture(t),first=createBatch(db,opts()),second=createBatch(db,opts());
  db.prepare('UPDATE workflow_batches SET max_input_chars=1000000').run();
  reserveCall(db,first.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:DAILY_LIMITS.max_input_chars});
  assert.throws(()=>reserveCall(db,second.id,{stage:'review',provider:'codex',model:'fixture-model',chars:1}),/跨批次模型输入字符上限/);
  assert.equal(batchRow(db,second.id).calls_used,0);
});

test('recent entry resumes prepared window automatically and keeps full-window selection',async t=>{
  const {db}=fixture(t),fake=fakeDependencies();
  const prepared=await runAutomatedPipeline(db,opts({prepareOnly:true}),fake.dependencies);
  const result=await runRecentPipeline(db,{windowEnd:WINDOW_END,maxCalls:4},fake.dependencies);
  assert.equal(result.batch_id,prepared.batch_id);
  assert.equal(result.status,'completed');
  assert.equal(fake.fetches.length,1);
  assert.equal(batchRow(db,result.batch_id).calls_used,2);
});

test('recent entry refuses automatic retry of a structural failure or manual stop',async t=>{
  const {db}=fixture(t),fake=fakeDependencies({transform:({result})=>{result.items[0].topic_id='999';return result;}});
  const first=await runAutomatedPipeline(db,opts(),fake.dependencies);
  const calls=fake.calls.length;
  const result=await runRecentPipeline(db,{windowEnd:WINDOW_END},fake.dependencies);
  assert.equal(result.status,'paused');assert.equal(fake.calls.length,calls);
  db.prepare("UPDATE workflow_batches SET pause_code='MODEL_BUDGET_EXCEEDED',stop_requested=1 WHERE id=?").run(first.batch_id);
  const stopped=await runRecentPipeline(db,{windowEnd:WINDOW_END},fake.dependencies);
  assert.match(stopped.error,/人为停止/);assert.equal(fake.calls.length,calls);
});

test('recent entry does not reset daily cap or create batches while daily budget is empty',async t=>{
  const {db}=fixture(t),fake=fakeDependencies(),batch=createBatch(db,opts());
  db.prepare('UPDATE workflow_batches SET max_calls=100').run();
  for(let i=0;i<DAILY_LIMITS.max_calls;i++)reserveCall(db,batch.id,{stage:'primary',provider:'codex',model:'fixture-model',chars:1});
  const before=count(db,'workflow_batches');
  const result=await runRecentPipeline(db,{windowEnd:WINDOW_END},fake.dependencies);
  assert.equal(result.status,'paused');assert.equal(fake.calls.length,0);
  assert.equal(count(db,'workflow_batches'),before);
});

test('recent next-day continuation raises only cumulative operational caps, never resets recorded use',async t=>{
  const {db}=fixture(t),fake=fakeDependencies();
  const prepared=await runAutomatedPipeline(db,opts({prepareOnly:true}),fake.dependencies);
  db.prepare("UPDATE workflow_batches SET calls_used=40,max_calls=40,input_chars_used=4000,elapsed_ms=300000,pause_code='MODEL_BUDGET_EXCEEDED' WHERE id=?").run(prepared.batch_id);
  const yesterday=new Date(Date.now()-86400000).toISOString();
  for(let i=0;i<40;i++)db.prepare("INSERT INTO model_call_ledger(batch_id,stage,provider,requested_model,input_chars,status,started_at) VALUES(?,'primary','codex','fixture-model',100,'failed',?)").run(prepared.batch_id,yesterday);
  const result=await runRecentPipeline(db,{windowEnd:WINDOW_END,maxCalls:2,maxChars:80000,maxMinutes:10},fake.dependencies);
  assert.equal(result.status,'completed');
  const saved=batchRow(db,prepared.batch_id);
  assert.equal(saved.calls_used,42);assert.equal(saved.max_calls,42);
  assert.equal(count(db,'model_call_ledger'),42);
  assert.ok(saved.input_chars_used>4000);assert.equal(saved.max_input_chars,84000);
  assert.equal(saved.max_minutes,15);
});

test('current original changed during review cannot create accepted records or a reusable review cache', async t => {
  const { db } = fixture(t), fake = fakeDependencies({ transform: ({ stage, result }) => {
    if (stage === 'review') mutateOriginal(db, `${BODY}但最新结论已经改为回避。`);
    return result;
  } });
  const run = await runAutomatedPipeline(db, opts(), fake.dependencies);
  assert.equal(run.status, 'paused', JSON.stringify(run));
  assert.equal(topicRow(db, run.batch_id).status, 'failed');
  assert.equal(count(db, 'viewpoints'), 0);
  assert.equal(count(db, 'viewpoint_approval_audit'), 0);
  assert.equal(count(db, 'topic_processing_state'), 0);
  assert.equal(db.prepare("SELECT count(*) n FROM workflow_model_cache WHERE stage='review'").get().n, 0);
});

test('new batch with same topic ID and changed content reprocesses that version and preserves old audit', async t => {
  const { db } = fixture(t), firstFake = fakeDependencies();
  await runAutomatedPipeline(db, opts(), firstFake.dependencies);
  const oldView = db.prepare('SELECT id FROM viewpoints').get().id;
  const changed = fakeDependencies({ topics: [source({ content: `${BODY}补充：新增催化继续强化该结论。` })] });
  const run = await runAutomatedPipeline(db, opts(), changed.dependencies);
  assert.equal(run.status, 'completed', JSON.stringify(run));
  assert.deepEqual(changed.calls.map(call => call.stage), ['primary', 'review']);
  assert.equal(count(db, 'raw_topics'), 1);
  assert.equal(count(db, 'raw_topic_versions'), 2);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints WHERE id=?').get(oldView).review_status, 'rejected');
  assert.equal(db.prepare("SELECT count(*) n FROM viewpoints WHERE review_status='reviewed'").get().n, 1);
  const currentHash = db.prepare('SELECT content_hash FROM raw_topics').get().content_hash;
  assert.equal(db.prepare('SELECT content_hash FROM extraction_results ORDER BY id DESC LIMIT 1').get().content_hash, currentHash);
  assert.equal(validatePublishBatch(db).ok, true);
});
