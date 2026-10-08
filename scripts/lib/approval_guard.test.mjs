import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { ensureWorkflowSchema } from './workflow_db.mjs';
import { APPROVAL_POLICY_VERSION, ensureApprovalSchema, viewpointFingerprint, recordApproval, invalidateApproval,
  currentApproval, validatePublishBatch, publishAccepted } from './approval_guard.mjs';

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'zsxq-approval-test-'));
  const dbPath = join(directory, 'fixture.sqlite');
  const db = new DatabaseSync(dbPath);
  db.exec('PRAGMA foreign_keys=ON');
  ensureWorkflowSchema(db);
  ensureApprovalSchema(db);
  t.after(() => { db.close(); rmSync(directory, { recursive: true, force: true }); });
  db.exec(`
    INSERT INTO raw_topics(topic_id,group_id,published_at,published_at_utc,title,content,eligibility,content_hash,raw_json,first_seen_at,last_seen_at)
      VALUES('t1','g1','2026-09-12','2026-09-12','长江证券电新','推荐关注宁德时代，目标价300元。','eligible','hash-v1','{}','2026-09-12','2026-09-12');
    INSERT INTO model_runs(id,stage,provider,model_id,prompt_version,status,started_at) VALUES(1,'extraction','test','test','test','success','2026-09-12');
    INSERT INTO topic_classifications(id,topic_id,model_run_id,content_hash,classification,confidence,evidence,provider,model_id,prompt_version,created_at)
      VALUES(1,'t1',1,'hash-v1','broker_note',0.95,'长江证券电新','test','test','test','2026-09-12');
    INSERT INTO extraction_results(id,topic_id,classification_id,model_run_id,content_hash,provider,model_id,prompt_version,status,created_at)
      VALUES(1,'t1',1,1,'hash-v1','test','test','test','success','2026-09-12');
    INSERT INTO securities(id,ts_code,symbol,market,name,synced_at) VALUES(1,'300750.SZ','300750','SZ','宁德时代','2026-09-12');
    INSERT INTO viewpoints(id,topic_id,extraction_result_id,ordinal,summary,direction,evidence,supporting_evidence_json,
      recommendation_broker,recommendation_team,recommendation_evidence,target_price,target_currency,target_price_raw,target_price_evidence,
      confidence,review_status,created_at,updated_at)
      VALUES(1,'t1',1,1,'推荐关注宁德时代','bullish','推荐关注宁德时代，目标价300元。','["推荐关注宁德时代"]',
      '长江证券','电新','长江证券电新',300,'CNY','300元','目标价300元',0.95,'reviewed','2026-09-12','2026-09-12');
    INSERT INTO viewpoint_securities(viewpoint_id,security_id,surface,code_hint,evidence,confidence,resolution_method,resolution_status)
      VALUES(1,1,'宁德时代','300750.SZ','宁德时代',0.99,'tushare','resolved');
  `);
  const approve = (options = {}) => recordApproval(db, { viewpointId: 1, decision: 'accept', method: 'human',
    scopeDecision: 'broker_note', reason: '已对照原文确认卖方观点、证券和方向', ...options });
  return { db, directory, dbPath, approve };
}

function issueCodes(db) { return validatePublishBatch(db).issues.map((issue) => issue.code); }

test('legacy reviewed flag alone is not an approval', (t) => {
  const { db } = fixture(t);
  assert.deepEqual(issueCodes(db), ['missing_approval']);
});

test('new AI decisions require explicit broker-note scope and model provenance', (t) => {
  const { db, approve } = fixture(t);
  assert.throws(() => approve({ method: 'ai', modelId: 'model', scopeDecision: 'not_broker_note' }), /卖方观点/);
  assert.throws(() => approve({ method: 'ai' }), /注明模型/);
  assert.throws(() => approve({ method: 'ai', modelId: 'model', policyVersion: 'legacy' }), /当前裁决规则/);
  approve({ method: 'ai', modelId: 'model', policyVersion: APPROVAL_POLICY_VERSION });
  assert.equal(validatePublishBatch(db).ok, true);
});

test('missing or partially unresolved links block publication', (t) => {
  const { db, approve } = fixture(t);
  db.exec("UPDATE viewpoint_securities SET resolution_status='unresolved'");
  approve();
  assert.ok(issueCodes(db).includes('unresolved_security'));
  db.exec('DELETE FROM viewpoint_securities');
  approve();
  assert.ok(issueCodes(db).includes('missing_security'));
});

test('legal market and exact evidence are required even after human acceptance', (t) => {
  const { db, approve } = fixture(t);
  db.exec("UPDATE securities SET ts_code='300750.US'; UPDATE viewpoint_securities SET evidence='原文不存在的证券'; UPDATE viewpoints SET evidence='编造观点'");
  approve();
  const codes = issueCodes(db);
  assert.ok(codes.includes('illegal_security'));
  assert.ok(codes.includes('invalid_evidence'));
  assert.ok(codes.includes('invalid_security_evidence'));
});

test('target price requires a finite positive number, legal currency and matching source numbers', (t) => {
  const { db, approve } = fixture(t);
  for (const [price, currency, evidence] of [[-3, 'CNY', '目标价300元'], [300, '亿元', '目标价300元'],
    [999, 'CNY', '目标价300元'], [300, 'CNY', null], [null, 'CNY', null]]) {
    db.prepare('UPDATE viewpoints SET target_price=?,target_currency=?,target_price_evidence=?').run(price, currency, evidence);
    approve();
    assert.ok(issueCodes(db).includes('invalid_target'), `${price}/${currency}/${evidence}`);
  }
});

test('broker source cannot be fabricated; unsigned notes can have NULL broker', (t) => {
  const { db, approve } = fixture(t);
  db.exec("UPDATE viewpoints SET recommendation_broker='中信证券'");
  approve();
  assert.ok(issueCodes(db).includes('invalid_source'));
  db.exec("UPDATE viewpoints SET recommendation_broker=NULL,recommendation_team='其他',recommendation_evidence=NULL");
  approve();
  assert.equal(validatePublishBatch(db).ok, true);
});

test('canonical broker metadata accepts exact encoded or abbreviated signature evidence', (t) => {
  const { db, approve } = fixture(t);
  for (const [broker, signature] of [['中信建投','【建投食饮】'], ['申万宏源','【申万电新】'], ['中国银河','【银河医药】'],
    ['中信建投','<e title="%E5%BB%BA%E6%8A%95%E9%A3%9F%E9%A5%AE"/>']]) {
    db.prepare('UPDATE raw_topics SET title=?').run(signature);
    db.prepare('UPDATE viewpoints SET recommendation_broker=?,recommendation_evidence=?').run(broker, signature);
    approve();
    assert.equal(validatePublishBatch(db).ok, true, broker);
  }
});

test('edits invalidate old approval even if a writer forgets to invalidate explicitly', (t) => {
  const { db, approve } = fixture(t);
  const before = viewpointFingerprint(db, 1);
  approve();
  db.exec("UPDATE viewpoints SET summary='更新后的观点',review_version=review_version+1");
  assert.ok(issueCodes(db).includes('stale_approval'));
  assert.throws(() => approve({ expectedFingerprint: before.fingerprint }), /已发生变化/);
  invalidateApproval(db, 1);
  assert.equal(currentApproval(db, 1).decision, 'invalidated');
  approve();
  assert.equal(validatePublishBatch(db).ok, true);
});

test('raw content, classification and security-master changes invalidate prior decisions', (t) => {
  const { db, approve } = fixture(t);
  for (const mutation of ["UPDATE raw_topics SET content=content||'补充说明'", "UPDATE raw_topics SET content_hash='new-hash'",
    "UPDATE topic_classifications SET classification='uncertain'", "UPDATE securities SET name='宁德时代新名称'"]) {
    approve();
    db.exec(mutation);
    assert.ok(issueCodes(db).includes('stale_approval'), mutation);
  }
});

test('human acceptance explicitly overrides a not-broker classification without deleting AI history', (t) => {
  const { db, approve } = fixture(t);
  db.exec("UPDATE topic_classifications SET classification='not_broker_note'");
  approve({ decision: 'uncertain', method: 'ai', modelId: 'model', scopeDecision: 'uncertain' });
  assert.ok(issueCodes(db).includes('approval_not_accepted'));
  approve();
  assert.equal(validatePublishBatch(db).ok, true);
  assert.equal(db.prepare('SELECT count(*) n FROM viewpoint_approval_audit').get().n, 2);
  assert.throws(() => db.exec("UPDATE viewpoint_approval_audit SET reason='overwrite'"), /不得覆盖/);
  assert.throws(() => db.exec('DELETE FROM viewpoint_approval_audit'), /不得删除/);
});

test('valid publication is backed up and existing published records cannot be republished', async (t) => {
  const { db, directory, approve } = fixture(t);
  approve();
  const result = await publishAccepted(db, { backupDir: join(directory, 'backups') });
  assert.equal(result.published, 1);
  assert.ok(existsSync(result.backup));
  const backup = new DatabaseSync(result.backup, { readOnly: true });
  assert.equal(backup.prepare('SELECT review_status FROM viewpoints WHERE id=1').get().review_status, 'reviewed');
  backup.close();
  assert.equal(db.prepare('SELECT review_status FROM viewpoints WHERE id=1').get().review_status, 'published');
  assert.equal(db.prepare('SELECT count(*) n FROM viewpoint_publish_batches').get().n, 1);
  assert.equal((await publishAccepted(db, { backupDir: join(directory, 'backups') })).published, 0);
  assert.ok(validatePublishBatch(db, [1]).issues.some((issue) => issue.code === 'not_reviewed'));
  assert.throws(() => approve(), /已发布观点/);
});

test('backup failure leaves reviewed state and no publication record', async (t) => {
  const { db, dbPath, approve } = fixture(t);
  approve();
  await assert.rejects(publishAccepted(db, { backupDir: dbPath }));
  assert.equal(db.prepare('SELECT review_status FROM viewpoints WHERE id=1').get().review_status, 'reviewed');
  assert.equal(db.prepare('SELECT count(*) n FROM viewpoint_publish_batches').get().n, 0);
});

test('recheck rejects edits during the asynchronous backup preparation', async (t) => {
  const { db, directory, approve } = fixture(t);
  approve();
  const publication = publishAccepted(db, { backupDir: join(directory, 'race-backup') });
  db.exec("UPDATE viewpoints SET summary='备份期间修改',review_version=review_version+1");
  approve();
  await assert.rejects(publication, /备份期间审核内容已变化/);
  assert.equal(db.prepare('SELECT review_status FROM viewpoints WHERE id=1').get().review_status, 'reviewed');
  assert.equal(db.prepare('SELECT count(*) n FROM viewpoint_publish_batches').get().n, 0);
});
