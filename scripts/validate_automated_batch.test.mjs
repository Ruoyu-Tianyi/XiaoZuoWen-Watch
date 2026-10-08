import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { ensureWorkflowSchema } from './lib/workflow_db.mjs';
import { ensureAutomationSchema } from './lib/automation_state.mjs';
import { createBatch } from './lib/automated_pipeline.mjs';
import { parseBatchArgs, inspectAutomatedBatch } from './validate_automated_batch.mjs';

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), 'zsxq-batch-validation-'));
  const path = join(directory, 'fixture.sqlite');
  const db = new DatabaseSync(path);
  ensureWorkflowSchema(db);
  ensureAutomationSchema(db);
  const batch = createBatch(db, { windowEnd: '2026-09-26T00:00:00.000Z', config: { group_id: 'fixture' } });
  t.after(() => { db.close(); rmSync(directory, { recursive: true, force: true }); });
  return { db, path, id: batch.id };
}

test('requires exactly one safe positive batch ID', () => {
  for (const args of [[], ['--batch=0'], ['--batch=-1'], ['--batch=1.5'], ['--batch=abc'], ['--batch=9007199254740993'], ['--batch=1', '--batch=2']]) assert.throws(() => parseBatchArgs(args));
  assert.equal(parseBatchArgs(['--batch=12']), 12);
});

test('unfinished batch fails machine acceptance without changing the database', t => {
  const { path, id } = fixture(t);
  const before = readFileSync(path);
  const result = inspectAutomatedBatch(path, id);
  assert.equal(result.ok, false);
  assert.equal(result.batch.terminal, false);
  assert.deepEqual(result.reviewed_viewpoint_check.checked_ids, []);
  assert.deepEqual(readFileSync(path), before);
});

test('terminal empty batch passes, missing batch fails, CLI honors ZSXQ_DB', t => {
  const { db, path, id } = fixture(t);
  db.prepare("UPDATE workflow_batches SET status='completed',stage='done',fetch_complete=1,selection_complete=1 WHERE id=?").run(id);
  const before = readFileSync(path);
  const result = inspectAutomatedBatch(path, id);
  assert.equal(result.ok, true);
  assert.equal(result.window_counts.raw, 0);
  assert.equal(result.database_checks.ok, true);
  assert.equal(result.reviewed_viewpoint_check.published, false);
  assert.throws(() => inspectAutomatedBatch(path, id + 1), /找不到批次/);
  const cli = spawnSync(process.execPath, [fileURLToPath(new URL('./validate_automated_batch.mjs', import.meta.url)), `--batch=${id}`], { env: { ...process.env, ZSXQ_DB: path }, encoding: 'utf8' });
  assert.equal(cli.status, 0, cli.stderr);
  assert.equal(JSON.parse(cli.stdout).ok, true);
  assert.deepEqual(readFileSync(path), before);
});

test('terminal count inconsistency fails acceptance', t => {
  const { db, path, id } = fixture(t);
  db.prepare("UPDATE workflow_batches SET status='completed',fetch_complete=1,selection_complete=1,selected_count=1 WHERE id=?").run(id);
  const result = inspectAutomatedBatch(path, id);
  assert.equal(result.ok, false);
  assert.ok(result.structural_anomalies.some(item => item.code === 'selected_count_mismatch'));
});
