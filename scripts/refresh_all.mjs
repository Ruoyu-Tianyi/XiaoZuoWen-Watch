import { spawnSync } from 'node:child_process';
import { existsSync, readFileSync, statSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { loadPipelineConfig } from './lib/direction_rules.mjs';
import { extractionVersion, securityMasterFingerprint } from './lib/pipeline_version.mjs';

const hours = String(Number(process.env.ZSXQ_REFRESH_HOURS || '168'));
const rawOutput = resolve(process.env.ZSXQ_REFRESH_RAW || 'data/zsxq_topics_7d.json');
const masterPath = resolve('data/security_master.json');
const databasePath = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const maxMasterAgeMs = 7 * 24 * 60 * 60 * 1000;

function run(script, extraEnv = {}, args = []) {
  const result = spawnSync(process.execPath, [resolve(script), ...args], {
    cwd: process.cwd(),
    env: { ...process.env, ...extraEnv },
    encoding: 'utf8',
    maxBuffer: 32 * 1024 * 1024,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.status !== 0) {
    if (result.stderr) process.stderr.write(result.stderr);
    throw new Error(`${script} failed with exit code ${result.status}`);
  }
}

function masterFingerprint() {
  if (!existsSync(masterPath)) return null;
  return securityMasterFingerprint(JSON.parse(readFileSync(masterPath, 'utf8')));
}

function hasStaleExtractions() {
  if (!existsSync(databasePath) || !existsSync(masterPath)) return false;
  const master = JSON.parse(readFileSync(masterPath, 'utf8'));
  const expected = extractionVersion(loadPipelineConfig(), securityMasterFingerprint(master));
  const db = new DatabaseSync(databasePath, { readOnly: true });
  try {
    return Number(db.prepare('SELECT count(*) n FROM topics WHERE extraction_version IS NULL OR extraction_version<>?').get(expected).n) > 0;
  } finally {
    db.close();
  }
}

const previousMasterFingerprint = masterFingerprint();
const masterIsStale = !existsSync(masterPath) || Date.now() - statSync(masterPath).mtimeMs > maxMasterAgeMs;
if (masterIsStale) {
  run('scripts/download_security_master.mjs');
  run('scripts/download_us_security_master.mjs');
}
const masterChanged = previousMasterFingerprint !== null && previousMasterFingerprint !== masterFingerprint();
const fullReprocessRequired = masterChanged || hasStaleExtractions();
run('scripts/fetch_recent.mjs', { ZSXQ_HOURS: hours, ZSXQ_RAW_OUTPUT: rawOutput });
run('scripts/pipeline_control.mjs', {}, ['init']);
if (fullReprocessRequired) run('scripts/pipeline_control.mjs', {}, ['reprocess']);
else run('scripts/pipeline_control.mjs', { PIPELINE_RAW_INPUT: rawOutput }, ['extract']);
run('scripts/enrich_history.mjs', {}, ['run', '--viewpoints', '--authoritative', '--changed-only']);
run('scripts/resolve_security_queue.mjs');
run('scripts/pipeline_control.mjs', {}, ['validate']);
run('scripts/pipeline_control.mjs', {}, ['diff']);
run('scripts/pipeline_control.mjs', {}, ['publish']);
run('scripts/report_stats.mjs');
run('scripts/check_data_quality.mjs');

console.log(JSON.stringify({ success: true, refreshed_at: new Date().toISOString(), hours: Number(hours), raw_output: rawOutput }));
