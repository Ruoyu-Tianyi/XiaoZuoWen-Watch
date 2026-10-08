import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';

function run(script, args = [], env = {}) {
  const result = spawnSync(process.execPath, [resolve(script), ...args], {
    cwd: process.cwd(), env: { ...process.env, ...env }, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
  });
  if (result.stdout) process.stdout.write(result.stdout);
  if (result.status !== 0) {
    if (result.stderr) process.stderr.write(result.stderr);
    throw new Error(`${script} ${args.join(' ')} failed with exit code ${result.status}`);
  }
}

run('scripts/pipeline_control.mjs', ['init']);
run('scripts/pipeline_control.mjs', ['backfill']);
run('scripts/pipeline_control.mjs', ['validate']);
run('scripts/pipeline_control.mjs', ['diff']);
run('scripts/pipeline_control.mjs', ['publish']);
run('scripts/report_stats.mjs');
run('scripts/check_data_quality.mjs');
console.log(JSON.stringify({ success: true, mode: 'backfill', finished_at: new Date().toISOString() }));
