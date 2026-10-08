import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { classifyDirection, loadPipelineConfig } from './lib/direction_rules.mjs';

const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const REPORT_PATH = resolve(process.env.ZSXQ_DIRECTION_REPORT || 'reports/direction_reclassification.json');
const config = loadPipelineConfig();

const db = new DatabaseSync(DB_PATH);
const before = Object.fromEntries(db.prepare('SELECT direction,count(*) n FROM mentions GROUP BY direction').all().map((row) => [row.direction, Number(row.n)]));
const candidates = db.prepare("SELECT id,evidence FROM mentions WHERE direction='neutral'").all();
const update = db.prepare("UPDATE mentions SET direction='bullish',direction_confidence=0.82 WHERE id=?");
const changed = { total: 0, recommendation: 0, attention: 0, high_elasticity: 0 };

db.exec('BEGIN IMMEDIATE');
try {
  for (const row of candidates) {
    const evidence = String(row.evidence || '');
    const result = classifyDirection(evidence, config);
    if (result.direction !== 'bullish') continue;
    update.run(row.id);
    changed.total += 1;
    if (/推荐/.test(evidence)) changed.recommendation += 1;
    if (/关注/.test(evidence)) changed.attention += 1;
    if (/高弹性/.test(evidence)) changed.high_elasticity += 1;
  }
  db.exec('COMMIT');
} catch (error) {
  db.exec('ROLLBACK');
  db.close();
  throw error;
}

const after = Object.fromEntries(db.prepare('SELECT direction,count(*) n FROM mentions GROUP BY direction').all().map((row) => [row.direction, Number(row.n)]));
const residual = db.prepare(`
  SELECT count(*) n FROM mentions
  WHERE direction='neutral' AND (evidence LIKE '%推荐%' OR evidence LIKE '%关注%' OR evidence LIKE '%高弹性%')
`).get().n;
const report = { checked_at: new Date().toISOString(), database: DB_PATH, before, after, changed, residual_neutral_core_terms: Number(residual) };
mkdirSync(dirname(REPORT_PATH), { recursive: true });
writeFileSync(REPORT_PATH, JSON.stringify(report, null, 2), 'utf8');
console.log(JSON.stringify(report, null, 2));
db.close();
