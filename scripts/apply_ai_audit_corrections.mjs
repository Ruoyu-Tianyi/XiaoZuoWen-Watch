import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const dbPath = resolve(process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite');
const audit = JSON.parse(readFileSync(resolve('reports/ai_quality_audit.json'), 'utf8'));
const corrections = (audit.disagreements || []).filter((row) =>
  row.audit_direction && row.audit_direction === row.verifier_direction && row.audit_direction !== row.candidate_direction);

const db = new DatabaseSync(dbPath);
const find = db.prepare(`SELECT m.id,m.direction FROM mentions m JOIN securities s ON s.id=m.security_id
  WHERE m.topic_id=? AND s.market=? AND s.code=? AND m.recognition_method='gpt_verified'`);
const update = db.prepare(`UPDATE mentions SET direction=?,direction_confidence=MAX(coalesce(direction_confidence,0),0.95),
  recognition_model='gpt-5.6-terra-audited' WHERE id=?`);
const applied = [];
db.exec('BEGIN IMMEDIATE');
try {
  for (const row of corrections) {
    const mention = find.get(String(row.topic_id), row.market, row.code);
    if (!mention) throw new Error(`找不到待修正提及 ${row.topic_id} ${row.market}:${row.code}`);
    update.run(row.audit_direction, mention.id);
    applied.push({ topic_id: String(row.topic_id), market: row.market, code: row.code, from: mention.direction, to: row.audit_direction });
  }
  db.exec('COMMIT');
} catch (error) {
  db.exec('ROLLBACK');
  throw error;
} finally {
  db.close();
}
console.log(JSON.stringify({ applied: applied.length, corrections: applied }, null, 2));
