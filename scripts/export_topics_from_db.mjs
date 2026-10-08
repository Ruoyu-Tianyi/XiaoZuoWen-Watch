import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const databasePath = resolve(process.env.ZSXQ_SOURCE_DB || 'data/broker_notes.sqlite');
const outputPath = resolve(process.env.ZSXQ_RAW_OUTPUT || 'data/zsxq_reprocess_chunk.json');
const db = new DatabaseSync(databasePath, { readOnly: true });
const topics = db.prepare('SELECT raw_json FROM topics ORDER BY published_at_utc DESC').all()
  .map((row) => {
    try { return JSON.parse(row.raw_json); }
    catch { return null; }
  })
  .filter(Boolean);
db.close();

const payload = {
  group_id: String(topics[0]?.group?.group_id || ''),
  group_name: topics[0]?.group?.name || '',
  fetched_at: new Date().toISOString(),
  cutoff_at: topics.at(-1)?.create_time || new Date(0).toISOString(),
  source_database: databasePath,
  count: topics.length,
  topics,
};
mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(outputPath, JSON.stringify(payload), 'utf8');
console.log(JSON.stringify({ source: databasePath, output: outputPath, topics: topics.length }));
