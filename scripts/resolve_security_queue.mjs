import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const option = (name, fallback = null) => process.argv.find((arg) => arg.startsWith(`--${name}=`))?.slice(name.length + 3) ?? fallback;
const dbPath = resolve(option('db', process.env.PIPELINE_CANDIDATE_DB || 'data/pipeline_candidate.sqlite'));
const masterPath = resolve(option('master', process.env.SECURITY_MASTER || 'data/security_master.json'));
const resolutionPath = resolve(option('resolutions', 'config/security_resolutions.json'));
const config = JSON.parse(readFileSync(resolutionPath, 'utf8'));
const master = JSON.parse(readFileSync(masterPath, 'utf8'));
const normalize = (value) => String(value || '').trim().toLowerCase();
const aliasMap = new Map();
for (const resolution of config.resolutions || []) for (const alias of [resolution.canonical_name, ...(resolution.aliases || [])]) aliasMap.set(normalize(alias), resolution);

const db = new DatabaseSync(dbPath);
db.exec('PRAGMA foreign_keys=ON');
const pending = db.prepare("SELECT * FROM security_resolution_queue WHERE status='pending' ORDER BY id").all();
const upsertSecurity = db.prepare('INSERT INTO securities(code,market,name) VALUES(?,?,?) ON CONFLICT(code,market) DO UPDATE SET name=excluded.name');
const getSecurity = db.prepare('SELECT id FROM securities WHERE code=? AND market=?');
const getViewpoint = db.prepare(`SELECT id FROM model_viewpoints WHERE topic_id=? AND content_hash=? AND ordinal=? AND recognition_model=? AND prompt_version=?`);
const upsertViewpointMention = db.prepare(`INSERT INTO model_viewpoint_mentions(viewpoint_id,security_id,surface,direction,target_price,target_currency,evidence,confidence,verification_status)
  VALUES(?,?,?,?,NULL,NULL,?,?,?) ON CONFLICT(viewpoint_id,security_id) DO UPDATE SET surface=excluded.surface,direction=excluded.direction,evidence=excluded.evidence,confidence=max(model_viewpoint_mentions.confidence,excluded.confidence),verification_status=excluded.verification_status`);
const resolveQueue = db.prepare(`UPDATE security_resolution_queue SET status='resolved',resolved_security_id=?,resolution_source=?,resolution_url=?,resolution_notes=?,updated_at=? WHERE id=?`);
const selectTopicEntities = db.prepare(`SELECT s.id security_id,s.code,s.market,vm.direction,vm.target_price,vm.target_currency,vm.evidence,vm.confidence
  FROM model_viewpoints v JOIN model_viewpoint_mentions vm ON vm.viewpoint_id=v.id JOIN securities s ON s.id=vm.security_id
  WHERE v.topic_id=? AND v.recognition_model=? AND v.prompt_version=?`);
const findMention = db.prepare('SELECT m.id FROM mentions m WHERE m.topic_id=? AND m.security_id=?');
const insertMention = db.prepare(`INSERT INTO mentions(topic_id,security_id,direction,direction_confidence,entity_confidence,target_price,target_currency,target_price_text,evidence,recognition_method,recognition_model,recognition_confidence,is_mixed)
  VALUES(?,?,?,?,?,?,?,?,?,'gpt_viewpoint_verified',?,?,?)`);
const updateMention = db.prepare(`UPDATE mentions SET direction=?,direction_confidence=?,entity_confidence=?,target_price=?,target_currency=?,target_price_text=?,evidence=?,recognition_method='gpt_viewpoint_verified',recognition_model=?,recognition_confidence=?,is_mixed=? WHERE id=?`);

const affected = new Map();
let resolvedRows = 0;
db.exec('BEGIN IMMEDIATE');
try {
  for (const resolution of config.resolutions || []) upsertSecurity.run(resolution.code, resolution.market, resolution.canonical_name);
  for (const row of pending) {
    const resolution = aliasMap.get(normalize(row.canonical_name)) || aliasMap.get(normalize(row.surface));
    if (!resolution) continue;
    upsertSecurity.run(resolution.code, resolution.market, resolution.canonical_name);
    const securityId = Number(getSecurity.get(resolution.code, resolution.market).id);
    const viewpoint = getViewpoint.get(row.topic_id, row.content_hash, row.viewpoint_ordinal, row.recognition_model, row.prompt_version);
    if (!viewpoint) continue;
    upsertViewpointMention.run(Number(viewpoint.id), securityId, row.surface, row.direction, row.evidence, row.confidence, 'authoritative_web_resolved');
    resolveQueue.run(securityId, resolution.source, resolution.url, resolution.notes, new Date().toISOString(), row.id);
    affected.set(`${row.topic_id}|${row.recognition_model}|${row.prompt_version}`, { topic_id: row.topic_id, model: row.recognition_model, prompt: row.prompt_version });
    resolvedRows += 1;
  }
  for (const item of affected.values()) {
    const groups = new Map();
    for (const row of selectTopicEntities.all(item.topic_id, item.model, item.prompt)) {
      if (!groups.has(row.security_id)) groups.set(row.security_id, []);
      groups.get(row.security_id).push(row);
    }
    for (const [securityId, rows] of groups) {
      const directions = new Set(rows.map((row) => row.direction));
      const mixed = directions.has('bullish') && directions.has('bearish');
      const direction = mixed ? 'neutral' : directions.has('bullish') ? 'bullish' : directions.has('bearish') ? 'bearish' : 'neutral';
      const target = rows.filter((row) => row.target_price != null).sort((a, b) => Number(b.confidence) - Number(a.confidence))[0] || null;
      const confidence = Math.max(...rows.map((row) => Number(row.confidence)));
      const evidence = [...new Set(rows.map((row) => row.evidence))].join(' | ').slice(0, 2000);
      const existing = findMention.get(item.topic_id, securityId);
      if (existing) updateMention.run(direction, confidence, confidence, target?.target_price ?? null, target?.target_currency || null,
        target?.target_price == null ? null : String(target.target_price), evidence, item.model, confidence, mixed ? 1 : 0, existing.id);
      else insertMention.run(item.topic_id, securityId, direction, confidence, confidence, target?.target_price ?? null, target?.target_currency || null,
        target?.target_price == null ? null : String(target.target_price), evidence, item.model, confidence, mixed ? 1 : 0);
    }
  }
  db.exec('COMMIT');
} catch (error) { db.exec('ROLLBACK'); db.close(); throw error; }
db.close();

const byKey = new Map((master.securities || []).map((row) => [`${row.market}:${row.code}`, row]));
for (const resolution of config.resolutions || []) byKey.set(`${resolution.market}:${resolution.code}`, { code: resolution.code, name: resolution.canonical_name, market: resolution.market });
master.securities = [...byKey.values()];
master.count = master.securities.length;
master.fetched_at = new Date().toISOString();
writeFileSync(masterPath, `${JSON.stringify(master, null, 2)}\n`, 'utf8');
console.log(JSON.stringify({ db: dbPath, pending_before: pending.length, resolved_rows: resolvedRows, affected_topics: affected.size, security_master_count: master.count }, null, 2));
