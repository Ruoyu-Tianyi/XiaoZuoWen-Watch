import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { basename, dirname, relative, resolve, sep } from 'node:path';
import { DatabaseSync } from 'node:sqlite';

const root = resolve(process.cwd());
const dataRoot = resolve(root, 'data');
const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const archiveRoot = resolve(dataRoot, 'archive', `legacy-${stamp}`);

if (!archiveRoot.startsWith(`${resolve(dataRoot, 'archive')}${sep}`)) throw new Error('归档目标不在 data/archive 内');
if (existsSync(archiveRoot)) throw new Error(`归档目录已存在: ${archiveRoot}`);

function inspectDb(path) {
  if (!existsSync(path)) return null;
  const db = new DatabaseSync(path);
  try {
    db.exec('PRAGMA busy_timeout=5000; PRAGMA wal_checkpoint(TRUNCATE);');
    const quick = db.prepare('PRAGMA quick_check').all().map((row) => Object.values(row)[0]);
    const foreignKeys = db.prepare('PRAGMA foreign_key_check').all().length;
    const table = db.prepare("SELECT count(*) n FROM sqlite_master WHERE type='table' AND name='topics'").get().n ? 'topics' : null;
    const topics = table ? Number(db.prepare('SELECT count(*) n FROM topics').get().n) : 0;
    const range = table ? db.prepare('SELECT min(published_at_utc) earliest,max(published_at_utc) latest FROM topics').get() : {};
    return { quick_check: quick, foreign_key_errors: foreignKeys, topics, earliest: range.earliest || null, latest: range.latest || null };
  } finally { db.close(); }
}

function move(source, destination) {
  const src = resolve(source);
  const dest = resolve(destination);
  if (!src.startsWith(`${root}${sep}`) || !dest.startsWith(`${archiveRoot}${sep}`)) throw new Error(`不安全的移动目标: ${src} -> ${dest}`);
  if (!existsSync(src)) return false;
  mkdirSync(dirname(dest), { recursive: true });
  renameSync(src, dest);
  return true;
}

function listFiles(path, output = []) {
  if (!existsSync(path)) return output;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    const full = resolve(path, entry.name);
    if (entry.isDirectory()) listFiles(full, output);
    else output.push(full);
  }
  return output;
}

function digest(path) {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

const formal = resolve(dataRoot, 'broker_notes.sqlite');
const candidate = resolve(dataRoot, 'pipeline_candidate.sqlite');
const databaseInspection = { formal: inspectDb(formal), candidate: inspectDb(candidate) };
for (const [name, result] of Object.entries(databaseInspection)) {
  if (result && (result.quick_check.some((value) => value !== 'ok') || result.foreign_key_errors)) throw new Error(`${name} 数据库一致性检查失败`);
}

mkdirSync(archiveRoot, { recursive: true });
for (const file of ['broker_notes.sqlite', 'broker_notes.sqlite-wal', 'broker_notes.sqlite-shm', 'pipeline_candidate.sqlite', 'pipeline_candidate.sqlite-wal', 'pipeline_candidate.sqlite-shm', 'pipeline_candidate.json']) {
  move(resolve(dataRoot, file), resolve(archiveRoot, 'databases', file));
}
move(resolve(dataRoot, 'backups'), resolve(archiveRoot, 'backups'));
move(resolve(dataRoot, 'ai_enrichment_tmp'), resolve(archiveRoot, 'ai_enrichment_tmp'));
for (const file of readdirSync(dataRoot, { withFileTypes: true })) {
  if (file.isFile() && (/^zsxq_.*\.json$/i.test(file.name) || file.name === 'security_master.json')) {
    move(resolve(dataRoot, file.name), resolve(archiveRoot, 'inputs', file.name));
  }
}
move(resolve(root, 'reports'), resolve(archiveRoot, 'reports'));
mkdirSync(resolve(root, 'reports'), { recursive: true });

const files = listFiles(archiveRoot).filter((path) => basename(path) !== 'manifest.json').map((path) => ({
  path: relative(archiveRoot, path).replaceAll('\\', '/'),
  size: statSync(path).size,
  sha256: digest(path),
}));
const manifest = {
  archive_version: 1,
  created_at: new Date().toISOString(),
  reason: '封存旧历史库，启用最近7天券商小作文工作流 v2',
  status: 'read_only_baseline',
  databases: databaseInspection,
  file_count: files.length,
  total_bytes: files.reduce((sum, file) => sum + file.size, 0),
  files,
};
writeFileSync(resolve(archiveRoot, 'manifest.json'), JSON.stringify(manifest, null, 2), 'utf8');
console.log(JSON.stringify({ archive: archiveRoot, file_count: manifest.file_count, total_bytes: manifest.total_bytes, databases: databaseInspection }, null, 2));
