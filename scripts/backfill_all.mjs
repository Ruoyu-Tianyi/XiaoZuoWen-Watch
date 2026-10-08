import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { getZsxqCommand } from './lib/zsxq_command.mjs';
import { loadPipelineConfig } from './lib/direction_rules.mjs';
import { topicIsEligible } from './lib/topic_filter.mjs';

const GROUP_ID = process.env.ZSXQ_GROUP_ID || JSON.parse(readFileSync('config/workflow.json', 'utf8')).group_id;
if (!GROUP_ID) throw new Error('请配置 ZSXQ_GROUP_ID 或 config/workflow.json 中的 group_id');
const DB_PATH = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const STAGING_PATH = resolve(process.env.ZSXQ_BACKFILL_STAGING || 'data/zsxq_backfill_chunk.json');
const BATCH_SIZE = Math.max(30, Number(process.env.ZSXQ_BACKFILL_BATCH || '500'));
const MAX_PAGES = Math.max(1, Number(process.env.ZSXQ_BACKFILL_MAX_PAGES || '5000'));
const pipelineConfig = loadPipelineConfig();

function sleep(milliseconds) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, milliseconds);
}

function runCli(endTime) {
  const args = ['group', '+topics', '--group-id', GROUP_ID, '--limit', '30', '--json'];
  if (endTime) args.push('--end-time', endTime);
  let lastError = '';
  for (let attempt = 0; attempt < 4; attempt += 1) {
    const invocation = getZsxqCommand(args);
    const result = spawnSync(invocation.command, invocation.args, {
      encoding: 'utf8', shell: invocation.shell, maxBuffer: 30 * 1024 * 1024,
    });
    if (result.status === 0) {
      const start = result.stdout.indexOf('{');
      if (start >= 0) return JSON.parse(result.stdout.slice(start));
      lastError = 'zsxq-cli did not return JSON';
    } else {
      lastError = (result.stderr || result.stdout || `zsxq-cli exited ${result.status}`).trim();
    }
    if (attempt < 3) sleep([5000, 15000, 30000][attempt]);
  }
  throw new Error(lastError);
}

function existingTopicIds() {
  const db = new DatabaseSync(DB_PATH, { readOnly: true });
  try { return new Set(db.prepare('SELECT topic_id FROM topics').all().map((row) => String(row.topic_id))); }
  finally { db.close(); }
}

function importBatch(topics, metadata) {
  if (!topics.length) return;
  const payload = {
    group_id: GROUP_ID,
    group_name: topics[0]?.group?.name || '',
    fetched_at: new Date().toISOString(),
    cutoff_at: topics.at(-1)?.create_time || new Date(0).toISOString(),
    window_hours: null,
    pages_fetched: metadata.pages,
    count: topics.length,
    topics,
  };
  mkdirSync(dirname(STAGING_PATH), { recursive: true });
  writeFileSync(STAGING_PATH, JSON.stringify(payload), 'utf8');
  const result = spawnSync(process.execPath, [resolve('scripts/build_database.mjs')], {
    cwd: process.cwd(),
    env: { ...process.env, ZSXQ_RAW_OUTPUT: STAGING_PATH, ZSXQ_DB: DB_PATH },
    encoding: 'utf8',
    maxBuffer: 30 * 1024 * 1024,
  });
  if (result.status !== 0) throw new Error((result.stderr || result.stdout || `import exited ${result.status}`).trim());
  for (const topic of topics) metadata.existing.add(String(topic.topic_id));
  metadata.imported += topics.length;
  console.log(`[回填] 已入库 ${metadata.imported} 篇新增主题；当前翻到第 ${metadata.pages} 页`);
}

const metadata = { pages: 0, imported: 0, skipped: 0, excluded: 0, existing: existingTopicIds() };
const pageSeen = new Set();
let pending = [];
let endTime = null;
let finished = false;

console.log(`[回填] 开始，全库已有 ${metadata.existing.size} 篇主题，批次 ${BATCH_SIZE} 篇`);
while (!finished && metadata.pages < MAX_PAGES) {
  const page = runCli(endTime);
  metadata.pages += 1;
  const topics = page.topics_brief || page.topics || [];
  for (const topic of topics) {
    const id = String(topic.topic_id);
    if (pageSeen.has(id)) continue;
    pageSeen.add(id);
    if (!topicIsEligible(topic, pipelineConfig)) {
      metadata.excluded += 1;
      continue;
    }
    if (metadata.existing.has(id)) metadata.skipped += 1;
    else pending.push(topic);
  }
  if (pending.length >= BATCH_SIZE) {
    importBatch(pending, metadata);
    pending = [];
  }
  if (metadata.pages % 10 === 0) {
    console.log(`[回填] 第 ${metadata.pages} 页：新增待入库 ${pending.length}，已跳过 ${metadata.skipped}`);
  }
  if (!topics.length || !page.has_more || !page.next_end_time) {
    finished = true;
    break;
  }
  if (page.next_end_time === endTime) {
    const fallback = new Date(new Date(endTime).getTime() - 1).toISOString();
    console.log(`[回填] 接口游标停滞于 ${endTime}，回退 1 毫秒后继续`);
    endTime = fallback;
  } else {
    endTime = page.next_end_time;
  }
}

if (!finished) throw new Error(`Safety stop after ${MAX_PAGES} pages; increase ZSXQ_BACKFILL_MAX_PAGES to continue`);
importBatch(pending, metadata);
console.log(JSON.stringify({ status: 'complete', pages: metadata.pages, imported: metadata.imported, skipped: metadata.skipped, excluded: metadata.excluded, total_topics: metadata.existing.size }, null, 2));
