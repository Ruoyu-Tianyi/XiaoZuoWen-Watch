import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { getZsxqCommand } from './lib/zsxq_command.mjs';
import { loadPipelineConfig } from './lib/direction_rules.mjs';
import { topicIsEligible } from './lib/topic_filter.mjs';

const GROUP_ID = process.env.ZSXQ_GROUP_ID || JSON.parse(readFileSync('config/workflow.json', 'utf8')).group_id;
if (!GROUP_ID) throw new Error('请配置 ZSXQ_GROUP_ID 或 config/workflow.json 中的 group_id');
const HOURS = Number(process.env.ZSXQ_HOURS || '72');
const OUTPUT = resolve(process.env.ZSXQ_RAW_OUTPUT || 'data/zsxq_topics_72h.json');
const cutoff = new Date(Date.now() - HOURS * 60 * 60 * 1000);
const pipelineConfig = loadPipelineConfig();

function fetchPage(endTime) {
  const args = ['group', '+topics', '--group-id', GROUP_ID, '--limit', '30', '--json'];
  if (endTime) args.push('--end-time', endTime);
  const invocation = getZsxqCommand(args);
  const result = spawnSync(invocation.command, invocation.args, {
    encoding: 'utf8', shell: invocation.shell, maxBuffer: 20 * 1024 * 1024,
  });
  if (result.status !== 0) {
    throw new Error((result.stderr || result.stdout || `zsxq-cli exited ${result.status}`).trim());
  }
  const start = result.stdout.indexOf('{');
  if (start < 0) throw new Error('zsxq-cli did not return JSON');
  return JSON.parse(result.stdout.slice(start));
}

const seen = new Map();
let endTime = null;
let pages = 0;
let reachedCutoff = false;

while (!reachedCutoff) {
  const page = fetchPage(endTime);
  pages += 1;
  for (const topic of page.topics_brief || []) {
    seen.set(String(topic.topic_id), topic);
  }
  const pageTopics = page.topics_brief || [];
  if (!pageTopics.length || !page.has_more || !page.next_end_time) break;
  const oldest = new Date(pageTopics.at(-1).create_time);
  if (oldest <= cutoff) {
    reachedCutoff = true;
    break;
  }
  if (page.next_end_time === endTime) throw new Error('Pagination cursor did not advance');
  endTime = page.next_end_time;
  if (pages >= 200) throw new Error('Safety stop after 200 pages');
}

const topics = [...seen.values()]
  .filter((topic) => new Date(topic.create_time) >= cutoff)
  .sort((a, b) => new Date(b.create_time) - new Date(a.create_time));

const payload = {
  group_id: GROUP_ID,
  group_name: topics[0]?.group?.name || '',
  fetched_at: new Date().toISOString(),
  cutoff_at: cutoff.toISOString(),
  window_hours: HOURS,
  pages_fetched: pages,
  count: topics.length,
  eligible_count: topics.filter((topic) => topicIsEligible(topic, pipelineConfig)).length,
  topics,
};

mkdirSync(dirname(OUTPUT), { recursive: true });
writeFileSync(OUTPUT, JSON.stringify(payload, null, 2), 'utf8');
console.log(JSON.stringify({ output: OUTPUT, pages, topics: topics.length, eligible_topics: payload.eligible_count, cutoff: cutoff.toISOString() }, null, 2));
