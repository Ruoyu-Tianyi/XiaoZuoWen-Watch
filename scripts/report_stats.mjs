import { DatabaseSync } from 'node:sqlite';
import { writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const dbPath = resolve(process.env.ZSXQ_DB || 'data/broker_notes.sqlite');
const output = resolve(process.env.ZSXQ_REPORT || 'reports/latest_stats.md');
const db = new DatabaseSync(dbPath, { readOnly: true });

const summary = db.prepare(`
  SELECT (SELECT count(*) FROM topics) topics,
         (SELECT count(*) FROM mentions) mentions,
         (SELECT count(*) FROM viewpoints) viewpoints,
         (SELECT count(DISTINCT topic_id) FROM mentions) topics_with_mentions,
         (SELECT count(*) FROM topics WHERE source_broker<>'') sourced_topics,
         (SELECT min(published_at) FROM topics) earliest,
         (SELECT max(published_at) FROM topics) latest
`).get();

function stats(hours, limit = 30) {
  return db.prepare(`
    SELECT s.name, s.code, s.market, count(DISTINCT m.topic_id) mentions,
           sum(m.direction='bullish') bullish,
           sum(m.direction='bearish') bearish,
           sum(m.direction='neutral') neutral,
           round(avg(m.entity_confidence),2) confidence
    FROM mentions m JOIN securities s ON s.id=m.security_id JOIN topics t ON t.topic_id=m.topic_id
    WHERE t.published_at_utc >= datetime('now', ?) AND m.entity_confidence >= 0.75
    GROUP BY s.id ORDER BY mentions DESC, bullish DESC, s.name LIMIT ?
  `).all(`-${hours} hours`, limit);
}

function table(rows) {
  const lines = ['| 标的 | 代码/市场 | 提及 | 看多 | 看空 | 中性 | 实体置信度 |', '|---|---:|---:|---:|---:|---:|---:|'];
  for (const row of rows) lines.push(`| ${row.name} | ${row.code}/${row.market} | ${row.mentions} | ${row.bullish} | ${row.bearish} | ${row.neutral} | ${row.confidence} |`);
  return lines.join('\n');
}

const lowConfidence = db.prepare(`
  SELECT t.published_at, s.name, s.code, m.entity_confidence, substr(m.evidence,1,120) evidence
  FROM mentions m JOIN securities s ON s.id=m.security_id JOIN topics t ON t.topic_id=m.topic_id
  WHERE m.entity_confidence < 0.7 ORDER BY t.published_at_utc DESC LIMIT 30
`).all();

const markdown = `# 券商小作文近 72 小时入库报告

- 数据范围：${summary.earliest} 至 ${summary.latest}
- 小作文：${summary.topics} 条
- 已识别标的提及：${summary.mentions} 条，覆盖 ${summary.topics_with_mentions} 篇小作文
- 观点段落：${summary.viewpoints} 条
- 已识别券商/团队来源：${summary.sourced_topics} 篇
- 统计口径：同一篇小作文对同一标的只计 1 次；方向为看多/看空/中性；最近 7 天因当前仅回填 72 小时，暂不完整。

## 最近 24 小时热度

${table(stats(24))}

## 最近 72 小时热度

${table(stats(72))}

## 待人工复核（低置信度）

${lowConfidence.length ? lowConfidence.map((row) => `- ${row.published_at}｜${row.name}(${row.code})｜${row.entity_confidence}｜${row.evidence}`).join('\n') : '- 无'}
`;

writeFileSync(output, markdown, 'utf8');
console.log(JSON.stringify({ output, summary, top24: stats(24).slice(0, 10), top72: stats(72).slice(0, 10) }, null, 2));
db.close();
