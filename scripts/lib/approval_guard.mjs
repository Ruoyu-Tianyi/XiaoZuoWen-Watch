import { createHash, randomUUID } from 'node:crypto';
import { mkdir } from 'node:fs/promises';
import { resolve } from 'node:path';
import { brokerSignatureMatches } from './note_normalization.mjs';

export const APPROVAL_POLICY_VERSION = 'viewpoint-adjudication-v2';
const MARKETS = new Set(['SH', 'SZ', 'BJ', 'HK']);
const CURRENCIES = new Set(['CNY', 'HKD', 'USD', 'EUR', 'GBP', 'JPY', 'SGD', 'AUD', 'CHF', 'CAD']);
const now = () => new Date().toISOString();
const hash = (value) => createHash('sha256').update(JSON.stringify(value)).digest('hex');

export function ensureApprovalSchema(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS viewpoint_approval_audit (
      id INTEGER PRIMARY KEY,
      viewpoint_id INTEGER NOT NULL REFERENCES viewpoints(id),
      topic_id TEXT NOT NULL REFERENCES raw_topics(topic_id),
      decision TEXT NOT NULL CHECK(decision IN ('accept','exclude','uncertain','invalidated')),
      method TEXT NOT NULL CHECK(method IN ('human','ai','system')),
      scope_decision TEXT NOT NULL CHECK(scope_decision IN ('broker_note','not_broker_note','uncertain')),
      reason TEXT NOT NULL,
      policy_version TEXT NOT NULL,
      model_id TEXT,
      content_hash TEXT NOT NULL,
      review_version INTEGER NOT NULL,
      fingerprint TEXT NOT NULL,
      metadata_json TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_viewpoint_approval_audit_latest ON viewpoint_approval_audit(viewpoint_id,id DESC);
    CREATE TRIGGER IF NOT EXISTS viewpoint_approval_audit_no_update BEFORE UPDATE ON viewpoint_approval_audit
      BEGIN SELECT RAISE(ABORT,'审批审计为追加记录，不得覆盖'); END;
    CREATE TRIGGER IF NOT EXISTS viewpoint_approval_audit_no_delete BEFORE DELETE ON viewpoint_approval_audit
      BEGIN SELECT RAISE(ABORT,'审批审计为追加记录，不得删除'); END;
    CREATE TABLE IF NOT EXISTS viewpoint_publish_batches (
      id TEXT PRIMARY KEY,
      backup_path TEXT NOT NULL,
      viewpoint_ids_json TEXT NOT NULL,
      approval_ids_json TEXT NOT NULL,
      created_at TEXT NOT NULL
    );
  `);
}

function snapshot(db, id) {
  const viewpoint = db.prepare('SELECT * FROM viewpoints WHERE id=?').get(id);
  if (!viewpoint) throw new Error(`观点 ${id} 不存在`);
  const topic = db.prepare('SELECT topic_id,title,content,content_hash,eligibility FROM raw_topics WHERE topic_id=?').get(viewpoint.topic_id);
  const links = db.prepare(`SELECT vs.*,s.ts_code,s.market,s.name,s.symbol,s.list_status
    FROM viewpoint_securities vs LEFT JOIN securities s ON s.id=vs.security_id
    WHERE vs.viewpoint_id=? ORDER BY vs.link_order,vs.id`).all(id);
  const classification = db.prepare(`SELECT id,classification,content_hash,evidence FROM topic_classifications
    WHERE topic_id=? ORDER BY created_at DESC,id DESC LIMIT 1`).get(viewpoint.topic_id) || null;
  // Workflow state/timestamps do not change the evidence being approved. All content,
  // linking, resolver output and security master identity do participate in the hash.
  const { review_status, updated_at, published_at, ...content } = viewpoint;
  return { viewpoint, topic, links, classification, fingerprint: hash({ viewpoint: content, topic, links, classification }) };
}

export function viewpointFingerprint(db, id) {
  const value = snapshot(db, id);
  return { fingerprint: value.fingerprint, contentHash: value.topic.content_hash, reviewVersion: value.viewpoint.review_version };
}

export function recordApproval(db, options) {
  const { viewpointId, decision, method, reason, scopeDecision, modelId = null,
    policyVersion = APPROVAL_POLICY_VERSION, expectedFingerprint, metadata = {} } = options;
  if (!['accept', 'exclude', 'uncertain'].includes(decision)) throw new Error('无效的审核决定');
  if (!['human', 'ai'].includes(method)) throw new Error('审核方式必须为 human 或 ai');
  if (!['broker_note', 'not_broker_note', 'uncertain'].includes(scopeDecision)) throw new Error('必须明确裁决是否属于卖方观点');
  if (decision === 'accept' && scopeDecision !== 'broker_note') throw new Error('仅明确属于卖方观点的内容可以接受');
  if (!String(reason || '').trim()) throw new Error('审核必须保留裁决理由');
  if (method === 'ai' && (!modelId || policyVersion !== APPROVAL_POLICY_VERSION)) throw new Error('AI审批必须注明模型并采用当前裁决规则');
  const value = snapshot(db, viewpointId);
  if (expectedFingerprint && expectedFingerprint !== value.fingerprint) throw new Error('观点已发生变化，请重新审核');
  if (value.viewpoint.review_status === 'published') throw new Error('已发布观点不能覆盖审批，请创建更正版本');
  const inserted = db.prepare(`INSERT INTO viewpoint_approval_audit
    (viewpoint_id,topic_id,decision,method,scope_decision,reason,policy_version,model_id,content_hash,review_version,fingerprint,metadata_json,created_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`).run(viewpointId, value.viewpoint.topic_id, decision, method, scopeDecision,
    String(reason).trim(), policyVersion, modelId, value.topic.content_hash, value.viewpoint.review_version,
    value.fingerprint, JSON.stringify(metadata), now());
  return db.prepare('SELECT * FROM viewpoint_approval_audit WHERE id=?').get(Number(inserted.lastInsertRowid));
}

export function invalidateApproval(db, viewpointId, reason = '观点经编辑，旧审批失效') {
  const value = snapshot(db, viewpointId);
  const result = db.prepare(`INSERT INTO viewpoint_approval_audit
    (viewpoint_id,topic_id,decision,method,scope_decision,reason,policy_version,content_hash,review_version,fingerprint,created_at)
    VALUES(?,?,'invalidated','system','uncertain',?,?,?,?,?,?)`).run(viewpointId, value.viewpoint.topic_id,
    reason, APPROVAL_POLICY_VERSION, value.topic.content_hash, value.viewpoint.review_version, value.fingerprint, now());
  return Number(result.lastInsertRowid);
}

function hasTable(db, name) {
  return Boolean(db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(name));
}

export function currentApproval(db, viewpointId) {
  if (!hasTable(db, 'viewpoint_approval_audit')) return null;
  return db.prepare('SELECT * FROM viewpoint_approval_audit WHERE viewpoint_id=? ORDER BY id DESC LIMIT 1').get(viewpointId) || null;
}

const ISSUE_LABELS = {
  missing_viewpoint: '观点不存在', not_reviewed: '观点尚未接受', missing_approval: '缺少当前规则的明确审批',
  approval_not_accepted: '最新审批未接受', stale_approval: '原文、观点或证券信息已变化，需要重新审核',
  invalid_approval: '审批规则、模型或范围裁决无效', excluded_topic: '原文不在可处理范围内',
  invalid_direction: '观点方向不明确', invalid_evidence: '观点证据不是原文精确片段',
  invalid_supporting_evidence: '补充证据不完整或不在原文', missing_security: '缺少证券关联',
  unresolved_security: '证券尚未全部解析', illegal_security: '证券代码或市场不合法',
  invalid_security_evidence: '标的证据不在原文', duplicate_security: '同篇同标的存在重复已接受或已发布观点',
  invalid_source: '券商或研究组缺少明确的原文署名证据', invalid_target: '目标价、币种或价格证据不合法',
};

export function validatePublishBatch(db, ids) {
  const selected = ids == null ? db.prepare("SELECT id FROM viewpoints WHERE review_status='reviewed' ORDER BY id").all().map((row) => row.id)
    : [...new Set(ids.map(Number))];
  if (selected.some((id) => !Number.isSafeInteger(id) || id <= 0)) throw new Error('发布清单包含无效观点 ID');
  const selectedSet = new Set(selected);
  const issues = [];
  const approvals = [];
  const fingerprints = {};
  const add = (id, code) => {
    if (!issues.some((issue) => issue.viewpoint_id === id && issue.code === code)) issues.push({ viewpoint_id: id, code, message: ISSUE_LABELS[code] });
  };
  for (const id of selected) {
    let value;
    try { value = snapshot(db, id); } catch { add(id, 'missing_viewpoint'); continue; }
    const { viewpoint: v, topic, links, fingerprint } = value;
    fingerprints[id] = fingerprint;
    if (v.review_status !== 'reviewed') add(id, 'not_reviewed');
    const approval = currentApproval(db, id);
    if (!approval) add(id, 'missing_approval');
    else {
      approvals.push({ viewpoint_id: id, approval_id: approval.id });
      if (approval.decision !== 'accept') add(id, 'approval_not_accepted');
      if (approval.fingerprint !== fingerprint || approval.content_hash !== topic.content_hash
          || approval.review_version !== v.review_version) add(id, 'stale_approval');
      if (!['human', 'ai'].includes(approval.method) || approval.scope_decision !== 'broker_note'
          || (approval.method === 'ai' && (approval.policy_version !== APPROVAL_POLICY_VERSION || !approval.model_id))) add(id, 'invalid_approval');
    }
    if (topic.eligibility !== 'eligible') add(id, 'excluded_topic');
    if (!['bullish', 'bearish'].includes(v.direction)) add(id, 'invalid_direction');
    const original = `${topic.title || ''}\n${topic.content || ''}`;
    const exact = (text) => typeof text === 'string' && text.trim().length > 0 && original.includes(text);
    if (!exact(v.evidence)) add(id, 'invalid_evidence');
    try {
      const supporting = JSON.parse(v.supporting_evidence_json);
      if (!Array.isArray(supporting) || supporting.some((text) => !exact(text))) add(id, 'invalid_supporting_evidence');
    } catch { add(id, 'invalid_supporting_evidence'); }
    if (!links.length) add(id, 'missing_security');
    const linked = new Set();
    for (const link of links) {
      if (link.resolution_status !== 'resolved' || !link.security_id) add(id, 'unresolved_security');
      if (!MARKETS.has(link.market) || !new RegExp(`^\\d{${link.market === 'HK' ? '5' : '6'}}\\.${link.market}$`).test(link.ts_code || '')) add(id, 'illegal_security');
      if (!exact(link.evidence)) add(id, 'invalid_security_evidence');
      if (linked.has(link.security_id)) add(id, 'duplicate_security');
      linked.add(link.security_id);
      if (link.security_id) {
        const other = db.prepare(`SELECT DISTINCT v.id,v.review_status FROM viewpoints v JOIN viewpoint_securities vs ON vs.viewpoint_id=v.id
          WHERE v.topic_id=? AND vs.security_id=? AND v.id<>? AND v.review_status IN ('reviewed','published')`).all(v.topic_id, link.security_id, id);
        if (other.some((row) => row.review_status === 'published' || selectedSet.has(row.id))) add(id, 'duplicate_security');
      }
    }
    const broker = String(v.recommendation_broker || '').trim();
    const team = String(v.recommendation_team || '').trim();
    const signature = v.recommendation_evidence;
    if (broker) {
      if (!exact(signature) || !brokerSignatureMatches(broker, signature)) add(id, 'invalid_source');
    } else if (team && team !== '其他' && !exact(signature)) add(id, 'invalid_source');
    if (v.target_price != null) {
      const priceNumbers = String(v.target_price_evidence || '').match(/\d+(?:,\d{3})*(?:\.\d+)?/gu) || [];
      if (typeof v.target_price !== 'number' || !Number.isFinite(v.target_price) || v.target_price <= 0
          || !CURRENCIES.has(v.target_currency) || !exact(v.target_price_evidence)
          || !priceNumbers.some((number) => Number(number.replaceAll(',', '')) === v.target_price)
          || (v.target_price_raw && !exact(v.target_price_raw))
          || (/(?:市值|万亿|亿元)/u.test(v.target_price_evidence || '') && !/(?:目标价|股价|每股|元\/股)/u.test(v.target_price_evidence || ''))) add(id, 'invalid_target');
    } else if (v.target_currency || v.target_price_raw || v.target_price_evidence) add(id, 'invalid_target');
  }
  return { ok: issues.length === 0, ids: selected, issues, approvals, fingerprints };
}

function failPreflight(report) {
  const error = new Error(`发布前检查未通过：${report.issues.slice(0, 6).map((item) => `${item.viewpoint_id} ${item.message}`).join('；')}`);
  error.code = 'PUBLISH_VALIDATION_FAILED';
  error.issues = report.issues;
  throw error;
}

export async function publishAccepted(db, { ids, backupDir = 'data/backups-v2', onPublish } = {}) {
  ensureApprovalSchema(db);
  const initial = validatePublishBatch(db, ids);
  if (!initial.ok) failPreflight(initial);
  if (!initial.ids.length) return { published: 0, backup: null, batchId: null };
  const batchId = randomUUID();
  const backupPath = resolve(backupDir, `broker_notes_before_publish_${now().replace(/[:.]/g, '-')}_${batchId}.sqlite`);
  await mkdir(resolve(backupDir), { recursive: true });
  // VACUUM INTO must succeed before publication. Then lock, repeat every gate and
  // compare the snapshot so an edit during backup cannot use an older approval.
  db.exec(`VACUUM INTO '${backupPath.replaceAll("'", "''")}'`);
  db.exec('BEGIN IMMEDIATE');
  try {
    const checked = validatePublishBatch(db, initial.ids);
    if (!checked.ok) failPreflight(checked);
    if (JSON.stringify(checked.fingerprints) !== JSON.stringify(initial.fingerprints)
        || JSON.stringify(checked.approvals) !== JSON.stringify(initial.approvals)) throw new Error('备份期间审核内容已变化，请重新发布');
    const when = now();
    db.prepare(`INSERT INTO viewpoint_publish_batches(id,backup_path,viewpoint_ids_json,approval_ids_json,created_at)
      VALUES(?,?,?,?,?)`).run(batchId, backupPath, JSON.stringify(checked.ids), JSON.stringify(checked.approvals), when);
    const update = db.prepare("UPDATE viewpoints SET review_status='published',published_at=?,updated_at=? WHERE id=? AND review_status='reviewed'");
    for (const id of checked.ids) {
      if (Number(update.run(when, when, id).changes) !== 1) throw new Error(`观点 ${id} 状态已变化`);
    }
    if(onPublish)onPublish({published:checked.ids.length,backup:backupPath,batchId,ids:checked.ids});
    db.exec('COMMIT');
    return { published: checked.ids.length, backup: backupPath, batchId };
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}
