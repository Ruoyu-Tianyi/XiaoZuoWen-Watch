import test from 'node:test';
import assert from 'node:assert/strict';
import { buildSecurityIndex, compareVerified, normalizeBroker, shouldAutoAcceptMention, validateTopicResult } from './ai_enrichment.mjs';

const master = { securities: [{ code: '300750', market: 'SZ', name: '宁德时代' }, { code: '01810', market: 'HK', name: '小米集团-W' }] };
const index = buildSecurityIndex(master);
const topic = { topic_id: '1', title: '宁德时代高弹性', content: '建议关注宁德时代，目标价300元。' };

test('validates exact security and evidence', () => {
  const result = validateTopicResult(topic, { topic_id: '1', mentions: [{ surface: '宁德时代', canonical_name: '宁德时代', code: '300750', market: 'SZ', direction: 'bullish', target_price: 300, target_currency: 'CNY', evidence: '建议关注宁德时代，目标价300元。', confidence: 0.97 }], source: null }, index);
  assert.equal(result.valid, true);
  assert.equal(result.mentions[0].market, 'SZ');
});

test('filters fabricated evidence and A market without failing the whole topic', () => {
  const result = validateTopicResult(topic, { topic_id: '1', mentions: [{ canonical_name: '宁德时代', code: '300750', market: 'A', direction: 'bullish', evidence: '不存在', confidence: 0.99 }], source: null }, index);
  assert.equal(result.valid, true);
  assert.equal(result.mentions.length, 0);
});

test('requires independent agreement for automatic mention', () => {
  const base = { mentions: [{ code: '300750', market: 'SZ', direction: 'bullish', target_price: 300, confidence: 0.97 }], source: null };
  const verified = compareVerified(base, { mentions: [{ code: '300750', market: 'SZ', direction: 'bullish', target_price: 300, confidence: 0.96 }], source: null });
  assert.equal(shouldAutoAcceptMention(verified.mentions[0]), true);
});

test('normalizes broker legal suffixes', () => {
  assert.equal(normalizeBroker('中信证券股份有限公司'), '中信证券');
});
