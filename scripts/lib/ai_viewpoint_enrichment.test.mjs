import test from 'node:test';
import assert from 'node:assert/strict';
import { buildSecurityIndex } from './ai_enrichment.mjs';
import { compareViewpointEntities, validateViewpointTopicResult, validateViewpointVerifierResult } from './ai_viewpoint_enrichment.mjs';

const index = buildSecurityIndex({ securities: [
  { code: '300750', market: 'SZ', name: '宁德时代' },
  { code: '00700', market: 'HK', name: '腾讯控股' },
] });

test('keeps macro viewpoints without forcing a security', () => {
  const topic = { topic_id: '1', title: '', content: '行业需求回暖，但价格仍然承压。', viewpoint_blocks: [{ ordinal: 1, text: '行业需求回暖，但价格仍然承压。' }] };
  const result = validateViewpointTopicResult(topic, {
    topic_id: '1', source: null, abstain_reason: '', ignored_blocks: [], viewpoints: [{
      ordinal: 1, source_block_ordinals: [1], summary: '行业需求回暖但价格承压', evidence: '行业需求回暖，但价格仍然承压。',
      viewpoint_type: 'industry', confidence: 0.96, entities: [], unresolved_entities: [],
    }],
  }, index);
  assert.equal(result.valid, true);
  assert.equal(result.viewpoints.length, 1);
  assert.equal(result.viewpoints[0].entities.length, 0);
});

test('validates viewpoint entity against security master and exact evidence', () => {
  const topic = { topic_id: '2', title: '', content: '重点推荐宁德时代，受益于储能需求增长。', viewpoint_blocks: [{ ordinal: 1, text: '重点推荐宁德时代，受益于储能需求增长。' }] };
  const result = validateViewpointTopicResult(topic, {
    topic_id: '2', source: null, abstain_reason: '', ignored_blocks: [], viewpoints: [{
      ordinal: 1, source_block_ordinals: [1], summary: '推荐宁德时代', evidence: '重点推荐宁德时代，受益于储能需求增长。',
      viewpoint_type: 'company', confidence: 0.98, unresolved_entities: [], entities: [{
        surface: '宁德时代', canonical_name: '宁德时代', code: '300750', market: 'SZ', direction: 'bullish',
        target_price: null, target_currency: '', evidence: '重点推荐宁德时代，受益于储能需求增长。', confidence: 0.98,
      }],
    }],
  }, index);
  assert.equal(result.valid, true);
  assert.deepEqual(result.viewpoints[0].entities.map((row) => `${row.market}:${row.code}`), ['SZ:300750']);
});

test('requires independent direction agreement for viewpoint entities', () => {
  const primary = { viewpoints: [{ ordinal: 1, source_block_ordinals: [1], entities: [{ market: 'HK', code: '00700', direction: 'bullish', confidence: 0.97 }] }] };
  const verifier = { viewpoints: [{ ordinal: 1, source_block_ordinals: [1], entities: [{ market: 'HK', code: '00700', direction: 'bearish', confidence: 0.98 }] }] };
  assert.equal(compareViewpointEntities(primary, verifier)[0].verified, false);
});

test('preserves unresolved candidate only with exact evidence', () => {
  const topic = { topic_id: '3', title: '', content: '建议关注星河科技，订单弹性较大。', viewpoint_blocks: [{ ordinal: 1, text: '建议关注星河科技，订单弹性较大。' }] };
  const result = validateViewpointTopicResult(topic, {
    topic_id: '3', source: null, abstain_reason: '', ignored_blocks: [], viewpoints: [{
      ordinal: 1, source_block_ordinals: [1], summary: '关注星河科技', evidence: '建议关注星河科技，订单弹性较大。',
      viewpoint_type: 'company', confidence: 0.9, entities: [], unresolved_entities: [{
        surface: '星河科技', canonical_name: '星河科技', direction: 'bullish', evidence: '建议关注星河科技，订单弹性较大。',
        confidence: 0.85, search_query: '星河科技 股票 代码', reason: '证券主表无唯一匹配',
      }],
    }],
  }, index);
  assert.equal(result.viewpoints[0].unresolved_entities.length, 1);
});

test('routes a specific security missing from the master into resolution queue candidates', () => {
  const topic = { topic_id: '4', title: '', content: '特斯拉新平台有望降低成本。', viewpoint_blocks: [{ ordinal: 1, text: '特斯拉新平台有望降低成本。' }] };
  const result = validateViewpointTopicResult(topic, {
    topic_id: '4', source: null, abstain_reason: '', ignored_blocks: [], viewpoints: [{
      ordinal: 1, source_block_ordinals: [1], summary: '特斯拉平台降本', evidence: '特斯拉新平台有望降低成本。',
      viewpoint_type: 'company', confidence: 0.95, unresolved_entities: [], entities: [{
        surface: '特斯拉', canonical_name: 'Tesla', code: 'TSLA', market: 'US', direction: 'bullish',
        target_price: null, target_currency: '', evidence: '特斯拉新平台有望降低成本。', confidence: 0.97,
      }],
    }],
  }, index);
  assert.equal(result.viewpoints[0].entities.length, 0);
  assert.equal(result.viewpoints[0].unresolved_entities[0].surface, '特斯拉');
  assert.match(result.viewpoints[0].unresolved_entities[0].reason, /security_master_missing/);
});

test('filters generic unresolved company phrases', () => {
  const topic = { topic_id: '5', title: '', content: '很多欧洲电池厂正在扩产。', viewpoint_blocks: [{ ordinal: 1, text: '很多欧洲电池厂正在扩产。' }] };
  const result = validateViewpointTopicResult(topic, {
    topic_id: '5', source: null, abstain_reason: '', ignored_blocks: [], viewpoints: [{
      ordinal: 1, source_block_ordinals: [1], summary: '欧洲电池厂扩产', evidence: '很多欧洲电池厂正在扩产。',
      viewpoint_type: 'industry', confidence: 0.9, entities: [], unresolved_entities: [{
        surface: '很多欧洲电池厂', canonical_name: '', direction: 'neutral', evidence: '很多欧洲电池厂正在扩产。',
        confidence: 0.8, search_query: '欧洲电池厂 股票', reason: '未指明名称',
      }],
    }],
  }, index);
  assert.equal(result.viewpoints[0].unresolved_entities.length, 0);
});

test('validates compact independent verifier output', () => {
  const topic = { topic_id: '6', title: '', content: '重点推荐宁德时代，受益于储能需求增长。' };
  const result = validateViewpointVerifierResult(topic, {
    topic_id: '6', source: null, abstain_reason: '', unresolved_entities: [], entities: [{
      surface: '宁德时代', canonical_name: '宁德时代', code: '300750', market: 'SZ', direction: 'bullish',
      evidence: '重点推荐宁德时代，受益于储能需求增长。', confidence: 0.98,
    }],
  }, index);
  assert.equal(result.valid, true);
  assert.deepEqual(result.viewpoints[0].entities.map((row) => `${row.market}:${row.code}:${row.direction}`), ['SZ:300750:bullish']);
});
