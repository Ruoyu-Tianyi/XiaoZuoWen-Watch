import test from 'node:test';
import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { readableText, originalEvidence, normalizeName, securityResolver, normalizeSource, normalizeView, brokerSignatureMatches } from './note_normalization.mjs';

// These securities and aliases are synthetic: fixtures test matching mechanics,
// not guessed aliases or claims about a real listed company's identity.
function master(t) {
  const db = new DatabaseSync(':memory:');
  t.after(() => db.close());
  db.exec(`CREATE TABLE securities(id INTEGER PRIMARY KEY,name TEXT,full_name TEXT,ts_code TEXT,market TEXT,symbol TEXT);
    CREATE TABLE security_aliases(alias TEXT,security_id INTEGER,source TEXT);
    INSERT INTO securities VALUES
      (1,'甲科技','甲科技股份有限公司','300001.SZ','SZ','300001'),
      (2,'甲科技-W','甲科技股份有限公司','00001.HK','HK','00001'),
      (3,'乙药业-B-SW','乙药业股份有限公司','00002.HK','HK','00002'),
      (4,'ALPHA','ALPHA LIMITED','00003.HK','HK','00003'),
      (5,'丙证券','丙证券股份有限公司','600001.SH','SH','600001'),
      (6,'丙控股','丙控股有限公司','00004.HK','HK','00004'),
      (7,'丁科技','丁科技股份有限公司','300002.SZ','SZ','300002');
    INSERT INTO security_aliases VALUES('甲科',1,'human_verified'),('甲科',2,'human_verified'),
      ('丙',5,'source_verified'),('丙',6,'source_verified'),('科技',1,'source_verified'),('科技',7,'source_verified'),
      ('孤儿别名',999,'source_verified');`);
  return securityResolver(db);
}
const topic = content => ({ title: '测试原文', content });
const raw = (surface, extra = {}) => ({ security_surface: surface, security_code_hint: null,
  security_evidence: surface, evidence: `推荐关注${surface}`, summary: `看好${surface}`, direction: 'bullish', confidence: 0.95,
  target_price: null, target_currency: null, target_price_raw: null, target_price_evidence: null, ...extra });

test('decodes emoji/hashtag labels and named/numeric entities with raw offsets', () => {
  const encoded = '<e type="emoji" title="%5B%E7%BA%A2%E5%8C%85%5D"/>';
  const name = '<e type="hashtag" title="%E7%94%B2%E7%A7%91%E6%8A%80"/>';
  const original = topic(`${encoded}推荐关注${name}，A&amp;B &#x1F525; &QUOT;好&#39;`);
  const text = readableText(original.content).text;
  assert.equal(text, '[红包]推荐关注甲科技，A&B 🔥 "好\'');
  assert.equal(originalEvidence(original, '[红包]推荐关注甲科技'), `${encoded}推荐关注${name}`);
  assert.equal(originalEvidence(original, '证券不存在'), null);
  assert.equal(originalEvidence(original, '证据：“推荐关注甲科技”'), `推荐关注${name}`);
});

test('security names permit deterministic listing suffix removal, not Latin stem damage', () => {
  assert.equal(normalizeName('乙药业-B-SW'), '乙药业');
  assert.equal(normalizeName('*ST丁科技'), '丁科技');
  assert.equal(normalizeName('STMicroelectronics'), 'STMicroelectronics');
});

test('unmarked dual listing defaults to A, HK-only exact names remain recognizable', t => {
  const resolve = master(t);
  assert.equal(resolve('甲科技', null, topic('推荐关注甲科技')).securities[0].ts_code, '300001.SZ');
  assert.equal(resolve('甲科技', '300001', topic('推荐关注甲科技')).status, 'resolved');
  assert.equal(resolve('甲科技', '00001.HK', topic('推荐关注甲科技')).status, 'ambiguous');
  assert.equal(resolve('乙药业', null, topic('推荐关注乙药业')).securities[0].ts_code, '00002.HK');
  assert.equal(resolve('ALPHA', null, topic('推荐关注ALPHA')).status, 'resolved');
});

test('AH expands only a provable single company and source-supported market markers', t => {
  const resolve = master(t);
  assert.deepEqual(resolve('甲科AH', null, topic('推荐关注甲科AH')).securities.map(s => s.ts_code), ['300001.SZ','00001.HK']);
  assert.equal(resolve('甲科技', null, topic('推荐关注甲科技 H')).securities[0].market, 'HK');
  assert.equal(resolve('甲科技 H', null, topic('推荐关注甲科技')).status, 'ambiguous');
  assert.equal(resolve('丙AH', null, topic('推荐关注丙AH')).status, 'ambiguous');
  assert.equal(resolve('甲科技', null, topic('甲科技A偏贵，甲科技H推荐关注')).status, 'ambiguous');
});

test('hints do not invent entities, resolve ambiguous aliases or discard actual A/H securities', t => {
  const resolve = master(t);
  assert.equal(resolve('未知公司', '300001.SZ', topic('推荐关注未知公司')).status, 'unresolved');
  assert.equal(resolve('科技', '300001.SZ', topic('推荐关注科技')).status, 'ambiguous');
  assert.equal(resolve('甲科技', '99999.US', topic('推荐关注甲科技')).status, 'ambiguous');
  assert.equal(resolve('NEW HK ENTITY', null, topic('推荐关注NEW HK ENTITY')).status, 'unresolved');
  assert.equal(resolve('foreign entity', 'TEST.US', topic('foreign entity')).status, 'out_of_scope');
});

test('commodity and medicine products are excluded while unresolved names retain candidate issues', t => {
  const resolve = master(t);
  for (const name of ['黄金','洛索洛芬钠凝胶贴膏','家具及其零件']) assert.equal(resolve(name, null, topic(name)).status, 'out_of_scope');
  const result = normalizeView(topic('推荐关注未知药业'), raw('未知药业'), resolve);
  assert.equal(result.surface, '未知药业');
  assert.equal(result.resolution.status, 'unresolved');
  assert.ok(result.issues.length);
});

test('surface and security evidence must identify the same object', t => {
  const resolve = master(t);
  const result = normalizeView(topic('推荐关注甲科技，另有丁科技'), raw('甲科技', { security_evidence: '丁科技' }), resolve);
  assert.ok(result.issues.includes('标的证据未包含当前证券名称'));
});

test('normalizes signed broker and embedded team while rejecting stock mention as attribution', () => {
  const original = topic('【长江电新】推荐关注甲科技');
  for (const recommendation of [{ broker: '长江', research_group: '长江电新', evidence: '【长江电新】' },
    { broker: '长江电新', research_group: null, evidence: '长江电新' }]) {
    const result = normalizeSource(original, recommendation);
    assert.equal(result.broker, '长江证券'); assert.equal(result.group, '电新'); assert.deepEqual(result.issues, []);
  }
  const unsigned = normalizeSource(topic('推荐关注中信证券'), { broker: '中信证券', evidence: '中信证券' });
  assert.equal(unsigned.broker, null); assert.ok(unsigned.issues.length);
  assert.equal(normalizeSource(topic('推荐关注甲科技'), {}).broker, null);
  const emptyTeam = normalizeSource(topic('【中信证券】推荐关注甲科技'), { broker: '中信证券', research_group: '其他', evidence: '【中信证券】' });
  assert.equal(emptyTeam.group, '其他'); assert.deepEqual(emptyTeam.issues, []);
});

test('broker signature aliases support known abbreviations without matching another broker', () => {
  assert.equal(brokerSignatureMatches('中信建投', '【建投食饮】'), true);
  assert.equal(brokerSignatureMatches('申万宏源', '申万计算机'), true);
  assert.equal(brokerSignatureMatches('中国银河', '银河医药'), true);
  assert.equal(brokerSignatureMatches('中信证券', '【中信建投食饮】'), false);
  const signature = '<e title="%E5%BB%BA%E6%8A%95%E9%A3%9F%E9%A5%AE"/>';
  assert.equal(brokerSignatureMatches('中信建投', signature), true);
});

test('元 follows unambiguous security market; A/H combined targets need currency evidence', t => {
  const resolve = master(t);
  for (const [name, expected] of [['甲科技','CNY'], ['乙药业','HKD']]) {
    const result = normalizeView(topic(`推荐关注${name}，目标价30元`), raw(name, { target_price: 30, target_currency: '元', target_price_raw: '30元', target_price_evidence: '目标价30元' }), resolve);
    assert.equal(result.targetCurrency, expected); assert.deepEqual(result.issues, []);
  }
  const ah = normalizeView(topic('推荐关注甲科技AH，目标价30元'), raw('甲科技AH', { target_price: 30, target_currency: '元', target_price_raw: '30元', target_price_evidence: '目标价30元' }), resolve);
  assert.equal(ah.targetCurrency, null); assert.ok(ah.issues.length);
  const wrong = normalizeView(topic('推荐关注乙药业，目标价30港元'), raw('乙药业', { target_price: 30, target_currency: 'CNY', target_price_raw: '30', target_price_evidence: '目标价30港元' }), resolve);
  assert.ok(wrong.issues.includes('目标价币种与原文明确标注不一致'));
});

test('market cap is cleared without erasing an actual share target in a broad evidence span', t => {
  const resolve = master(t);
  const content = '推荐关注甲科技，市值空间1000亿元，对应目标价30元';
  const proper = normalizeView(topic(content), raw('甲科技', { target_price: 30, target_currency: '元', target_price_raw: '30元', target_price_evidence: content }), resolve);
  assert.equal(proper.targetPrice, 30); assert.equal(proper.targetCurrency, 'CNY'); assert.deepEqual(proper.issues, []);
  const cap = normalizeView(topic(content), raw('甲科技', { target_price: 1000, target_currency: '亿元', target_price_raw: '1000亿元', target_price_evidence: content }), resolve);
  assert.equal(cap.targetPrice, null); assert.ok(cap.normalizationNotes.length); assert.deepEqual(cap.issues, []);
});

test('invalid target evidence remains a candidate issue rather than silently vanishing', t => {
  const resolve = master(t);
  const result = normalizeView(topic('推荐关注甲科技，目标价30元'), raw('甲科技', { target_price: 999, target_currency: 'CNY', target_price_raw: '999元', target_price_evidence: '目标价30元' }), resolve);
  assert.equal(result.targetPrice, 999); assert.ok(result.issues.length); assert.equal(result.targetRaw, null);
});
