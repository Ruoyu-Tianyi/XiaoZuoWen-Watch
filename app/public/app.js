const state = { view: 'dashboard', window: 24, q: '', direction: '', broker: '', data: null, pollTimer: null, historyQuery: '', reviewPage: 1, reviewPages: 1, automation: null };
const $ = (selector) => document.querySelector(selector);
const $$ = (selector) => [...document.querySelectorAll(selector)];

function formatNumber(value) { return new Intl.NumberFormat('zh-CN').format(Number(value || 0)); }
function formatPercent(value) { return `${Math.round(Number(value || 0) * 100)}%`; }
function formatTime(value) {
  if (!value) return '暂无';
  return new Intl.DateTimeFormat('zh-CN', { month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(value));
}
function directionLabel(value) { return ({ bullish: '看多', bearish: '看空' })[value] || value || '未判断'; }
function classificationLabel(value) { return ({ broker_note: '小作文', not_broker_note: '非小作文', uncertain: '不确定' })[value] || '未分类'; }
function stageLabel(value) { return ({ fetch: '抓取原文', select: '选择增量', primary: '分类与观点识别', review: 'AI逐条裁决', retry: '等待补齐失败项', done: '处理结束', extract: '观点识别', extraction: '观点识别', classification: '文本分类', approval: 'AI审核', adjudicate: 'AI裁决', completed: '处理完成' })[value] || '准备处理'; }
function statusLabel(value) { return ({ idle: '空闲', running: '运行中', paused: '已暂停', completed: '处理完成', completed_with_issues: '已完成，仍有疑难项', success: '成功', failed: '失败', candidate: '待复核', reviewed: '已接受', published: '已发布', rejected: '已排除 / 驳回' })[value] || value || '暂无'; }

async function api(path, options = {}) {
  const response = await fetch(path, options);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.message || payload.error || `HTTP ${response.status}`);
  return payload;
}

function jsonOptions(method, body) {
  return { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
}

function params() {
  const query = new URLSearchParams({ window: String(state.window), limit: '50' });
  if (state.q) query.set('q', state.q);
  if (state.direction) query.set('direction', state.direction);
  if (state.broker) query.set('broker', state.broker);
  return query;
}

function showNotice(selector, message, isError = false) {
  const notice = $(selector);
  notice.textContent = message;
  notice.classList.toggle('hidden', !message);
  notice.classList.toggle('error', isError);
}

async function loadDashboard() {
  try {
    document.body.classList.add('loading');
    const data = await api(`/api/dashboard?${params()}`);
    state.data = data;
    renderDashboard(data);
  } catch (error) { showNotice('#notice', `读取失败：${error.message}`, true); }
  finally { document.body.classList.remove('loading'); }
}

function renderDashboard(data) {
  const summary = data.summary;
  const windowLabel = state.window === 168 ? '最近7天' : `最近${state.window}小时`;
  $('#freshness').textContent = summary.latest_post ? `窗口内最新已发布观点 ${formatTime(summary.latest_post)}` : `${windowLabel}及当前筛选下暂无已发布观点`;
  renderKpis(summary);
  renderTrend(data.trend);
  renderQuality(data.quality);
  renderBrokers(data.sources);
  renderRanking(data.hotness);
  const earliest = summary.coverage?.earliest;
  const latest = summary.coverage?.latest;
  const coverageHours = earliest && latest ? (new Date(latest) - new Date(earliest)) / 3600000 : 0;
  if (!summary.posts) showNotice('#notice', `${windowLabel}及当前筛选条件下没有正式发布观点；不代表历史库为空。原文和待审核内容请到“入库管理”查看。`);
  else if (state.window === 168 && coverageHours < 160) showNotice('#notice', `当前原文覆盖约 ${Math.round(coverageHours)} 小时，7天窗口仍在建立。`);
  else showNotice('#notice', '');
}

function renderKpis(summary) {
  const directional = summary.bullish + summary.bearish;
  const cards = [
    ['已发布小作文', formatNumber(summary.posts), `${state.window === 168 ? '7天' : `${state.window}小时`}内有明确观点的主题`],
    ['标的', formatNumber(summary.securities), 'Tushare标准代码去重'],
    ['原子观点', formatNumber(summary.mentions), '一条观点对应一个标的和方向'],
    ['看多占比', directional ? formatPercent(summary.bullish / directional) : '-', `${summary.bullish} 多 / ${summary.bearish} 空`],
    ['署名覆盖', formatPercent(summary.source_coverage), `${summary.sourced_posts} 篇有明确券商署名`],
  ];
  const grid = $('#kpiGrid'); grid.replaceChildren();
  for (const [label, value, note] of cards) {
    const card = document.createElement('article'); card.className = 'kpi-card';
    card.innerHTML = '<div class="kpi-label"></div><div class="kpi-value"></div><div class="kpi-note"></div>';
    card.children[0].textContent = label; card.children[1].textContent = value; card.children[2].textContent = note; grid.append(card);
  }
}

function renderTrend(rows) {
  const chart = $('#trendChart'); chart.replaceChildren();
  const max = Math.max(1, ...rows.map((row) => Number(row.mentions)));
  for (const row of rows) {
    const column = document.createElement('div'); column.className = 'trend-column';
    column.dataset.tooltip = `${row.bucket} · ${row.mentions}次（多${row.bullish}/空${row.bearish}）`;
    const stack = document.createElement('div'); stack.className = 'trend-stack'; stack.style.height = `${Math.max(2, Number(row.mentions) / max * 188)}px`;
    for (const [name, value] of [['bullish', row.bullish], ['bearish', row.bearish]]) {
      const segment = document.createElement('i'); segment.className = `trend-segment bar-${name}`; segment.style.height = `${Number(value) / Number(row.mentions || 1) * 100}%`; stack.append(segment);
    }
    const label = document.createElement('span'); label.className = 'trend-label'; label.textContent = state.window === 168 ? row.bucket : row.bucket.slice(6);
    column.append(stack, label); chart.append(column);
  }
}

function renderQuality(quality) {
  const badge = $('#qualityBadge');
  badge.textContent = ({ pass: '代码完整', warning: '代码缺失', unknown: '待发布' })[quality.status] || quality.status;
  badge.className = `quality-badge ${quality.status === 'pass' ? '' : quality.status}`;
  const rates = quality.rates || {};
  const content = $('#qualityContent'); content.replaceChildren();
  const score = document.createElement('div'); score.className = 'quality-score'; score.textContent = rates.mention_coverage == null ? '暂无' : formatPercent(rates.mention_coverage); const scope = document.createElement('small'); scope.textContent = ' 累计已发布观点代码完整率'; score.append(scope);
  const rows = document.createElement('div'); rows.className = 'quality-bars';
  for (const [label, value] of [['证券代码解析', rates.mention_coverage || 0], ['明确券商署名', rates.source_coverage || 0]]) {
    const row = document.createElement('div'); row.className = 'quality-row'; row.innerHTML = '<div class="quality-label"><span></span><b></b></div><div class="meter"><i></i></div>';
    row.querySelector('span').textContent = label; row.querySelector('b').textContent = formatPercent(value); row.querySelector('i').style.width = formatPercent(value); rows.append(row);
  }
  const caveat = document.createElement('p'); caveat.className = 'automation-hint'; caveat.textContent = '统计累计全部已发布记录，不随上方时间窗口或筛选变化；仅衡量字段完整性，不是识别准确率或召回率。';
  content.append(score, rows, caveat);
}

function renderBrokers(rows) {
  const select = $('#brokerSelect'); const current = select.value; select.replaceChildren(new Option('全部券商', ''));
  for (const row of rows) select.append(new Option(`${row.broker}（${row.posts}）`, row.broker));
  select.value = rows.some((row) => row.broker === current) ? current : '';
}

function renderRanking(rows) {
  const body = $('#rankingBody'); body.replaceChildren(); $('#emptyState').classList.toggle('hidden', rows.length > 0);
  const max = Math.max(1, ...rows.map((row) => Number(row.mentions)));
  rows.forEach((row, index) => {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td class="rank"></td><td><div class="security-name"></div><div class="security-code"></div></td><td class="heat-cell"><div class="heat-track"><i></i></div></td><td class="number"></td><td class="number bullish"></td><td class="number bearish"></td><td><div class="time-cell"></div></td><td><button class="detail-button" aria-label="查看标的详情">→</button></td>';
    tr.children[0].textContent = String(index + 1).padStart(2, '0'); tr.querySelector('.security-name').textContent = row.name; tr.querySelector('.security-code').textContent = row.ts_code || `${row.code}.${row.market}`;
    tr.querySelector('.heat-track i').style.width = `${Number(row.mentions) / max * 100}%`; tr.children[3].textContent = row.mentions; tr.children[4].textContent = row.bullish; tr.children[5].textContent = row.bearish;
    tr.querySelector('.time-cell').textContent = formatTime(row.latest_mention); tr.querySelector('button').addEventListener('click', () => openSecurity(row)); body.append(tr);
  });
}

async function openSecurity(row, allHistory = false) {
  openDrawer('正在读取标的详情...');
  try {
    const data = await api(`/api/security?window=${state.window}&code=${encodeURIComponent(row.code)}&market=${encodeURIComponent(row.market)}${allHistory ? '&scope=all' : ''}`);
    const content = $('#drawerContent'); content.replaceChildren();
    const title = document.createElement('h2'); title.className = 'drawer-title'; title.textContent = data.name;
    const subtitle = document.createElement('div'); subtitle.className = 'drawer-subtitle'; subtitle.textContent = `${data.ts_code} · ${allHistory ? '历史累计' : `${state.window}小时内`} ${data.total_mentions} 篇，${data.total_viewpoints} 条观点`;
    content.append(title, subtitle);
    for (const mention of data.viewpoint_mentions) {
      const card = document.createElement('article'); card.className = 'mention-card';
      const meta = document.createElement('div'); meta.className = 'mention-meta'; meta.innerHTML = '<span></span><span></span>';
      meta.children[0].textContent = `${formatTime(mention.published_at)} · ${mention.source_broker || '未署名'} / ${mention.source_team || '其他'}`; meta.children[1].textContent = directionLabel(mention.direction);
      const heading = document.createElement('h4'); heading.textContent = mention.viewpoint_summary; const evidence = document.createElement('p'); evidence.className = 'mention-evidence'; evidence.textContent = mention.evidence;
      card.append(meta, heading, evidence);
      if (mention.target_price != null || mention.target_price_raw) { const target = document.createElement('div'); target.className = 'target-price'; target.textContent = `目标价 ${mention.target_price_raw || `${mention.target_price} ${mention.target_currency || ''}`}`; card.append(target); }
      const button = document.createElement('button'); button.className = 'open-topic'; button.textContent = '查看小作文原文 →'; button.addEventListener('click', () => openTopic(mention.topic_id, row, allHistory)); card.append(button); content.append(card);
    }
  } catch (error) { $('#drawerContent').textContent = `读取失败：${error.message}`; }
}

async function openTopic(topicId, securityRow, allHistory = false) {
  const content = $('#drawerContent'); content.textContent = '正在读取原文...';
  try {
    const topic = await api(`/api/topic/${encodeURIComponent(topicId)}`); content.replaceChildren();
    const back = document.createElement('button'); back.className = 'topic-back'; back.textContent = `← 返回${securityRow.name}`; back.addEventListener('click', () => openSecurity(securityRow, allHistory));
    const title = document.createElement('h2'); title.className = 'drawer-title'; title.textContent = topic.title || '无标题';
    const subtitle = document.createElement('div'); subtitle.className = 'drawer-subtitle'; subtitle.textContent = `${formatTime(topic.published_at)} · ${topic.source_broker || '未署名'} / ${topic.source_team || '其他'}`;
    const body = document.createElement('div'); body.className = 'topic-content'; body.textContent = topic.content; content.append(back, title, subtitle, body);
  } catch (error) { content.textContent = `读取失败：${error.message}`; }
}

async function searchHistory() {
  const container = $('#historyResults');
  if (!state.historyQuery) { container.innerHTML = '<div class="history-prompt">输入标的名称或代码开始检索。</div>'; return; }
  container.innerHTML = '<div class="history-prompt">正在检索已发布观点...</div>';
  try {
    const rows = await api(`/api/history/search?q=${encodeURIComponent(state.historyQuery)}`); container.replaceChildren();
    if (!rows.length) { container.innerHTML = '<div class="history-prompt">没有找到匹配标的。</div>'; return; }
    for (const row of rows) {
      const card = document.createElement('article'); card.className = 'history-card';
      card.innerHTML = '<div><div class="security-name"></div><div class="security-code"></div></div><div class="history-stat"><b></b><span>累计提及</span></div><div class="history-directions"></div><div class="history-dates"></div><button class="history-detail">查看明细 →</button>';
      card.querySelector('.security-name').textContent = row.name; card.querySelector('.security-code').textContent = row.ts_code || `${row.code}.${row.market}`; card.querySelector('.history-stat b').textContent = formatNumber(row.mentions);
      card.querySelector('.history-directions').textContent = `看多 ${row.bullish} · 看空 ${row.bearish}`; card.querySelector('.history-dates').textContent = `${formatTime(row.first_mention)} - ${formatTime(row.latest_mention)}`;
      card.querySelector('button').addEventListener('click', () => openSecurity(row, true)); container.append(card);
    }
  } catch (error) { container.innerHTML = `<div class="history-prompt">检索失败：${error.message}</div>`; }
}

function setView(view) {
  state.view = view; $('#dashboardView').classList.toggle('hidden', view !== 'dashboard'); $('#workflowView').classList.toggle('hidden', view !== 'workflow');
  $$('.view-button').forEach((button) => button.classList.toggle('active', button.dataset.view === view));
  if (view === 'workflow') loadWorkflow(); else { if (state.pollTimer) { clearInterval(state.pollTimer); state.pollTimer = null; } loadDashboard(); }
}

async function loadWorkflow() {
  try {
    const [data, automation] = await Promise.all([api('/api/workflow'), api('/api/automation/status')]); renderWorkflow(data); renderAutomation(automation);
    // Also observe processes started from the terminal, including their final
    // pause/completion transition. This polls local status only, never models.
    if (state.view === 'workflow' && !state.pollTimer) state.pollTimer = setInterval(loadWorkflow, 5000);
  } catch (error) { showNotice('#workflowNotice', `工作流状态读取失败：${error.message}`, true); }
}

function renderWorkflow(data) {
  const counts = [
    ['原文主题', data.counts.raw], ['可处理文本', data.counts.eligible], ['附件跳过', data.counts.excluded], ['已分类主题', data.counts.classified],
    ['待复核观点', data.counts.candidate], ['已接受未发布', data.counts.reviewed], ['正式发布观点', data.counts.published], ['待解析代码', data.counts.unresolved],
  ];
  const container = $('#workflowCounts'); container.replaceChildren();
  for (const [label, value] of counts) { const item = document.createElement('div'); item.className = 'workflow-count'; item.innerHTML = '<span></span><b></b>'; item.children[0].textContent = label; item.children[1].textContent = formatNumber(value); container.append(item); }
  const runtime = data.runtime; $('#runtimeState').textContent = runtime.status === 'running' ? `正在${runtime.action}` : runtime.status === 'failed' ? `失败：${runtime.error || ''}` : statusLabel(runtime.status);
  $$('.workflow-actions button').forEach((button) => { button.disabled = runtime.status === 'running'; });
  if (runtime.status === 'failed') showNotice('#workflowNotice', `上次运行失败：${runtime.error || '请查看运行记录'}`, true);
  // Keep edit/publish feedback visible; batch status has its own persistent panel.
  renderRunHistory(data.runs, '#runHistory'); renderModelHistory(data.model_runs);
  if ($('#reviewList').dataset.loaded !== 'true') loadReview();
}

function renderAutomation(data) {
  state.automation = data;
  renderAutomationCalls(data.recent_calls || []);
  const batch = data.latest_batch;
  const running = data.runtime.status === 'running' || data.batches.some((item) => item.status === 'running' && item.worker_alive !== false);
  const resumable = data.batches.find((item) => ['paused', 'failed'].includes(item.status) || (item.status === 'running' && item.worker_alive === false));
  $('#runAutomationButton').disabled = running;
  $('#resumeAutomationButton').disabled = running || !resumable;
  $('#resumeAutomationButton').textContent = resumable ? `继续批次 ${resumable.id}` : '没有待续跑批次';
  $('#stopAutomationButton').disabled = !data.batches.some((item) => item.status === 'running' && item.worker_alive !== false && !item.stop_requested);
  $('#publishButton').disabled = running;
  $$('.workflow-actions button').forEach((button) => { button.disabled = running; });
  if (running && $('#workflowNotice').textContent.startsWith('上次运行失败')) showNotice('#workflowNotice', '');
  const batchSelect = $('#publishBatchSelect'); const selectedBatch = batchSelect.value;
  batchSelect.replaceChildren(...data.batches.map((item) => new Option(`批次 ${item.id} · ${statusLabel(item.status)} · 待发布 ${item.reviewed_viewpoints || 0} 条`, String(item.id))));
  if (!data.batches.length) batchSelect.append(new Option('暂无批次', ''));
  else if (data.batches.some((item) => String(item.id) === selectedBatch)) batchSelect.value = selectedBatch;
  renderBatchPublish();
  const coverage = `原文最新：${formatTime(data.coverage?.latest)} · 最近成功抓取：${formatTime(data.coverage?.last_fetch_at)} · 最近7天待处理 ${formatNumber(data.pending_recent)} 篇 · 未解决疑点 ${formatNumber(data.remaining_issues)} 项`;
  if (!batch) { $('#automationProgress').textContent = `${coverage}\n尚无增量批次。点击“一键增量入库”开始；不会自动发布。`; return; }
  const counts = batch.counts || {};
  const stage = stageLabel(batch.stage);
  const batchStatus = batch.status === 'running' && batch.worker_alive === false ? '进程已退出，可续跑' : statusLabel(batch.status);
  $('#runtimeState').textContent = `批次 ${batch.id} · ${batchStatus}`;
  $('#automationProgress').textContent = `${coverage}\n批次 ${batch.id} · ${batchStatus} · ${stage}${batch.stop_requested ? ' · 已请求安全暂停' : ''}\n窗口 ${formatTime(batch.window_start)} — ${formatTime(batch.window_end)} · 已抓取 ${batch.fetch_pages || 0} 页 · 本批次 ${batch.selected_count} 篇\n待处理 ${counts.queued || 0} · 已识别 ${counts.extracted || 0} · 接受 ${counts.accepted || 0} · 排除 ${counts.excluded || 0} · 存疑 ${counts.uncertain || 0} · 失败 ${counts.failed || 0}\n模型调用 ${batch.calls_used} / ${batch.max_calls} 次 · 输入 ${formatNumber(batch.input_chars_used)} / ${formatNumber(batch.max_input_chars)} 字符 · 累计 ${Math.round((batch.elapsed_ms || 0) / 60000)} / ${batch.max_minutes} 分钟${batch.error && !running ? `\n暂停或错误原因：${batch.error}` : ''}\n${data.cost_note}`;
}

function renderAutomationCalls(rows) {
  const container = $('#automationCallHistory'); container.replaceChildren();
  if (!rows.length) { container.textContent = '本批次暂无模型调用；缓存复用不产生新调用。'; return; }
  for (const row of rows) {
    const item = document.createElement('div'); item.className = 'log-row';
    const detail = document.createElement('div'); const heading = document.createElement('strong');
    heading.textContent = `调用 ${row.id} · ${stageLabel(row.stage)} · ${row.actual_model || row.requested_model}`;
    const timing = document.createElement('span'); timing.textContent = `${formatTime(row.started_at)} · 输入 ${formatNumber(row.input_chars)} 字符 · ${row.duration_ms == null ? '尚未结束' : `${Math.round(row.duration_ms / 1000)}秒`}`;
    detail.append(heading, timing);
    if (row.error) { const error = document.createElement('p'); error.className = 'automation-hint'; error.textContent = `失败详情：${row.error}`; detail.append(error); }
    const status = document.createElement('b'); status.className = `status-text ${row.status}`; status.textContent = row.status === 'interrupted' ? '已中断' : statusLabel(row.status);
    item.append(detail, status); container.append(item);
  }
}

function renderBatchPublish() {
  const data = state.automation;
  const batch = data?.batches.find((item) => String(item.id) === $('#publishBatchSelect').value);
  const count = batch?.reviewed_viewpoints || 0;
  const running = data?.runtime.status === 'running' || data?.batches.some((item) => item.status === 'running');
  $('#publishBatchButton').disabled = !batch || running || !['completed', 'completed_with_issues'].includes(batch.status) || !count;
  $('#publishBatchButton').textContent = `发布本批次已通过观点（${count}条）`;
  $('#publishBatchSummary').textContent = batch ? `仅包含批次 ${batch.id} 的已接受记录；已发布 ${batch.published_viewpoints || 0} 条。全部通过发布前检查才会发布，不跳过无效记录。` : '选择已完成批次；不混入旧试跑记录。';
}

async function publishSelectedBatch() {
  const batch = state.automation?.batches.find((item) => String(item.id) === $('#publishBatchSelect').value);
  if (!batch || !batch.reviewed_viewpoints) return;
  if (!window.confirm(`确认检查并发布批次 ${batch.id} 的 ${batch.reviewed_viewpoints} 条已接受观点？\n仅发布这个批次，不包含旧试跑或其他批次；任何一条检查失败则整批不发布。`)) return;
  try {
    const result = await api('/api/workflow/publish', jsonOptions('POST', { batch_id: batch.id, expected_count: batch.reviewed_viewpoints }));
    showNotice('#workflowNotice', `批次 ${batch.id} 已发布 ${result.published} 条观点，发布前备份已保留。`); await loadWorkflow(); await loadReview();
  } catch (error) { showNotice('#workflowNotice', `本批次未发布：${error.message}`, true); }
}

async function runAutomation(resume = false) {
  try {
    const body = resume ? {} : { limit: Number($('#budgetLimit').value), max_calls: Number($('#budgetCalls').value), max_input_chars: Number($('#budgetChars').value), max_minutes: Number($('#budgetMinutes').value), skip_fetch: $('#skipFetch').checked };
    if (resume) body.resume = state.automation?.batches.find((item) => ['paused', 'failed'].includes(item.status) || (item.status === 'running' && item.worker_alive === false))?.id;
    if (resume && !body.resume) throw new Error('没有可续跑批次');
    await api('/api/automation/run', jsonOptions('POST', body));
    showNotice('#workflowNotice', resume ? '已请求续跑；沿用原批次累计预算。' : '增量流程已启动；正式发布仍需你确认。');
    await loadWorkflow();
  } catch (error) { showNotice('#workflowNotice', `无法启动：${error.message}`, true); }
}

async function stopAutomation() {
  const batch = state.automation?.batches.find((item) => item.status === 'running');
  if (!batch) return;
  try { await api('/api/automation/stop', jsonOptions('POST', { batch_id: batch.id })); showNotice('#workflowNotice', '已请求安全暂停；当前操作结束后保存进度。'); await loadWorkflow(); }
  catch (error) { showNotice('#workflowNotice', error.message, true); }
}

function renderRunHistory(rows, selector) {
  const container = $(selector); container.replaceChildren();
  if (!rows.length) { container.textContent = '暂无运行记录'; return; }
  for (const row of rows) { const item = document.createElement('div'); item.className = 'log-row'; item.innerHTML = '<div><strong></strong><span></span></div><b></b>'; item.querySelector('strong').textContent = row.command; item.querySelector('span').textContent = `${formatTime(row.started_at)} · 成功 ${row.success_count} · 失败 ${row.failed_count}`; item.querySelector('b').textContent = statusLabel(row.status); item.querySelector('b').className = `status-text ${row.status}`; container.append(item); }
}

function renderModelHistory(rows) {
  const container = $('#modelHistory'); container.replaceChildren();
  if (!rows.length) { container.textContent = '暂无模型调用'; return; }
  for (const row of rows) { const item = document.createElement('div'); item.className = 'log-row'; item.innerHTML = '<div><strong></strong><span></span></div><b></b>'; item.querySelector('strong').textContent = `${stageLabel(row.stage)} · ${row.model_id}`; item.querySelector('span').textContent = `${row.success_count}/${row.input_count} 成功 · ${Math.round(row.total_duration_ms / 1000)}秒`; item.querySelector('b').textContent = statusLabel(row.status); item.querySelector('b').className = `status-text ${row.status}`; container.append(item); }
}

async function runWorkflow(action) {
  try { await api('/api/workflow/run', jsonOptions('POST', { action })); await loadWorkflow(); }
  catch (error) { showNotice('#workflowNotice', `无法启动：${error.message}`, true); }
}

async function loadReview() {
  const container = $('#reviewList'); container.innerHTML = '<div class="history-prompt">正在读取原文与观点...</div>';
  try {
    const result = await api(`/api/review?status=${encodeURIComponent($('#reviewStatus').value)}&page=${state.reviewPage}&page_size=25`); const rows = result.items; container.replaceChildren(); container.dataset.loaded = 'true';
    state.reviewPage = result.page; state.reviewPages = result.total_pages;
    $('#reviewPageSummary').textContent = `共 ${formatNumber(result.total)} 篇 · 第 ${result.page} / ${result.total_pages} 页 · 每页 ${result.page_size} 篇`;
    $('#reviewPrevious').disabled = result.page <= 1; $('#reviewNext').disabled = result.page >= result.total_pages;
    if (!rows.length) { container.innerHTML = '<div class="history-prompt">当前状态下没有待复核内容。</div>'; return; }
    for (const topic of rows) container.append(renderReviewTopic(topic));
  } catch (error) { container.innerHTML = `<div class="history-prompt">读取失败：${error.message}</div>`; }
}

function renderReviewTopic(topic) {
  const article = document.createElement('article'); article.className = 'review-topic';
  const header = document.createElement('div'); header.className = 'review-topic-header';
  const heading = document.createElement('div'); const title = document.createElement('h4'); title.textContent = `${topic.sample_order ? `样本 ${topic.sample_order} · ` : ''}${topic.title || '无标题'}`;
  const meta = document.createElement('p'); meta.textContent = `${formatTime(topic.published_at)} · ${topic.eligibility === 'excluded' ? `原文保留，跳过处理：${topic.exclusion_reason || '附件内容'}` : classificationLabel(topic.classification)} · 主题 ${topic.topic_id}`; heading.append(title, meta);
  const select = document.createElement('select'); for (const value of ['broker_note', 'uncertain', 'not_broker_note']) select.append(new Option(classificationLabel(value), value)); select.value = topic.classification || 'uncertain';
  select.disabled = !topic.classification_id || topic.viewpoints.some((viewpoint) => viewpoint.review_status === 'published');
  select.addEventListener('change', async () => { try { await api(`/api/review/classification/${topic.classification_id}`, jsonOptions('PATCH', { classification: select.value })); showNotice('#workflowNotice', '分类修订已保存，相关旧审批已失效。'); await loadReview(); } catch (error) { showNotice('#workflowNotice', error.message, true); } });
  header.append(heading, select);
  const approvalReasonLabels = {
    classification_disagreement: '首轮与二审分类不一致', uncertain_classification: '存在不确定分类',
    low_classification_confidence: '分类置信度不足', low_overall_confidence: '二审整体置信度不足',
    reviewer_risk_flags: '二审标记了风险', viewpoint_set_disagreement: '标的或方向集合不一致',
    viewpoint_gate_failed: '标的未解析或观点置信度不足', target_disagreement: '目标价或币种不一致',
    recommendation_source_disagreement: '券商或研究组不一致'
  };
  let approvalReasons = [];
  try { approvalReasons = JSON.parse(topic.ai_approval_reasons_json || '[]'); } catch {}
  const approval = document.createElement('div');
  const approvalState = topic.ai_approval_status === 'failed' ? 'failed' : topic.ai_approval_decision === 'auto_accept' ? 'auto-accept' : 'human-review';
  approval.className = `ai-review-note ${approvalState}`;
  const approvalConfidence = topic.ai_approval_confidence == null ? '' : ` · 二审置信度 ${Math.round(Number(topic.ai_approval_confidence) * 100)}%`;
  if (topic.issues?.length) approval.textContent = `待裁决事项：${topic.issues.map((issue) => issue.reason).join('；')}`;
  else if (topic.viewpoints.some((viewpoint) => viewpoint.approval)) approval.textContent = `当前版本审批：本页展示的 ${topic.viewpoints.length} 条观点中，${topic.viewpoints.filter((viewpoint) => viewpoint.approval_valid).length} 条有效接受；具体裁决和理由见各条观点。`;
  else if (topic.ai_approval_status === 'failed') approval.textContent = `历史AI二审失败，可在增量流程中重试：${topic.ai_approval_error || '未返回有效结果'}`;
  else if (topic.ai_approval_decision === 'auto_accept') approval.textContent = `AI自动通过${approvalConfidence} · 已进入“已接受”，尚未发布`;
  else if (topic.ai_approval_decision === 'human_review') approval.textContent = `AI转人工${approvalConfidence} · 原因：${approvalReasons.map((reason) => approvalReasonLabels[reason] || reason).join('、') || '未满足自动通过门槛'}`;
  else approval.textContent = $('#reviewStatus').value === 'raw' ? '原文层保留全部内容，不代表已识别或已发布。' : '审核以每条观点的当前版本为准；历史二审结果不能代替有效审批。';
  const original = document.createElement('details'); original.className = 'original-text'; original.open = true;
  const summary = document.createElement('summary'); summary.textContent = '小作文原文全文（点击收起）';
  const classificationEvidence = document.createElement('div'); classificationEvidence.className = 'classification-evidence';
  classificationEvidence.textContent = `分类依据：${topic.classification_evidence || '暂无'}`;
  const text = document.createElement('pre'); text.textContent = `标题：${topic.title || '无标题'}\n\n正文：\n${topic.content || '（正文为空）'}`;
  original.append(summary, classificationEvidence, text);
  const viewpoints = document.createElement('div'); viewpoints.className = 'review-viewpoints';
  if (!topic.viewpoints.length) { const empty = document.createElement('div'); empty.className = 'review-empty'; empty.textContent = $('#reviewStatus').value === 'raw' ? '当前是原文视图，不展示结构化结果。' : topic.issues?.length || topic.ai_approval_decision === 'human_review' ? '当前筛选下没有观点明细，但本篇仍有待核对事项。请对照原文判断是否存在遗漏或争议；这不表示已通过审核。' : topic.ai_approval_status === 'failed' ? 'AI二审没有成功完成，尚不能确定是否存在个股观点。' : topic.classification === 'not_broker_note' ? '当前分类为非小作文；原文仍然保留。' : '当前筛选下没有“具体标的 + 明确方向”的观点记录。'; viewpoints.append(empty); }
  for (const viewpoint of topic.viewpoints) viewpoints.append(renderViewpointForm(viewpoint));
  if (topic.issues?.length) {
    const resolution = document.createElement('form'); resolution.className = 'viewpoint-form';
    const note = field('人工核对说明（必填；关闭整篇疑点不会新增或接受观点）', '', 'note');
    const button = document.createElement('button'); button.type = 'submit'; button.className = 'primary-action'; button.textContent = '已核对，无需补充个股观点';
    button.disabled = topic.viewpoints.some((viewpoint) => viewpoint.review_status === 'candidate');
    resolution.append(note, button);
    resolution.addEventListener('submit', async (event) => { event.preventDefault(); try {
      const result = await api(`/api/review/topic/${encodeURIComponent(topic.topic_id)}/resolve`, jsonOptions('POST', Object.fromEntries(new FormData(resolution))));
      showNotice('#workflowNotice', `已记录人工说明，关闭 ${result.resolved} 项疑点；批次历史保持不变。`); await loadReview(); await loadWorkflow();
    } catch (error) { showNotice('#workflowNotice', error.message, true); } });
    viewpoints.append(resolution);
  }
  article.append(header, approval, original, viewpoints); return article;
}

function field(label, value, name, type = 'text') {
  const wrapper = document.createElement('label'); wrapper.className = 'review-field'; const span = document.createElement('span'); span.textContent = label;
  const input = document.createElement('input'); input.type = type; input.name = name; input.value = value ?? ''; wrapper.append(span, input); return wrapper;
}

function renderViewpointForm(viewpoint) {
  const securityLinks = Array.isArray(viewpoint.security_links) ? viewpoint.security_links : [];
  const resolvedCodes = securityLinks.filter((link) => link.resolution_status === 'resolved' && link.ts_code).map((link) => link.ts_code);
  const form = document.createElement('form'); form.className = 'viewpoint-form'; form.dataset.id = viewpoint.id;
  const version = document.createElement('input'); version.type = 'hidden'; version.name = 'review_version'; version.value = viewpoint.review_version; form.append(version);
  const top = document.createElement('div'); top.className = 'viewpoint-form-top'; const title = document.createElement('strong'); title.textContent = viewpoint.summary;
  const badge = document.createElement('span'); badge.className = `status-chip ${viewpoint.review_status}`; badge.textContent = statusLabel(viewpoint.review_status); top.append(title, badge);
  const grid = document.createElement('div'); grid.className = 'review-field-grid';
  const summary = field('观点摘要', viewpoint.summary, 'summary'); summary.classList.add('wide');
  const direction = document.createElement('label'); direction.className = 'review-field'; direction.innerHTML = '<span>方向</span>'; const directionSelect = document.createElement('select'); directionSelect.name = 'direction'; directionSelect.append(new Option('看多', 'bullish'), new Option('看空', 'bearish')); directionSelect.value = viewpoint.direction; direction.append(directionSelect);
  grid.append(summary, direction, field('标准代码（多个用逗号分隔）', resolvedCodes.join(', '), 'ts_codes'), field('券商', viewpoint.recommendation_broker || '', 'recommendation_broker'),
    field('研究组', viewpoint.recommendation_team || '其他', 'recommendation_team'), field('目标价', viewpoint.target_price ?? '', 'target_price', 'number'),
    field('币种', viewpoint.target_currency || '', 'target_currency'), field('目标价原文', viewpoint.target_price_raw || '', 'target_price_raw'),
    field('目标价证据（原文精确片段）', viewpoint.target_price_evidence || '', 'target_price_evidence'), field('署名证据（原文精确片段）', viewpoint.recommendation_evidence || '', 'recommendation_evidence'));
  let supportingEvidence = [];
  try { supportingEvidence = JSON.parse(viewpoint.supporting_evidence_json || '[]'); } catch {}
  if (!supportingEvidence.length && viewpoint.evidence) supportingEvidence = [viewpoint.evidence];
  const codeText = resolvedCodes.length ? securityLinks.filter((link) => link.ts_code).map((link) => `${link.name} ${link.ts_code}`).join(' / ') : '代码待解析';
  const evidence = document.createElement('blockquote'); evidence.textContent = `合并依据：\n${supportingEvidence.map((item, index) => `${index + 1}. ${item}`).join('\n')}\n\n标的证据：${viewpoint.surface} · ${codeText}`;
  const viewpointReasonLabels = {
    classification_not_confirmed: '两次模型未同时确认是小作文', low_classification_confidence: '小作文分类置信度不足',
    low_overall_confidence: 'AI二审整体置信度不足', reviewer_risk_flags: 'AI二审标记了风险',
    recommendation_source_disagreement: '券商或研究组不一致', current_viewpoint_unresolved: '首轮代码未解析',
    current_viewpoint_low_confidence: '首轮观点置信度不足', reviewer_missing_viewpoint: '二审未确认该观点',
    reviewer_viewpoint_unresolved: '二审代码未解析', reviewer_viewpoint_low_confidence: '二审观点置信度不足',
    target_disagreement: '目标价或币种不一致'
  };
  let viewpointReasons = [];
  try { viewpointReasons = JSON.parse(viewpoint.ai_viewpoint_reasons_json || '[]'); } catch {}
  const aiNote = document.createElement('div');
  aiNote.className = `viewpoint-ai-note ${viewpoint.ai_viewpoint_decision === 'auto_accept' ? 'auto-accept' : 'human-review'}`;
  const viewpointConfidence = viewpoint.ai_viewpoint_confidence == null ? '' : ` · 一致置信度 ${Math.round(Number(viewpoint.ai_viewpoint_confidence) * 100)}%`;
  aiNote.textContent = viewpoint.approval ? `当前审批：${viewpoint.approval_valid ? '有效接受' : viewpoint.approval.decision === 'invalidated' ? '编辑后失效，需重新接受' : viewpoint.approval.decision === 'exclude' ? '已排除' : '待裁决'} · ${viewpoint.approval.method === 'human' ? '人工' : viewpoint.approval.method === 'ai' ? 'AI' : '系统'} · ${viewpoint.approval.reason}` : viewpoint.ai_viewpoint_decision === 'auto_accept'
    ? `该观点已由AI自动接受${viewpointConfidence}`
    : `该观点未自动接受${viewpointConfidence} · ${viewpointReasons.map((reason) => viewpointReasonLabels[reason] || reason).join('、') || '等待人工确认'}`;
  const actions = document.createElement('div'); actions.className = 'review-actions';
  const acceptLabel = viewpoint.review_status === 'rejected' ? '修改并接受' : '接受';
  for (const [label, action, className] of [['保存修改', 'save', ''], [acceptLabel, 'accept', 'accept'], ['驳回', 'reject', 'reject']]) { const button = document.createElement('button'); button.type = 'button'; button.textContent = label; button.className = className; button.addEventListener('click', () => handleViewpointAction(form, action)); actions.append(button); }
  form.append(top);
  if (viewpoint.ai_viewpoint_decision) form.append(aiNote);
  if (viewpoint.review_status === 'published') { [...grid.querySelectorAll('input,select')].forEach((input) => { input.disabled = true; }); actions.replaceChildren(); const text = document.createElement('span'); text.textContent = '正式发布记录只读，避免直接覆盖已发布历史。'; actions.append(text); }
  form.append(grid, evidence, actions); return form;
}

async function handleViewpointAction(form, action) {
  const id = form.dataset.id;
  try {
    if (action === 'save') { const body = Object.fromEntries(new FormData(form)); await api(`/api/review/viewpoint/${id}`, jsonOptions('PATCH', body)); }
    else if (action === 'accept') { const body = { ...Object.fromEntries(new FormData(form)), accept: true }; await api(`/api/review/viewpoint/${id}`, jsonOptions('PATCH', body)); }
    else await api(`/api/review/viewpoint/${id}/${action}`, jsonOptions('POST', {}));
    showNotice('#workflowNotice', action === 'accept' ? '修改已保存，观点已接受并进入“已接受”。' : action === 'reject' ? '观点已驳回。' : '修改已保存。'); await loadWorkflow(); await loadReview();
  } catch (error) { showNotice('#workflowNotice', `操作失败：${error.message}`, true); }
}

async function publishReviewed() {
  if (!window.confirm('确认发布所有已接受观点？发布后它们会进入热度看板。')) return;
  try { const result = await api('/api/workflow/publish', jsonOptions('POST', {})); showNotice('#workflowNotice', `已发布 ${result.published} 条观点。`); await loadWorkflow(); await loadReview(); }
  catch (error) { showNotice('#workflowNotice', `发布失败：${error.message}`, true); }
}

function openDrawer(initial = '') { $('#drawerContent').textContent = initial; $('#drawer').classList.add('open'); $('#drawer').setAttribute('aria-hidden', 'false'); $('#drawerBackdrop').classList.remove('hidden'); }
function closeDrawer() { $('#drawer').classList.remove('open'); $('#drawer').setAttribute('aria-hidden', 'true'); $('#drawerBackdrop').classList.add('hidden'); }

$$('.view-button').forEach((button) => button.addEventListener('click', () => setView(button.dataset.view)));
$$('.window-tab').forEach((button) => button.addEventListener('click', () => { state.window = Number(button.dataset.window); $$('.window-tab').forEach((item) => item.classList.toggle('active', item === button)); loadDashboard(); }));
$$('.workflow-actions button').forEach((button) => button.addEventListener('click', () => runWorkflow(button.dataset.action)));
$('#runAutomationButton').addEventListener('click', () => runAutomation(false)); $('#resumeAutomationButton').addEventListener('click', () => runAutomation(true)); $('#stopAutomationButton').addEventListener('click', stopAutomation);
$('#publishBatchSelect').addEventListener('change', renderBatchPublish); $('#publishBatchButton').addEventListener('click', publishSelectedBatch);
$('#reloadReview').addEventListener('click', loadReview); $('#reviewStatus').addEventListener('change', () => { state.reviewPage = 1; loadReview(); }); $('#publishButton').addEventListener('click', publishReviewed);
$('#reviewPrevious').addEventListener('click', () => { state.reviewPage--; loadReview(); }); $('#reviewNext').addEventListener('click', () => { state.reviewPage++; loadReview(); });
let searchTimer; $('#searchInput').addEventListener('input', (event) => { clearTimeout(searchTimer); searchTimer = setTimeout(() => { state.q = event.target.value.trim(); loadDashboard(); }, 260); });
let historyTimer; $('#historySearchInput').addEventListener('input', (event) => { clearTimeout(historyTimer); historyTimer = setTimeout(() => { state.historyQuery = event.target.value.trim(); searchHistory(); }, 260); });
$('#directionSelect').addEventListener('change', (event) => { state.direction = event.target.value; loadDashboard(); }); $('#brokerSelect').addEventListener('change', (event) => { state.broker = event.target.value; loadDashboard(); });
$('#drawerClose').addEventListener('click', closeDrawer); $('#drawerBackdrop').addEventListener('click', closeDrawer); document.addEventListener('keydown', (event) => { if (event.key === 'Escape') closeDrawer(); });

loadDashboard();
