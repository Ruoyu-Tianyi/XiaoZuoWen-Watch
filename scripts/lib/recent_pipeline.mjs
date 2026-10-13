import { runAutomatedPipeline } from './automated_pipeline.mjs';
import { dailyBudget, ensureAutomationSchema } from './automation_state.mjs';

// One invocation drains small closed-loop chunks; never waits for a daily reset.
// Reinvoking on another day resumes budget pauses without resetting paid usage.
export async function runRecentPipeline(db, options={}, dependencies={}) {
  ensureAutomationSchema(db);
  const end=options.windowEnd||new Date().toISOString();
  const cutoff=new Date(Date.parse(end)-168*3600000).toISOString();
  const pending=db.prepare(`SELECT * FROM workflow_batches WHERE status='paused'
    AND window_end>=? AND window_end<=? AND (selection_complete=0 OR EXISTS(
      SELECT 1 FROM workflow_batch_topics WHERE batch_id=workflow_batches.id AND status IN ('queued','extracted','failed')))
    ORDER BY id DESC LIMIT 1`).get(cutoff,end);
  if(pending && ((pending.pause_code && pending.pause_code!=='MODEL_BUDGET_EXCEEDED') ||
    (!pending.pause_code && pending.error && !/预算|上限|工作进程退出/.test(pending.error))))
    return {batch_id:pending.id,status:'paused',pause_code:pending.pause_code,
      error:'上批因非预算异常暂停，须检查后显式 --resume；不会自动重复付费',daily_budget:dailyBudget(db)};
  if(pending?.stop_requested)
    return {batch_id:pending.id,status:'paused',error:'已有人为停止请求，须显式 --resume',daily_budget:dailyBudget(db)};
  const daily=dailyBudget(db);
  if(daily.calls_remaining<2 || daily.input_chars_remaining<1)
    return {batch_id:pending?.id||null,status:'paused',pause_code:'MODEL_BUDGET_EXCEEDED',
      error:'每日预算不足，未新建付费批次；下一次触发再检查预算',daily_budget:daily};
  const calls=Math.min(Number(options.maxCalls??20),daily.calls_remaining);
  const chars=Math.min(Number(options.maxChars??300000),daily.input_chars_remaining);
  const minutes=Number(options.maxMinutes??60);
  if(!Number.isInteger(calls)||calls<2||!Number.isInteger(chars)||chars<1||!Number.isInteger(minutes)||minutes<1)
    throw new Error('recent 运行预算必须为整数，调用数至少为2');
  const runOptions=pending?{resume:pending.id,batchSize:options.batchSize??10,prepareOnly:!!options.prepareOnly,
    reviewChars:options.reviewChars??40000,compactReview:true,
    autoPublish:options.autoPublish,
    maxCalls:Number(pending.calls_used)+calls,maxChars:Number(pending.input_chars_used)+chars,
    maxMinutes:Math.ceil(Number(pending.elapsed_ms)/60000)+minutes}
    :{...options,windowEnd:end,hours:168,limit:options.limit??5000,batchSize:options.batchSize??10,
      maxCalls:calls,maxChars:chars,maxMinutes:minutes};
  return {...await runAutomatedPipeline(db,runOptions,dependencies),daily_budget:dailyBudget(db)};
}
