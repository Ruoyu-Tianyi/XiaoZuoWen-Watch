import { publishAccepted, currentApproval, validatePublishBatch, recordApproval } from './approval_guard.mjs';
import { now } from './automation_state.mjs';

export const AUTO_PUBLISH_POLICY='closed-topic-ai-publish-v1';
export const AUTO_PUBLISH_CONFIDENCE=0.95;

export function ensureAutoPublishSchema(db){
  db.exec(`CREATE TABLE IF NOT EXISTS workflow_auto_publish_events(
    id INTEGER PRIMARY KEY,batch_id INTEGER NOT NULL REFERENCES workflow_batches(id),
    policy_version TEXT NOT NULL,status TEXT NOT NULL,published_count INTEGER NOT NULL DEFAULT 0,
    viewpoint_ids_json TEXT NOT NULL,blocked_json TEXT NOT NULL,backup_path TEXT,publish_batch_id TEXT,
    error TEXT,created_at TEXT NOT NULL)`);
}

function eligibleReason(db,batchId,v,model){
  const approval=currentApproval(db,v.id);
  let score=null,review=null;
  try{score=JSON.parse(approval?.metadata_json||'{}').confidence;review=JSON.parse(v.review_json||'null');}catch{}
  if(!['accepted','uncertain'].includes(v.topic_status)||!v.primary_json||!review)return '本篇识别与复核尚未闭环';
  if(!approval||approval.method!=='ai'||approval.decision!=='accept'||approval.model_id!==model||review.classification!=='broker_note')return '不具备本批模型明确AI卖方观点审批';
  if(!Number.isFinite(score)||score<AUTO_PUBLISH_CONFIDENCE||score>1||!Number.isFinite(v.confidence)||v.confidence<AUTO_PUBLISH_CONFIDENCE||v.confidence>1)return '识别或审核置信度低于自动发布阈值';
  if(db.prepare("SELECT 1 FROM workflow_topic_issues WHERE batch_id=? AND topic_id=? AND status='pending' LIMIT 1").get(batchId,v.topic_id))return '本篇仍有主题级疑点';
  return null;
}

// Publish only this run's closed topics, never legacy reviewed rows or raw candidates.
export async function autoPublishReady(db,batchId,{backupDir='data/backups-v2'}={}){
  ensureAutoPublishSchema(db);
  const batch=db.prepare('SELECT * FROM workflow_batches WHERE id=?').get(batchId);
  if(!batch||!batch.fetch_complete||!batch.selection_complete)throw new Error('自动发布要求原文抓取及选题已完成');
  if(batch.lease_pid && batch.lease_pid!==process.pid){
    try{process.kill(batch.lease_pid,0);throw Object.assign(new Error('批次仍由其他进程处理，不能并发发布'),{code:'AUTO_PUBLISH_WORKER_ACTIVE'});}
    catch(e){if(e.code!=='ESRCH')throw e;}
  }
  const candidates=db.prepare(`SELECT v.*,bt.status topic_status,bt.primary_json,bt.review_json
    FROM viewpoints v JOIN workflow_batch_topics bt ON bt.topic_id=v.topic_id AND bt.extraction_result_id=v.extraction_result_id
    WHERE bt.batch_id=? AND v.review_status='reviewed' ORDER BY v.id`).all(batchId);
  const reviewModel=JSON.parse(batch.config_json).review_model.model;
  const blocked=[],ready=[];
  for(const v of candidates){
    const reason=eligibleReason(db,batchId,v,reviewModel);
    if(reason)blocked.push({viewpoint_id:v.id,reason});else ready.push(v.id);
  }
  const gate=validatePublishBatch(db,ready),invalid=new Set(gate.issues.map(i=>i.viewpoint_id));
  for(const issue of gate.issues)blocked.push({viewpoint_id:issue.viewpoint_id,reason:issue.message,code:issue.code});
  const ids=ready.filter(id=>!invalid.has(id));
  if(!candidates.length)return {published:0,blocked:[],policy:AUTO_PUBLISH_POLICY};
  const holdForHumans=()=>{
    for(const id of new Set(blocked.map(b=>b.viewpoint_id))){
      const current=currentApproval(db,id);
      if(current?.method!=='ai'||current.decision!=='accept')continue;
      if(db.prepare(`SELECT 1 FROM viewpoint_approval_audit WHERE viewpoint_id=? AND method='human' UNION ALL
        SELECT 1 FROM review_events WHERE viewpoint_id=? AND action IN ('accept','reject','edit','edit_viewpoint','edit_and_accept','manual_edit','manual_accept') LIMIT 1`).get(id,id))continue;
      if(db.prepare('SELECT review_status FROM viewpoints WHERE id=?').get(id)?.review_status!=='reviewed')continue;
      recordApproval(db,{viewpointId:id,decision:'uncertain',method:'ai',scopeDecision:'broker_note',modelId:reviewModel,
        reason:blocked.filter(b=>b.viewpoint_id===id).map(b=>b.reason).join('；'),metadata:{batch_id:batchId,auto_publish_policy:AUTO_PUBLISH_POLICY}});
      db.prepare("UPDATE viewpoints SET review_status='candidate',updated_at=? WHERE id=?").run(now(),id);
      db.prepare("UPDATE workflow_batch_topics SET status='uncertain',reasons_json=json_insert(reasons_json,'$[#]','自动发布未通过，已转人工复核'),updated_at=? WHERE batch_id=? AND topic_id=?")
        .run(now(),batchId,current.topic_id);
      db.prepare("UPDATE topic_processing_state SET status='uncertain',updated_at=? WHERE batch_id=? AND topic_id=?").run(now(),batchId,current.topic_id);
    }
  };
  let result;
  try{
    const save=published=>db.prepare(`INSERT INTO workflow_auto_publish_events(batch_id,policy_version,status,published_count,viewpoint_ids_json,blocked_json,backup_path,publish_batch_id,created_at)
      VALUES(?,?,?,?,?,?,?,?,?)`).run(batchId,AUTO_PUBLISH_POLICY,ids.length?'published':'blocked',published.published,JSON.stringify(ids),JSON.stringify(blocked),published.backup,published.batchId,now());
    result=await publishAccepted(db,{ids,backupDir,onPublish:published=>{
      for(const id of ids){
        const current=db.prepare(`SELECT v.*,bt.status topic_status,bt.primary_json,bt.review_json FROM viewpoints v JOIN workflow_batch_topics bt
          ON bt.topic_id=v.topic_id AND bt.extraction_result_id=v.extraction_result_id WHERE bt.batch_id=? AND v.id=?`).get(batchId,id);
        if(!current||eligibleReason(db,batchId,current,reviewModel))throw new Error('自动发布期间本篇审批或疑点状态已变化');
      }
      holdForHumans();
      save(published);
    }});
    if(!ids.length){
      db.exec('BEGIN IMMEDIATE');
      try{holdForHumans();save(result);db.exec('COMMIT');}catch(e){db.exec('ROLLBACK');throw e;}
    }
    return {...result,blocked,policy:AUTO_PUBLISH_POLICY};
  }catch(error){
    db.prepare(`INSERT INTO workflow_auto_publish_events(batch_id,policy_version,status,published_count,viewpoint_ids_json,blocked_json,error,created_at)
      VALUES(?,?,'failed',?,?,?,?,?)`).run(batchId,AUTO_PUBLISH_POLICY,result?.published||0,JSON.stringify(ids),JSON.stringify(blocked),error.message,now());
    throw error;
  }
}
