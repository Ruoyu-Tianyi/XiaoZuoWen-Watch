import { openWorkflowDb } from './lib/workflow_db.mjs';
import { runAutomatedPipeline } from './lib/automated_pipeline.mjs';
import { getAutomationStatus, requestBatchStop, ensureAutomationSchema } from './lib/automation_state.mjs';

const command=process.argv[2]||'status';
const option=name=>process.argv.find(a=>a.startsWith(`--${name}=`))?.slice(name.length+3);
const db=openWorkflowDb(undefined,command==='status'?{readOnly:true}:{});
try {
  if(command==='status')console.log(JSON.stringify(getAutomationStatus(db),null,2));
  else if(command==='stop'){ensureAutomationSchema(db);console.log(JSON.stringify(requestBatchStop(db,Number(option('batch')))));}
  else if(command==='run'){
    const result=await runAutomatedPipeline(db,{resume:option('resume'),limit:option('limit'),hours:option('hours'),batchSize:option('batch-size'),
      maxCalls:option('max-calls'),maxChars:option('max-chars'),maxMinutes:option('max-minutes'),
      skipFetch:process.argv.includes('--skip-fetch'),reconcile:process.argv.includes('--reconcile-pilot')},
      {log:event=>console.log(JSON.stringify(event))});
    console.log(JSON.stringify(result,null,2));
    if(result.status==='paused')process.exitCode=2;
  }else throw new Error('支持命令：run、status、stop');
}catch(error){console.error(error.message);process.exitCode=1;}finally{db.close();}
