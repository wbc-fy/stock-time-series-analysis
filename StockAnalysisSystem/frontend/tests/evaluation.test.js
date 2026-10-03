import test from 'node:test'
import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { createHash } from 'node:crypto'
import { execFileSync } from 'node:child_process'
import { createRequestClient } from '../src/api/client.js'
import { returnPercent } from '../src/utils/prediction.js'
const evaluation = await import('../src/utils/evaluation.js').catch(() => ({}))
const id = 'eval_' + 'a'.repeat(32), code = '000001.SZ'
const names = ['return_1d','return_5d','return_10d','log_return','ma5','ma10','ma20','ma60','ema5','ema10','ema20','macd_dif','macd_dea','macd_hist','rsi','bb_width','vol_ma5','vol_ma10','volume_ratio','volatility_20d','turnover_rate','pe_ttm','pb']
const methods = { zero_return: {}, ridge: { alpha: 1, solver: 'svd', imputation: 'train_median_all_missing_zero', scaling: 'standard_scaler_train_only' }, xgboost: { objective: 'reg:squarederror', n_estimators: 80, max_depth: 3, learning_rate: .05, subsample: 1, colsample_bytree: 1, random_state: 42, n_jobs: 1, tree_method: 'hist', device: 'cpu' } }
function fixture() {
  const days = Array.from({length: 700}, (_, i) => new Date(Date.UTC(2020,0,1+i)).toISOString().slice(0,10))
  const metrics = () => Object.fromEntries(Object.keys(methods).map(m => [m, {rmse: .01, mae: .01, r2: null}]))
  const series = days.slice(399,699).map((day,i) => ({ signal_date: day, target_date: days[400+i], fold_id: Math.floor(i/100)+1, actual_return: .01, predicted_returns: {zero_return:0,ridge:0,xgboost:0} }))
  const folds = [1,2,3].map((f,i) => ({ fold_id: f, train: {signal_start:days[59],signal_end:days[397+i*100],label_end:days[398+i*100],count:339+i*100,purged_count:1}, evaluation:{signal_start:days[399+i*100],signal_end:days[498+i*100],target_start:days[400+i*100],target_end:days[499+i*100],count:100}, metrics:metrics() }))
  return { schema_version:1,report_id:id,ts_code:code,target:'next_trading_day_close_return',horizon:1,units:'fractional_return',protocol_id:'v08_expanding_3x100_1',evaluation_kind:'retrospective_walk_forward',history_previously_observed:true,prospective_validation:false,created_at:'2025-01-01T00:00:00+00:00',source:{signal_start:days[0],signal_end:days.at(-1),row_count:700,data_hash:'b'.repeat(64)},feature_version:'v07_fixed_1',feature_names:names,methods:structuredClone(methods),dependency_versions:Object.fromEntries(['numpy','pandas','sklearn','xgboost','threadpoolctl'].map(k=>[k,'1.0'])),source_continuity:{source:'tushare.trade_cal',exchange:'SSE',calendar_start:days[0],calendar_end:days.at(-1),source_start:days[0],source_end:days.at(-1),session_count:700,open_dates:days,source_dates_hash:createHash('sha256').update(days.join('\n')).digest('hex')},folds,overall_metrics:metrics(),series }
}
function summary(r=fixture()) {
  const {series,source_continuity,feature_names,methods,dependency_versions,...s}=r
  const first=s.folds[0].evaluation,last=s.folds[2].evaluation
  return {...s,evaluation:{signal_start:first.signal_start,signal_end:last.signal_end,target_start:first.target_start,target_end:last.target_end,count:300},report_sha256:'c'.repeat(64),report_bytes:1000}
}
test('strict normalizers exist and preserve fractional values', () => {
  assert.equal(typeof evaluation.normalizeEvaluation,'function')
  const report=fixture()
  assert.equal(evaluation.normalizeEvaluation(report,code,id),report)
  assert.equal(returnPercent(report.series[0].actual_return),1)
  assert.deepEqual(evaluation.normalizeEvaluations([summary()],code),[summary()])
  assert.deepEqual(evaluation.normalizeEvaluations([],code),[])
})
test('rejects malformed report bindings, fixed schema, methods, dates, finite returns and metrics', () => {
  assert.equal(typeof evaluation.normalizeEvaluation,'function')
  const changes=[r=>r.series.pop(),r=>r.series[0].actual_return=null,r=>r.series[0].actual_return=NaN,r=>r.series[0].target_date=r.series[2].signal_date,r=>r.ts_code='600000.SH',r=>r.report_id='a'.repeat(32),r=>r.units='percent',r=>r.history_previously_observed=false,r=>r.prospective_validation=true,r=>r.feature_names.reverse(),r=>delete r.methods.ridge,r=>r.methods.ridge.alpha=2,r=>r.series[0].predicted_returns.extra=0,r=>r.folds[1].train.count++,r=>r.folds[0].metrics.ridge.rmse=.02,r=>r.overall_metrics.xgboost.mae=Infinity,r=>r.source_continuity.open_dates[10]=r.source_continuity.open_dates[9],r=>r.schema_version=true,r=>r.extra=true,r=>r.created_at='2025 Jan 1Z']
  for(const change of changes){const r=structuredClone(fixture());change(r);assert.throws(()=>evaluation.normalizeEvaluation(r,code,id))}
  assert.throws(()=>evaluation.normalizeEvaluation(fixture(),code,'eval_'+'d'.repeat(32)))
  for(const change of [s=>s.ts_code='600000.SH',s=>s.overall_metrics.ridge.rmse=null,s=>s.evaluation.count=299,s=>s.report_bytes=0,s=>s.series=[]]){const s=summary();change(s);assert.throws(()=>evaluation.normalizeEvaluations([s],code))}
})
test('client allowlist admits only exact evaluation paths',async()=>{
  const calls=[]
  const client=createRequestClient({mode:'hybrid',mockDelayMs:0,fetchImpl:async path=>{calls.push(path);return{ok:true,json:async()=>[]}}})
  for(const path of ['/api/analysis/evaluations?ts_code=000001.SZ',`/api/analysis/evaluations/${code}/${id}`])await client(()=>{throw Error('mock')},path)
  assert.equal(calls.length,2)
  for(const path of ['/api/analysis/evaluations/x',`/api/analysis/evaluations/${code}/${id}/extra`,`/api/analysis/evaluations/${code}/${'a'.repeat(32)}`])assert.equal(await client(()=> 'demo',path),'demo')
})
const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b});return{promise,resolve,reject}}
test('component uses the tested controller, mutually exclusive panels and precise proxies',async()=>{
  const component=await readFile(new URL('../src/views/analysis/AnalysisEvaluationLive.vue',import.meta.url),'utf8').catch(()=> '')
  assert.match(component,/createEvaluationController/)
  assert.match(component,/onBeforeUnmount/)
  const wrapper=await readFile(new URL('../src/views/analysis/AnalysisPrediction.vue',import.meta.url),'utf8')
  assert.match(wrapper,/AnalysisEvaluationLive v-else/)
  const vite=await readFile(new URL('../vite.config.js',import.meta.url),'utf8')
  assert.match(vite,/\^\/api\/analysis\/evaluations/)
  assert.doesNotMatch(vite,/'\/api\/analysis'\s*:/)
})
for(const boundary of ['report','stock','subpanel','mode','unmount'])for(const outcome of ['success','error'])test(`actual flow ignores stale ${outcome} after ${boundary}, even when AbortSignal ignored`,async()=>{
  assert.equal(typeof evaluation.createEvaluationController,'function')
  const state={};const old=deferred(),fresh=deferred();let detailCalls=0
  const flow=evaluation.createEvaluationController({state,list:async()=>[summary()],detail:()=>++detailCalls===1?old.promise:fresh.promise})
  await flow.loadStock(code)
  if(boundary==='report')flow.selectReport('eval_'+'d'.repeat(32))
  else if(boundary==='stock')await flow.loadStock('600000.SH')
  else flow.cancel()
  const before=structuredClone(state)
  if(outcome==='success')old.resolve(fixture());else old.reject(Error('stale secret'))
  await new Promise(resolve=>setImmediate(resolve))
  assert.deepEqual(state,before)
  flow.cancel();fresh.resolve(null)
})
for(const outcome of ['success','error'])test(`list ${outcome} cannot resurrect detail after stock or unmount`,async()=>{
  assert.equal(typeof evaluation.createEvaluationController,'function')
  const state={},old=deferred();let details=0
  const flow=evaluation.createEvaluationController({state,list:()=>old.promise,detail:async()=>{details++;return fixture()}})
  const pending=flow.loadStock(code);flow.cancel();const before=structuredClone(state)
  if(outcome==='success')old.resolve([summary()]);else old.reject(Error('stale'))
  await pending;assert.deepEqual(state,before);assert.equal(details,0)
})
test('unknown report and current errors clear old report without fallback or training',async()=>{
  assert.equal(typeof evaluation.createEvaluationController,'function')
  const state={};let result=fixture()
  const flow=evaluation.createEvaluationController({state,list:async()=>[summary()],detail:async()=>result})
  await flow.loadStock(code);await new Promise(resolve=>setImmediate(resolve));assert.equal(state.report.report_id,id)
  result=null;await flow.selectReport(id);assert.equal(state.report,null);assert.equal(state.detailError,'')
  result=Promise.reject(Error('HTTP 503'));await flow.selectReport(id);assert.equal(state.report,null);assert.equal(state.detailError,'HTTP 503')
})
test('API wrappers use GET, exact full IDs, abort and 404 empty without demo fallback',()=>{
  execFileSync(process.execPath,['--input-type=module','-e',`
    import assert from 'node:assert/strict';
    import {readFileSync} from 'node:fs';
    const report=JSON.parse(readFileSync(0,'utf8'));
    const calls=[];let status=200;
    globalThis.fetch=async(url,options)=>{calls.push({url,options});return new Response(JSON.stringify(url.includes('?')?[]:report),{status})};
    const {dataSource}=await import('./src/api/client.js');
    const api=await import('./src/api/analysis.js');dataSource.value='hybrid';
    const abort=new AbortController();
    assert.deepEqual(await api.getEvaluations('${code}',{signal:abort.signal,method:'POST'}),[]);
    assert.equal((await api.getEvaluation('${code}','${id}',{signal:abort.signal})).series[0].actual_return,.01);
    assert.equal(calls[0].url,'/api/analysis/evaluations?ts_code=${code}');
    assert.equal(calls[1].url,'/api/analysis/evaluations/${code}/${id}');
    assert.equal(calls[0].options.method,'GET');assert.ok(calls[0].options.signal);
    status=404;assert.equal(await api.getEvaluation('${code}','${id}'),null);
    status=503;await assert.rejects(api.getEvaluation('${code}','${id}'),/503/);
    const count=calls.length;dataSource.value='mock';await assert.rejects(api.getEvaluations('${code}'),/演示模式不可用/);assert.equal(calls.length,count);
    await assert.rejects(api.getEvaluation('${code}','${'a'.repeat(32)}'),/报告标识无效/);
  `],{cwd:new URL('..',import.meta.url),stdio:'pipe',input:JSON.stringify(fixture())})
})
test('timestamp must be a UTC datetime, not a parseable date-only string',()=>{
  const r=fixture();r.created_at='2025-01-01Z'
  assert.throws(()=>evaluation.normalizeEvaluation(r,code,id))
})
test('proxy exact evaluation regexes retain all existing service targets',async()=>{
  const {default:config}=await import('../vite.config.js')
  const proxy=config({mode:'test'}).server.proxy
  const matches=path=>Object.entries(proxy).filter(([key])=>key.startsWith('^')?new RegExp(key).test(path):path.startsWith(key))
  assert.equal(matches(`/api/analysis/evaluations/${code}/${id}`)[0][1].target,'http://127.0.0.1:8084')
  assert.equal(matches('/api/analysis/evaluations?ts_code=000001.SZ')[0][1].target,'http://127.0.0.1:8084')
  assert.equal(matches('/api/analysis/evaluations/invalid').length,0)
  assert.equal(matches(`/api/analysis/evaluations/${code}/${id}/extra`).length,0)
  assert.equal(matches('/api/analysis/kline/000001.SZ')[0][1].target,'http://127.0.0.1:8083')
  assert.equal(matches('/api/consumer/statistics')[0][1].target,'http://127.0.0.1:8080')
})
for(const boundary of ['stock','subpanel','mode','unmount'])for(const outcome of ['success','error'])test(`pending list ignores ${outcome} across ${boundary}`,async()=>{
  const state={},old=deferred();let calls=0,details=0
  const flow=evaluation.createEvaluationController({state,list:()=>++calls===1?old.promise:Promise.resolve([]),detail:async()=>{details++;return fixture()}})
  const pending=flow.loadStock(code);await Promise.resolve()
  if(boundary==='stock')await flow.loadStock('600000.SH');else flow.cancel()
  const before=structuredClone(state)
  if(outcome==='success')old.resolve([summary()]);else old.reject(Error('old list'))
  await pending;assert.deepEqual(state,before);assert.equal(details,0)
})
