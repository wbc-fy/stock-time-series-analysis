import { finiteJson, createGenerationGuard, settleDetailRequest } from './prediction.js'

export const EVALUATION_METHODS = ['zero_return', 'ridge', 'xgboost']
const FEATURES = ['return_1d','return_5d','return_10d','log_return','ma5','ma10','ma20','ma60','ema5','ema10','ema20','macd_dif','macd_dea','macd_hist','rsi','bb_width','vol_ma5','vol_ma10','volume_ratio','volatility_20d','turnover_rate','pe_ttm','pb']
const METHODS = { zero_return: {}, ridge: { alpha: 1, solver: 'svd', imputation: 'train_median_all_missing_zero', scaling: 'standard_scaler_train_only' }, xgboost: { objective: 'reg:squarederror', n_estimators: 80, max_depth: 3, learning_rate: .05, subsample: 1, colsample_bytree: 1, random_state: 42, n_jobs: 1, tree_method: 'hist', device: 'cpu' } }
const COMMON = 'schema_version report_id ts_code target horizon units protocol_id evaluation_kind history_previously_observed prospective_validation created_at source feature_version folds overall_metrics'.split(' ')
const require = condition => { if (!condition) throw new Error('滚动评估报告格式、口径或绑定无效') }
const keys = (value, fields) => require(value && typeof value === 'object' && !Array.isArray(value) && Object.keys(value).sort().join('|') === [...fields].sort().join('|'))
const number = value => require(typeof value === 'number' && Number.isFinite(value))
const integer = (value, low, high) => require(Number.isSafeInteger(value) && value >= low && value <= high)
const hash = value => require(typeof value === 'string' && /^[0-9a-f]{64}$/.test(value))
const day = value => require(typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value)) && new Date(value).toISOString().slice(0,10) === value)
function equal(a,b) {
  if(a && typeof a === 'object' && b && typeof b === 'object'){keys(b,Object.keys(a));for(const k of Object.keys(a))equal(a[k],b[k])}
  else require(a === b)
}
function metrics(value) {
  keys(value,EVALUATION_METHODS)
  for(const m of Object.values(value)) {
    keys(m,['rmse','mae','r2']);number(m.rmse);number(m.mae);require(m.rmse>=0 && m.mae>=0)
    if(m.r2!==null){number(m.r2);require(m.r2<=1)}
  }
}
function range(value, fields, low, high) {
  keys(value,fields.split(' '));for(const k of Object.keys(value))if(k.endsWith('start')||k.endsWith('end'))day(value[k])
  integer(value.count,low,high);require(value.signal_start<=value.signal_end)
}
function common(r,code,id) {
  finiteJson(r)
  require(code==='000001.SZ' && r.ts_code===code && /^eval_[0-9a-f]{32}$/.test(r.report_id) && (id===undefined || r.report_id===id))
  const fixed={schema_version:1,target:'next_trading_day_close_return',horizon:1,units:'fractional_return',protocol_id:'v08_expanding_3x100_1',evaluation_kind:'retrospective_walk_forward',history_previously_observed:true,prospective_validation:false,feature_version:'v07_fixed_1'}
  for(const k of Object.keys(fixed))require(r[k]===fixed[k])
  require(typeof r.created_at==='string' && /^\d{4}-\d{2}-\d{2}T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d{1,6})?(?:Z|\+00:00)$/.test(r.created_at) && Number.isFinite(Date.parse(r.created_at)))
  day(r.created_at.slice(0,10))
  keys(r.source,['signal_start','signal_end','row_count','data_hash']);day(r.source.signal_start);day(r.source.signal_end);integer(r.source.row_count,300,2500);hash(r.source.data_hash);require(r.source.signal_start<r.source.signal_end)
  require(Array.isArray(r.folds) && r.folds.length===3)
  r.folds.forEach((f,i)=>{
    keys(f,['fold_id','train','evaluation','metrics']);require(f.fold_id===i+1)
    const t=f.train,e=f.evaluation
    range(t,'signal_start signal_end label_end count purged_count',300,2500);require(t.purged_count===1)
    range(e,'signal_start signal_end target_start target_end count',100,100)
    require(r.source.signal_start<=t.signal_start && t.signal_end<t.label_end && t.label_end<e.signal_start && e.signal_end<e.target_end && e.target_end<=r.source.signal_end && e.signal_start<e.target_start && e.target_start<=e.target_end)
    if(i){const prev=r.folds[i-1];require(prev.evaluation.target_end===e.signal_start && t.count===prev.train.count+100 && t.signal_start===prev.train.signal_start)}
    metrics(f.metrics)
  });metrics(r.overall_metrics)
}
function evaluationRange(r) {
  const first=r.folds[0].evaluation,last=r.folds[2].evaluation
  return {signal_start:first.signal_start,signal_end:last.signal_end,target_start:first.target_start,target_end:last.target_end,count:300}
}
export function normalizeEvaluations(rows,code) {
  require(Array.isArray(rows));const ids=new Set()
  rows.forEach(r=>{
    keys(r,[...COMMON,'evaluation','report_sha256','report_bytes']);common(r,code)
    require(!ids.has(r.report_id));ids.add(r.report_id)
    hash(r.report_sha256);integer(r.report_bytes,1,2*1024*1024);equal(evaluationRange(r),r.evaluation)
    require(new TextEncoder().encode(JSON.stringify(r)).length<=65535)
  });return rows
}
function checkComputed(rows,supplied) {
  const actual=rows.map(r=>r.actual_return),mean=actual.reduce((a,b)=>a+b,0)/actual.length
  const variance=actual.every(v=>v===actual[0])?0:actual.reduce((s,v)=>s+(v-mean)**2,0)
  for(const method of EVALUATION_METHODS){
    const errors=rows.map(r=>r.actual_return-r.predicted_returns[method]),sse=errors.reduce((s,v)=>s+v*v,0)
    const expected={rmse:Math.sqrt(sse/rows.length),mae:errors.reduce((s,v)=>s+Math.abs(v),0)/rows.length,r2:variance?1-sse/variance:null}
    for(const k of Object.keys(expected)){const a=expected[k],b=supplied[method][k];require(a===null?b===null:b!==null && Math.abs(a-b)<=Math.max(1e-12,1e-10*Math.max(Math.abs(a),Math.abs(b))))}
  }
}
export function normalizeEvaluation(r,code,id) {
  keys(r,[...COMMON,'source_continuity','feature_names','methods','dependency_versions','series']);common(r,code,id)
  require(Array.isArray(r.feature_names) && JSON.stringify(r.feature_names)===JSON.stringify(FEATURES));equal(METHODS,r.methods)
  keys(r.dependency_versions,['numpy','pandas','sklearn','xgboost','threadpoolctl']);require(Object.values(r.dependency_versions).every(v=>typeof v==='string' && v.length>0 && v.length<=100))
  const p=r.source_continuity
  keys(p,'source exchange calendar_start calendar_end source_start source_end session_count open_dates source_dates_hash'.split(' '))
  require(p.source==='tushare.trade_cal' && p.exchange==='SSE');hash(p.source_dates_hash)
  for(const k of ['calendar_start','calendar_end','source_start','source_end'])day(p[k])
  const days=p.open_dates;require(Array.isArray(days));integer(p.session_count,300,2500)
  require(days.length===p.session_count && days.length===r.source.row_count && days[0]===p.source_start && days.at(-1)===p.source_end && days[0]===r.source.signal_start && days.at(-1)===r.source.signal_end && p.calendar_start<=days[0] && days.at(-1)<=p.calendar_end && (Date.parse(p.calendar_end)-Date.parse(p.calendar_start))/86400000<=10000)
  days.forEach((d,i)=>{day(d);require(!i || days[i-1]<d)})
  require(Array.isArray(r.series) && r.series.length===300)
  r.series.forEach((row,i)=>{
    keys(row,['signal_date','target_date','fold_id','actual_return','predicted_returns'])
    require(row.fold_id===Math.floor(i/100)+1 && row.signal_date===days[days.length-301+i] && row.target_date===days[days.length-300+i])
    number(row.actual_return);keys(row.predicted_returns,EVALUATION_METHODS);Object.values(row.predicted_returns).forEach(number);require(row.predicted_returns.zero_return===0)
  })
  r.folds.forEach((f,i)=>{
    const rows=r.series.slice(i*100,(i+1)*100),first=rows[0],last=rows.at(-1),t=f.train
    equal({signal_start:first.signal_date,signal_end:last.signal_date,target_start:first.target_date,target_end:last.target_date,count:100},f.evaluation)
    const begin=days.indexOf(t.signal_start),end=days.indexOf(t.signal_end)
    require(begin>=0 && end-begin+1===t.count && end===days.length-303+i*100 && days[end+1]===t.label_end)
    checkComputed(rows,f.metrics)
  });checkComputed(r.series,r.overall_metrics);return r
}

// Shared with Vue: transport cancellation is advisory, generation validity is authoritative.
export function createEvaluationController({state,list,detail}) {
  const listGuard=createGenerationGuard(),detailGuard=createGenerationGuard();let code=''
  const reset=()=>Object.assign(state,{rows:[],selected:'',report:null,listLoading:false,detailLoading:false,listError:'',detailError:''})
  reset()
  const cancel=()=>{listGuard.cancel();detailGuard.cancel();reset()}
  async function selectReport(id){
    const run=detailGuard.next(),stock=code
    Object.assign(state,{selected:id,report:null,detailError:'',detailLoading:Boolean(id)})
    if(!id)return
    await settleDetailRequest(Promise.resolve().then(()=>detail(stock,id,{signal:run.signal})),run,{
      success(dto){state.report=dto},error(e){state.report=null;state.detailError=e.message},settled(){state.detailLoading=false},
    })
  }
  async function loadStock(stock){
    cancel();code=stock;const run=listGuard.next();state.listLoading=true
    await settleDetailRequest(Promise.resolve().then(()=>list(stock,{signal:run.signal})),run,{
      success(rows){state.rows=rows;if(rows.length)void selectReport(rows[0].report_id)},error(e){state.listError=e.message},settled(){state.listLoading=false},
    })
  }
  return {loadStock,selectReport,cancel}
}
