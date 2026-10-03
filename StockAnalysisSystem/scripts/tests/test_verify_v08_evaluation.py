"""Synthetic, independently constructed raw sessions; no production imports."""
import copy
import hashlib
import importlib.util
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from verify_v07_prediction import metrics, VerificationError


@pytest.fixture
def oracle():
    path = SCRIPTS / 'verify_v08_evaluation.py'
    assert path.exists(), 'V0.8 independent oracle is not implemented'
    spec = importlib.util.spec_from_file_location('v08_oracle', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def sample():
    days = [(date(2022, 1, 1)+timedelta(days=i)).isoformat() for i in range(750)]
    source = [dict(trade_date=d, close=Decimal(100+i%17)) for i, d in enumerate(days)]
    calendar = dict(source='tushare.trade_cal', exchange='SSE', start=days[0], end=days[-1], open_dates=days)
    proof = dict(source='tushare.trade_cal', exchange='SSE', calendar_start=days[0], calendar_end=days[-1],
        source_start=days[0], source_end=days[-1], open_dates=days, session_count=len(days),
        source_dates_hash=hashlib.sha256('\n'.join(days).encode()).hexdigest())
    rows = []
    for i in range(449, 749):
        rows.append(dict(signal_date=days[i], target_date=days[i+1], fold_id=(i-449)//100+1,
            actual_return=float(source[i+1]['close']/source[i]['close']-1),
            predicted_returns=dict(zero_return=0., ridge=.01, xgboost=.02)))
    def computed(part):
        actual = [Decimal(str(r['actual_return'])) for r in part]
        return {m: {k: float(v) if v is not None else None for k,v in metrics(actual,
            [Decimal(str(r['predicted_returns'][m])) for r in part]).items()} for m in ('zero_return','ridge','xgboost')}
    folds = []
    for f in range(3):
        begin = 449+100*f
        folds.append(dict(fold_id=f+1, train=dict(signal_start=days[59], signal_end=days[begin-2],
            label_end=days[begin-1], count=begin-60, purged_count=1),
            evaluation=dict(signal_start=days[begin], signal_end=days[begin+99], target_start=days[begin+1], target_end=days[begin+100], count=100),
            metrics=computed(rows[f*100:(f+1)*100])))
    report = dict(schema_version=1, report_id='eval_'+'a'*32, ts_code='000001.SZ', horizon=1,
        target='next_trading_day_close_return', units='fractional_return', protocol_id='v08_expanding_3x100_1',
        evaluation_kind='retrospective_walk_forward', history_previously_observed=True, prospective_validation=False,
        feature_version='v07_fixed_1', source=dict(signal_start=days[0], signal_end=days[-1], row_count=750, data_hash='a'*64),
        source_continuity=proof, folds=folds, series=rows, overall_metrics=computed(rows))
    return source, report, calendar


def test_full_raw_tail_and_decimal_metrics(oracle, sample):
    result = oracle.verify(*sample)
    assert result['status'] == 'PASS'
    assert result['checked_rows'] == 300
    assert result['training_fit_provenance_verified'] is False


@pytest.mark.parametrize('change', [
    lambda r: r.update(report_id='eval_short'), lambda r: r.update(ts_code='000002.SZ'),
    lambda r: r.update(horizon=True), lambda r: r.update(units='percent'),
    lambda r: r.update(history_previously_observed=False), lambda r: r.update(prospective_validation=True),
    lambda r: r['series'][0].update(target_date=r['series'][0]['signal_date']),
    lambda r: r['series'][0].update(signal_date=r['series'][1]['signal_date']),
    lambda r: r['series'][0].update(actual_return=.5),
    lambda r: r['series'][0]['predicted_returns'].update(zero_return=.001),
    lambda r: r['series'][0]['predicted_returns'].update(ridge=float('nan')),
    lambda r: r['folds'][0]['train'].update(label_end=r['series'][0]['signal_date']),
    lambda r: r['folds'][0]['train'].update(count=299),
    lambda r: r['folds'][0]['train'].update(purged_count=0),
    lambda r: r['folds'][1]['evaluation'].update(count=99),
    lambda r: r['folds'][0]['metrics']['ridge'].update(rmse=.9),
    lambda r: r['overall_metrics']['ridge'].update(mae=.9),
])
def test_rejects_tampering(oracle, sample, change):
    source, report, calendar = sample
    change(report)
    with pytest.raises(VerificationError): oracle.verify(source, report, calendar)


def test_calendar_missing_session(oracle, sample):
    source, report, calendar = sample
    calendar['open_dates'].pop(100)
    with pytest.raises(VerificationError): oracle.verify(source, report, calendar)


def test_pooled_rmse_is_not_mean_fold_rmse():
    a = [Decimal(0)]*300
    p = [Decimal('.01')]*100+[Decimal('.02')]*100+[Decimal('.03')]*100
    assert metrics(a,p)['rmse'] != sum(metrics(a[i:i+100],p[i:i+100])['rmse'] for i in (0,100,200))/3


@pytest.mark.parametrize('raw', [b'{"x":1,"x":2}', b'{"x":NaN}', b'x'* (2*1024*1024+1)], ids=['duplicate','nonfinite','oversize'])
def test_safe_json(oracle, raw):
    with pytest.raises(VerificationError): oracle.read_json(raw)


@pytest.mark.parametrize('change', [dict(timeout=30), dict(timeout=float('nan')), dict(timeout=0),
    dict(api_base='http://user:secret@localhost'), dict(api_base='http://localhost/path'),
    dict(api_base='http://localhost:bad'), dict(report='eval_bad'), dict(stock='000002.SZ'),
    dict(start='2024-01-01',end='2023-01-01')])
def test_cli_invalid_before_io(oracle, change):
    args = SimpleNamespace(stock='000001.SZ', report='eval_'+'a'*32,start='2022-01-01',end='2023-01-01',
        calendar='missing',api_base='http://127.0.0.1:8084',timeout=15)
    vars(args).update(change)
    with pytest.raises(VerificationError): oracle.run(args)


def test_cli_binds_requested_report_and_source_bounds(oracle, sample, monkeypatch, tmp_path):
    import json
    source, report, calendar = sample
    path = tmp_path/'calendar.json'
    path.write_text(json.dumps(calendar))
    args = SimpleNamespace(stock='000001.SZ', report='eval_'+'b'*32,start=calendar['start'],end=calendar['end'],
        calendar=str(path),api_base='http://localhost:8084',timeout=15)
    monkeypatch.setattr(oracle,'load_source',lambda *a:source)
    calls=[]
    monkeypatch.setattr(oracle,'fetch',lambda *a: calls.append(a) or report)
    with pytest.raises(VerificationError): oracle.run(args)
    assert calls[0][1] == '/api/analysis/evaluations/000001.SZ/'+args.report


@pytest.mark.parametrize('bound', ['start', 'end'])
def test_cli_matching_id_rejects_requested_source_bounds(oracle, sample, monkeypatch, tmp_path, bound):
    import json
    source, report, calendar = sample
    path = tmp_path/'calendar.json'
    path.write_text(json.dumps(calendar))
    args = SimpleNamespace(stock=report['ts_code'], report=report['report_id'],
        start=calendar['start'], end=calendar['end'], calendar=str(path),
        api_base='http://localhost:8084', timeout=15)
    setattr(args, bound, calendar['open_dates'][1 if bound == 'start' else -2])
    monkeypatch.setattr(oracle, 'load_source', lambda *a: source)
    monkeypatch.setattr(oracle, 'fetch', lambda *a: report)
    with pytest.raises(VerificationError, match='Requested source bounds mismatch'):
        oracle.run(args)


@pytest.mark.parametrize('change', [
    lambda s,r,c: s[1].update(close=0),
    lambda s,r,c: s[1].update(close=float('inf')),
    lambda s,r,c: s[1].update(trade_date=s[0]['trade_date']),
    lambda s,r,c: c.update(exchange='BSE'),
    lambda s,r,c: c.update(end=c['start']),
    lambda s,r,c: r['source'].update(row_count=749),
    lambda s,r,c: r['source_continuity'].update(source_dates_hash='b'*64),
    lambda s,r,c: r['series'][0]['predicted_returns'].update(extra=0),
    lambda s,r,c: r['folds'][0]['metrics']['xgboost'].update(r2=None),
])
def test_input_and_calendar_faults(oracle, sample, change):
    change(*sample)
    with pytest.raises(VerificationError): oracle.verify(*sample)


def test_null_r2_constant_actual_strictly_matches(oracle, sample):
    source, report, calendar = sample
    for row in source: row['close'] = Decimal(100)
    for row in report['series']: row['actual_return'] = 0.
    supplied = {m:{k:float(v) if v is not None else None for k,v in
        metrics([Decimal(0)]*100,[Decimal(str(report['series'][0]['predicted_returns'][m]))]*100).items()} for m in ('zero_return','ridge','xgboost')}
    for fold in report['folds']: fold['metrics'] = copy.deepcopy(supplied)
    report['overall_metrics'] = copy.deepcopy(supplied)
    assert oracle.verify(*sample)['status'] == 'PASS'
    report['overall_metrics']['ridge']['r2'] = 0.
    with pytest.raises(VerificationError): oracle.verify(*sample)


def test_bounded_http_and_sanitized_dependency_failure(oracle, monkeypatch):
    import verify_v07_prediction as primitives
    class Response:
        def __enter__(self): return self
        def __exit__(self,*args): pass
        def read(self,n):
            assert n == 2*1024*1024+1
            return b'x'*n
    monkeypatch.setattr(primitives,'urlopen',lambda *a,**kw: Response())
    with pytest.raises(VerificationError): oracle.fetch('http://localhost','/read',15)
    def unavailable(*args,**kwargs): raise RuntimeError('secret-password')
    monkeypatch.setattr(primitives,'urlopen', unavailable)
    with pytest.raises(VerificationError, match='details suppressed') as error:
        oracle.fetch('http://localhost','/read',15)
    assert 'secret' not in str(error.value)


def test_main_suppresses_unknown_errors(oracle, monkeypatch, capsys):
    def broken(args): raise RuntimeError('secret-password')
    monkeypatch.setattr(oracle,'run',broken)
    assert oracle.main(['--stock','000001.SZ','--report','eval_'+'a'*32,'--start','2022-01-01',
        '--end','2023-01-01','--calendar','missing']) == 1
    assert 'secret' not in capsys.readouterr().err
