import importlib.util
from pathlib import Path
from decimal import Decimal
import pytest

PATH = Path(__file__).parents[1] / 'verify_v06_market_api.py'

def module():
    assert PATH.exists(), 'read-only verifier is not implemented'
    spec = importlib.util.spec_from_file_location('verifier', PATH)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result

def rows(n=20, volume='3'):
    return [dict(trade_date=f'2026-01-{i+1:02}', close=Decimal(i+1), vol=Decimal(volume), pct_chg=None) for i in range(n)]

def test_decimal_trailing_and_nullable_warmup():
    m = module()
    assert m.oracle(rows(), 19)['ma20'] == Decimal('10.500000')
    assert m.oracle(rows(), 3)['ma5'] is None
    assert m.oracle(rows(), 4)['volume_ratio'] == Decimal('1.000000')

def test_zero_volume_ratio_null():
    assert module().oracle(rows(volume='0'), 19)['volume_ratio'] is None

def test_half_up_and_ratio_uses_rounded_mean():
    m = module()
    values = rows(5)
    for row in values:
        row['close'] = Decimal('1.0000005')
        row['vol'] = Decimal('0.0000014')
    assert m.oracle(values,4)['ma5'] == Decimal('1.000001')
    assert m.oracle(values,4)['volume_ratio'] == Decimal('1.400000')

def test_normal_mode_checks_only_full_window_with_prior_context():
    m = module()
    history = rows()
    actual = dict(trade_date=history[-1]['trade_date'],window_size=20,is_warmup=False,
                  close=20,volume=3,pct_chg=None,**m.oracle(history,19))
    checked, unchecked = m.check_indicators(history[-1:], [actual], history[:-1], False)
    assert checked == ['2026-01-20']
    assert unchecked == []

def test_fresh_window_mismatch():
    m = module()
    with pytest.raises(m.VerificationError, match='window'):
        m.check_indicators(rows(1), [dict(trade_date='2026-01-01', window_size=20, is_warmup=False)], [], True)

@pytest.mark.parametrize('dates', [['2026-01-01','2026-01-01'], ['2026-01-02','2026-01-01']])
def test_duplicate_and_unordered_dates(dates):
    m = module()
    with pytest.raises(m.VerificationError, match='ordered'):
        m.ordered([dict(trade_date=d) for d in dates])

@pytest.mark.parametrize('code,start,end', [('bad','2026-01-01','2026-01-02'), ('000001.SZ','2026-02-01','2026-01-01'), ('000001.SZ','2020-01-01','2026-01-01')])
def test_invalid_scope(code,start,end):
    m = module()
    with pytest.raises(m.VerificationError):
        m.scope(code,start,end)

def test_http_failure_sanitized(monkeypatch):
    m = module()
    def fail(*args, **kwargs):
        raise OSError('password=secret')
    monkeypatch.setattr(m, 'urlopen', fail)
    with pytest.raises(m.VerificationError) as error:
        m.fetch('http://localhost:8083', '/actuator/health', 2)
    assert 'secret' not in str(error.value)

def test_normal_mode_requires_full_window():
    m = module()
    with pytest.raises(m.VerificationError, match='full'):
        m.check_indicators(rows(1), [dict(trade_date='2026-01-01',window_size=1,is_warmup=True,close=1,volume=3,pct_chg=None)], [], False)
