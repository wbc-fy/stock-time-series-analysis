"""Strict public JSON contract for V0.7 regression publications."""
import json
import re
from datetime import date, datetime
from .continuity import validate_evidence, require_prefix

ANALYSIS_TYPE = 'v07_xgboost_regression'
TARGET = 'next_trading_day_close_return'
FEATURE_NAMES = ['return_1d', 'return_5d', 'return_10d', 'log_return',
                 'ma5', 'ma10', 'ma20', 'ma60', 'ema5', 'ema10', 'ema20',
                 'macd_dif', 'macd_dea', 'macd_hist', 'rsi', 'bb_width',
                 'vol_ma5', 'vol_ma10', 'volume_ratio', 'volatility_20d',
                 'turnover_rate', 'pe_ttm', 'pb']
FEATURE_VERSION = 'v07_fixed_1'


def validate_stock(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{6}\.(SH|SZ|BJ)', value):
        raise ValueError('Invalid stock code')
    return value


def validate_model_id(value):
    if not isinstance(value, str) or not re.fullmatch(r'v07_[0-9a-f]{32}', value):
        raise ValueError('Invalid model ID')
    return value


def validate_date(value):
    if not isinstance(value, str) or not re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}', value):
        raise ValueError('Invalid ISO date')
    date.fromisoformat(value)
    return value


def validate_payload(payload):
    # JSON serialization also rejects non-native types and nonfinite nested values.
    if not isinstance(payload, dict):
        raise ValueError('Invalid forecast payload')
    try:
        json.dumps(payload, allow_nan=False)
        validate_stock(payload['ts_code'])
        validate_model_id(payload['model_id'])
        if type(payload['schema_version']) is not int or payload['schema_version'] != 1:
            raise ValueError('Invalid schema')
        if payload['target'] != TARGET or type(payload['horizon']) is not int or payload['horizon'] != 1:
            raise ValueError('Invalid target')
        for key, expected in (('model_name', 'xgboost_regressor'), ('model_type', 'xgboost'),
                              ('model_version', '0.7.0')):
            if type(payload[key]) is not str or payload[key] != expected:
                raise ValueError('Invalid model metadata')
        created_at = payload['created_at']
        if (type(created_at) is not str or not re.fullmatch(
                r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?(?:Z|[+-][0-9]{2}:[0-9]{2})', created_at)
                or datetime.fromisoformat(created_at).utcoffset() is None):
            raise ValueError('Invalid creation timestamp')
        validate_date(payload['data_cutoff'])
        validate_date(payload['seen_through'])
        validate_date(payload['source_cutoff'])
        if payload['source_cutoff'] > payload['data_cutoff']:
            raise ValueError('Invalid source cutoff')
        source_days = validate_evidence(payload['source_continuity'], payload['ts_code'])
        inference_days = validate_evidence(payload['inference_continuity'], payload['ts_code'])
        require_prefix(source_days, inference_days)
        if source_days[-1] != payload['source_cutoff'] or inference_days[-1] != payload['data_cutoff']:
            raise ValueError('Calendar evidence cutoff mismatch')
        successors = dict(zip(source_days, source_days[1:]))
        features = payload['feature_names']
        if features != FEATURE_NAMES or payload['feature_version'] != FEATURE_VERSION:
            raise ValueError('Invalid feature contract')
        if type(payload['n_features']) is not int or payload['n_features'] != len(features):
            raise ValueError('Invalid feature count')
        if type(payload['seed']) is not int or payload['seed'] != 42:
            raise ValueError('Invalid random seed')
        previous = None
        for row in payload['test_series']:
            validate_date(row['signal_date'])
            validate_date(row['target_date'])
            if successors.get(row['signal_date']) != row['target_date']:
                raise ValueError('Test target is not the proven next open session')
            if row['signal_date'] <= payload['seen_through'] or row['target_date'] <= row['signal_date']:
                raise ValueError('Invalid test dates')
            if previous is not None and row['signal_date'] <= previous:
                raise ValueError('Unordered test series')
            previous = row['signal_date']
            _number(row['actual_return'])
            _number(row['predicted_return'])
        if not payload['test_series']:
            raise ValueError('Empty test series')
        latest = payload['latest']
        validate_date(latest['signal_date'])
        if (latest['signal_date'] != payload['data_cutoff'] or latest['signal_date'] <= payload['seen_through']
                or latest['actual_return'] is not None or latest['target_date'] is not None
                or type(latest['horizon']) is not int or latest['horizon'] != 1):
            raise ValueError('Invalid latest forecast')
        _number(latest['predicted_return'])
        for group in ('metrics', 'baseline_metrics'):
            for name in ('rmse', 'mae', 'r2'):
                value = payload[group][name]
                if name != 'r2' or value is not None:
                    _number(value)
        for name in ('train', 'val', 'test'):
            split = payload['splits'][name]
            for field in ('signal_start', 'signal_end', 'label_end'):
                validate_date(split[field])
            if type(split['count']) is not int or split['count'] < 1:
                raise ValueError('Invalid split count')
            if not split['signal_start'] <= split['signal_end'] < split['label_end']:
                raise ValueError('Invalid split date ordering')
        splits = payload['splits']
        if not (splits['train']['label_end'] < splits['val']['signal_start']
                and splits['val']['label_end'] < splits['test']['signal_start']
                and payload['seen_through'] == splits['val']['label_end']):
            raise ValueError('Invalid split boundaries')
        series = payload['test_series']
        if (splits['test']['count'] != len(series)
                or splits['test']['signal_start'] != series[0]['signal_date']
                or splits['test']['signal_end'] != series[-1]['signal_date']
                or splits['test']['label_end'] != series[-1]['target_date']
                or series[-1]['target_date'] > payload['source_cutoff']):
            raise ValueError('Test metadata mismatch')
        if payload['importance_method'] != 'gain':
            raise ValueError('Invalid importance method')
        importance_names = []
        for item in payload['feature_importance']:
            if item['feature'] not in features:
                raise ValueError('Unknown importance feature')
            _number(item['importance'])
            if item['importance'] < 0:
                raise ValueError('Negative importance')
            importance_names.append(item['feature'])
        if len(importance_names) != len(features) or set(importance_names) != set(features):
            raise ValueError('Incomplete importance')
        for key in ('params', 'dependency_versions'):
            if not isinstance(payload[key], dict) or not payload[key]:
                raise ValueError('Missing metadata')
        versions = payload['dependency_versions']
        if set(versions) != {'xgboost', 'pandas', 'numpy'}:
            raise ValueError('Invalid dependency metadata')
        for version in versions.values():
            if type(version) is not str or not re.fullmatch(
                    r'[0-9]+\.[0-9]+\.[0-9]+(?:[a-zA-Z0-9.+-]*)', version):
                raise ValueError('Invalid dependency version')
        if not re.fullmatch(r'[0-9a-f]{64}', payload['data_hash']):
            raise ValueError('Invalid data hash')
        params = payload['params']
        if (type(params['n_estimators']) is not int or not 1 <= params['n_estimators'] <= 200
                or type(params['n_jobs']) is not int or params['n_jobs'] != 1
                or params['random_state'] != payload['seed'] or params['device'] != 'cpu'
                or params['objective'] != 'reg:squarederror'):
            raise ValueError('Invalid bounded model parameters')
    except (KeyError, TypeError, OverflowError) as exc:
        raise ValueError('Invalid forecast payload') from exc
    return payload


def _number(value):
    if type(value) not in (int, float):
        raise ValueError('Invalid numeric value')


def parse_payload(serialized):
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    return validate_payload(json.loads(serialized, object_pairs_hook=unique_pairs))
