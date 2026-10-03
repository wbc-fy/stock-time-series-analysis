"""One deterministic frozen model, evaluated only on unseen observations."""
import copy
import hashlib
from datetime import datetime, timezone
from uuid import uuid4
import numpy as np
import pandas as pd
import xgboost as xgb
from .samples import build_samples, FEATURE_VERSION
from .contracts import TARGET, validate_payload
from .continuity import require_prefix

PARAMS = dict(objective='reg:squarederror', n_estimators=80, max_depth=3,
              learning_rate=.05, subsample=1., colsample_bytree=1.,
              random_state=42, n_jobs=1, tree_method='hist', device='cpu')


def metrics(actual, predicted):
    residual = np.asarray(actual)-np.asarray(predicted)
    denominator = float(np.sum((actual-np.mean(actual))**2))
    return dict(rmse=float(np.sqrt(np.mean(residual**2))), mae=float(np.mean(abs(residual))),
                r2=float(1-np.sum(residual**2)/denominator) if denominator else None)


def _latest(model, samples):
    row = samples['latest']
    return dict(signal_date=row.signal_date.iloc[0].strftime('%Y-%m-%d'), target_date=None,
                horizon=1, actual_return=None,
                predicted_return=float(model.predict(row[samples['feature_names']])[0]))


def train_forecast(frame, ts_code, calendar=None):
    samples = build_samples(frame, ts_code, calendar)
    features = samples['feature_names']
    model = xgb.XGBRegressor(**PARAMS)
    # Fixed parameters: validation is recorded as seen, but no model selection/test feedback.
    model.fit(samples['train'][features], samples['train'].actual_return,
              eval_set=[(samples['val'][features], samples['val'].actual_return)], verbose=False)
    test = samples['test']
    predictions = model.predict(test[features])
    series = [dict(signal_date=row.signal_date.strftime('%Y-%m-%d'),
                   target_date=row.target_date.strftime('%Y-%m-%d'),
                   actual_return=float(row.actual_return), predicted_return=float(pred))
              for row, pred in zip(test.itertuples(), predictions)]
    gain = model.get_booster().get_score(importance_type='gain')
    importance = sorted([dict(feature=name, importance=float(gain.get(name, 0.)))
                         for name in features], key=lambda item: (-item['importance'], item['feature']))
    payload = dict(schema_version=1, model_id='v07_'+uuid4().hex, ts_code=ts_code,
                   target=TARGET, horizon=1, model_name='xgboost_regressor', model_type='xgboost',
                   model_version='0.7.0', created_at=datetime.now(timezone.utc).isoformat(),
                   feature_version=FEATURE_VERSION, feature_names=features, n_features=len(features),
                   seed=42, params=dict(PARAMS), splits=samples['splits'],
                   seen_through=samples['seen_through'], data_cutoff=samples['data_cutoff'],
                   source_cutoff=samples['data_cutoff'],
                   source_continuity=samples['source_continuity'],
                   inference_continuity=copy.deepcopy(samples['source_continuity']),
                   data_hash=hashlib.sha256(pd.util.hash_pandas_object(samples['frame'], index=False).values.tobytes()).hexdigest(),
                   dependency_versions=dict(xgboost=xgb.__version__, pandas=pd.__version__, numpy=np.__version__),
                   metrics=metrics(test.actual_return.values, predictions),
                   baseline_metrics=metrics(test.actual_return.values, np.zeros(len(test))),
                   importance_method='gain', feature_importance=importance, test_series=series,
                   latest=_latest(model, samples))
    return model, validate_payload(payload)


def predict_latest(model, payload, frame, ts_code, calendar=None):
    validate_payload(payload)
    if payload['ts_code'] != ts_code:
        raise ValueError('Model stock mismatch')
    samples = build_samples(frame, ts_code, calendar)
    require_prefix(payload['source_continuity']['open_dates'], samples['source_continuity']['open_dates'])
    if samples['data_cutoff'] <= payload['seen_through']:
        raise ValueError('Latest signal must be unseen')
    if samples['data_cutoff'] < payload['data_cutoff']:
        raise ValueError('Cannot regress publication cutoff')
    revision = copy.deepcopy(payload)
    revision['data_cutoff'] = samples['data_cutoff']
    revision['latest'] = _latest(model, samples)
    revision['inference_continuity'] = samples['source_continuity']
    return validate_payload(revision)
