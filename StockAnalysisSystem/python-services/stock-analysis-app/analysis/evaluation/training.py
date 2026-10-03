"""Deterministic fixed models fitted independently in each retrospective fold."""
import copy
import hashlib
from datetime import datetime, timezone
from uuid import uuid4
import numpy as np
import pandas as pd
import sklearn
import threadpoolctl
import xgboost as xgb
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import Ridge
from threadpoolctl import threadpool_limits
from analysis.forecast.contracts import FEATURE_NAMES, FEATURE_VERSION, TARGET
from analysis.forecast.training import PARAMS
from .protocol import build_folds
from .contracts import METHODS, validate_report


def metrics(actual, predicted):
    actual, predicted = np.asarray(actual, dtype=float), np.asarray(predicted, dtype=float)
    if (actual.ndim != 1 or actual.size == 0 or actual.shape != predicted.shape
            or not np.isfinite(actual).all() or not np.isfinite(predicted).all()):
        raise ValueError('Metrics require nonempty finite paired observations')
    residual = actual-predicted
    constant = actual.size < 2 or np.all(actual == actual[0])
    denominator = 0. if constant else float(np.sum((actual-np.mean(actual))**2))
    result = dict(rmse=float(np.sqrt(np.mean(residual**2))), mae=float(np.mean(abs(residual))),
        r2=float(1-np.sum(residual**2)/denominator) if denominator else None)
    if any(v is not None and not np.isfinite(v) for v in result.values()):
        raise ValueError('Nonfinite metrics')
    return result


def _day(value):
    return value.strftime('%Y-%m-%d')


def evaluate(frame, ts_code, calendar):
    prepared = build_folds(frame, ts_code, calendar)
    folds, series = [], []
    with threadpool_limits(limits=1):
        for part in prepared['folds']:
            train, evaluation = part['train'], part['evaluation']
            train_x, eval_x = train[FEATURE_NAMES], evaluation[FEATURE_NAMES]
            medians = train_x.median().fillna(0)
            scaler = StandardScaler()
            scaled_train = scaler.fit_transform(train_x.fillna(medians))
            scaled_eval = scaler.transform(eval_x.fillna(medians))
            ridge = Ridge(alpha=1.0, solver='svd').fit(scaled_train, train.actual_return)
            boosted = xgb.XGBRegressor(**PARAMS).fit(train_x, train.actual_return)
            predictions = dict(zero_return=np.zeros(100), ridge=ridge.predict(scaled_eval),
                xgboost=boosted.predict(eval_x))
            fold_metrics = {name: metrics(evaluation.actual_return, pred) for name, pred in predictions.items()}
            for index, row in enumerate(evaluation.itertuples()):
                series.append(dict(signal_date=_day(row.signal_date), target_date=_day(row.target_date),
                    fold_id=part['fold_id'], actual_return=float(row.actual_return),
                    predicted_returns={name: float(pred[index]) for name, pred in predictions.items()}))
            folds.append(dict(fold_id=part['fold_id'], train=dict(signal_start=_day(train.signal_date.iloc[0]),
                signal_end=_day(train.signal_date.iloc[-1]), label_end=_day(train.target_date.iloc[-1]),
                count=len(train), purged_count=part['purged_count']), evaluation=dict(
                signal_start=_day(evaluation.signal_date.iloc[0]), signal_end=_day(evaluation.signal_date.iloc[-1]),
                target_start=_day(evaluation.target_date.iloc[0]), target_end=_day(evaluation.target_date.iloc[-1]),
                count=len(evaluation)), metrics=fold_metrics))
    rows = prepared['frame']
    report = dict(schema_version=1, report_id='eval_' + uuid4().hex, ts_code=ts_code, target=TARGET,
        horizon=1, units='fractional_return', protocol_id='v08_expanding_3x100_1',
        evaluation_kind='retrospective_walk_forward', history_previously_observed=True,
        prospective_validation=False, created_at=datetime.now(timezone.utc).isoformat(),
        source=dict(signal_start=_day(rows.signal_date.iloc[0]), signal_end=_day(rows.signal_date.iloc[-1]),
            row_count=len(rows), data_hash=hashlib.sha256(pd.util.hash_pandas_object(rows, index=False).values.tobytes()).hexdigest()),
        source_continuity=prepared['source_continuity'], feature_version=FEATURE_VERSION,
        feature_names=list(FEATURE_NAMES), methods=copy.deepcopy(METHODS),
        dependency_versions=dict(numpy=np.__version__, pandas=pd.__version__, sklearn=sklearn.__version__,
            xgboost=xgb.__version__, threadpoolctl=threadpoolctl.__version__), folds=folds, series=series,
        overall_metrics={name: metrics([r['actual_return'] for r in series],
            [r['predicted_returns'][name] for r in series]) for name in METHODS})
    return validate_report(report)
