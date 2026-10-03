"""Fixed raw-session boundaries with strictly purged expanding training."""
from analysis.forecast.samples import prepare_causal_rows


def build_folds(frame, ts_code, calendar):
    if ts_code != '000001.SZ':
        raise ValueError('Evaluation supports only 000001.SZ')
    prepared = prepare_causal_rows(frame, ts_code, calendar)
    rows = prepared['frame']
    required = ['return_10d', 'ma60', 'rsi', 'actual_return', 'target_date']
    folds = []
    for offset in range(3):
        begin = len(rows) - 301 + offset * 100
        evaluation = rows.iloc[begin:begin+100].copy()
        if len(evaluation) != 100 or evaluation[required].isna().any().any():
            raise ValueError('Invalid evaluation features or labels')
        eligible = rows.dropna(subset=required)
        earlier = eligible[eligible.signal_date < evaluation.signal_date.iloc[0]]
        train = earlier[earlier.target_date < evaluation.signal_date.iloc[0]].copy()
        if len(train) < 300:
            raise ValueError('Evaluation requires at least 300 training samples per fold')
        folds.append(dict(fold_id=offset+1, train=train, evaluation=evaluation,
                          purged_count=len(earlier)-len(train)))
    return dict(prepared, folds=folds)
