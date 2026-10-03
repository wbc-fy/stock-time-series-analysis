"""Bounded read-only V0.8 evaluations; no training or publication imports."""
from fastapi import HTTPException, Request
from analysis.forecast.contracts import validate_stock
from analysis.evaluation.contracts import validate_report, validate_summary, validate_report_id


def register_evaluation_routes(app):
    # Runtime import reuses V0.7 request semantics without an initialization cycle.
    from api.forecast import _query, _limit, _validate

    def read(repository, method, *args):
        try:
            if repository is None:
                raise RuntimeError('Dependency unavailable')
            return getattr(repository, method)(*args)
        except Exception:
            raise HTTPException(503, 'Evaluation dependency unavailable') from None

    def stock(ts_code):
        _validate(validate_stock, ts_code)
        if not read(app.state.repository, 'stock_exists', ts_code):
            raise HTTPException(404, 'Resource not found')

    @app.get('/api/analysis/evaluations')
    def evaluations(request: Request, ts_code: str, limit: str = '20'):
        _query(request, {'ts_code', 'limit'})
        bound = _limit(limit, 100)
        stock(ts_code)
        rows = read(app.state.evaluation_repository, 'list', ts_code, bound)
        try:
            if type(rows) is not list or len(rows) > bound:
                raise ValueError('Invalid list')
            for row in rows:
                validate_summary(row)
                if row['ts_code'] != ts_code:
                    raise ValueError('Stock binding mismatch')
        except Exception:
            raise HTTPException(503, 'Evaluation dependency unavailable') from None
        return rows

    @app.get('/api/analysis/evaluations/{ts_code}/{report_id}')
    def detail(request: Request, ts_code: str, report_id: str):
        _query(request, set())
        _validate(validate_report_id, report_id)
        stock(ts_code)
        report = read(app.state.evaluation_repository, 'get', ts_code, report_id)
        if report is None:
            raise HTTPException(404, 'Resource not found')
        try:
            validate_report(report)
        except Exception:
            raise HTTPException(503, 'Evaluation dependency unavailable') from None
        if report['ts_code'] != ts_code or report['report_id'] != report_id:
            raise HTTPException(404, 'Resource not found')
        return report
