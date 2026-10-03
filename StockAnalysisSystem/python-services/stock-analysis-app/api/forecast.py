"""Read-only V0.7 forecast API."""
import re
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from analysis.forecast.contracts import validate_stock, validate_model_id, validate_payload


def _production_repository():
    # Importing the app or injecting a test repository must not require secrets.
    from config.settings import DATABASE_CONFIG
    from database.db_connector import DatabaseConnector
    from analysis.forecast.repository import ForecastRepository
    config = dict(DATABASE_CONFIG, connect_timeout=5, read_timeout=5,
                  write_timeout=5, pool_timeout=5)
    return ForecastRepository(DatabaseConnector(config))


def _query(request, allowed):
    if any(key not in allowed or len(request.query_params.getlist(key)) != 1
           for key in request.query_params):
        raise HTTPException(400, 'Invalid request')


def _limit(value, maximum):
    if not re.fullmatch(r'[0-9]{1,3}', value) or not 1 <= int(value) <= maximum:
        raise HTTPException(400, 'Invalid request')
    return int(value)


def _validate(validator, value):
    try:
        return validator(value)
    except ValueError:
        raise HTTPException(400, 'Invalid request') from None


def _metadata(payload):
    return {k: v for k, v in payload.items()
            if k not in ('test_series', 'latest', 'feature_importance')}


def create_app(repository=None, evaluation_repository=None):
    @asynccontextmanager
    async def lifespan(app):
        if repository is None:
            try:
                app.state.repository = _production_repository()
            except Exception:
                app.state.repository = None
        if evaluation_repository is None and app.state.repository is not None:
            try:
                from analysis.evaluation.repository import EvaluationRepository
                app.state.evaluation_repository = EvaluationRepository(app.state.repository.connector)
            except Exception:
                app.state.evaluation_repository = None
        try:
            yield
        finally:
            if repository is None and app.state.repository is not None:
                app.state.repository.connector.close()

    app = FastAPI(lifespan=lifespan)
    app.state.repository = repository
    app.state.evaluation_repository = evaluation_repository

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse(status_code=400, content={'detail': 'Invalid request'})

    def read(method, *args):
        try:
            repo = app.state.repository
            if repo is None:
                raise RuntimeError('Dependency unavailable')
            return getattr(repo, method)(*args)
        except Exception:
            raise HTTPException(503, 'Forecast dependency unavailable') from None

    def stock(ts_code):
        _validate(validate_stock, ts_code)
        if not read('stock_exists', ts_code):
            raise HTTPException(404, 'Resource not found')

    def publication(method, *args):
        payload = read(method, *args)
        if payload is None:
            raise HTTPException(404, 'Resource not found')
        try:
            validate_payload(payload)
        except (ValueError, TypeError, RecursionError):
            raise HTTPException(503, 'Forecast dependency unavailable') from None
        return payload

    @app.get('/api/analysis/models')
    def models(request: Request, ts_code: str, limit: str = '20'):
        _query(request, {'ts_code', 'limit'})
        bound = _limit(limit, 100)
        stock(ts_code)
        return read('models', ts_code, bound)

    @app.get('/api/analysis/models/{model_id}/importance')
    def importance(request: Request, model_id: str, top: str = '15'):
        _query(request, {'top'})
        bound = _limit(top, 100)
        _validate(validate_model_id, model_id)
        payload = publication('get_model', model_id)
        return dict(model_id=model_id, ts_code=payload['ts_code'], importance_method=payload['importance_method'],
                    feature_importance=sorted(payload['feature_importance'], key=lambda x: x['importance'], reverse=True)[:bound])

    @app.get('/api/analysis/prediction/{ts_code}')
    def prediction(request: Request, ts_code: str, model: str, limit: str = '60'):
        _query(request, {'model', 'limit'})
        bound = _limit(limit, 500)
        _validate(validate_stock, ts_code)
        _validate(validate_model_id, model)
        stock(ts_code)
        payload = publication('get', ts_code, model)
        if payload['ts_code'] != ts_code or payload['model_id'] != model:
            raise HTTPException(404, 'Resource not found')
        return dict(metadata=_metadata(payload), test_series=payload['test_series'][-bound:], latest=payload['latest'])

    @app.get('/api/analysis/results/{ts_code}')
    def results(request: Request, ts_code: str, limit: str = '20'):
        _query(request, {'limit'})
        bound = _limit(limit, 100)
        stock(ts_code)
        return read('results', ts_code, bound)

    @app.get('/health')
    def health():
        try:
            if read('health') is not True:
                raise HTTPException(503)
        except HTTPException:
            return JSONResponse(status_code=503, content={'status': 'DOWN', 'database': 'DOWN'})
        return {'status': 'UP', 'database': 'UP'}

    from api.evaluations import register_evaluation_routes
    register_evaluation_routes(app)
    return app


app = create_app()
