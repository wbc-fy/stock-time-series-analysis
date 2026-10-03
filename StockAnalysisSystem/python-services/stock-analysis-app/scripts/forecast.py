"""Explicit offline train/predict publication. Never invoked by web GETs."""
import argparse
import sys
from analysis.forecast.artifacts import ArtifactStore
from analysis.forecast.contracts import validate_date, validate_model_id, validate_stock
from analysis.forecast.repository import ForecastRepository
from analysis.forecast.training import predict_latest, train_forecast
from analysis.forecast.continuity import load_calendar, validate_calendar


def main(argv=None):
    parser = argparse.ArgumentParser(description='Offline next-trading-day return forecasts')
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('train', 'predict'):
        command = commands.add_parser(name)
        command.add_argument('--stock', required=True)
        command.add_argument('--start', required=True)
        command.add_argument('--end', required=True)
        command.add_argument('--calendar', required=True, help='Trusted local tushare.trade_cal JSON; no automatic fetch')
        if name == 'predict':
            command.add_argument('--model', required=True)
    args = parser.parse_args(argv)
    try:
        validate_stock(args.stock)
        validate_date(args.start)
        validate_date(args.end)
        if args.start > args.end:
            raise ValueError('Invalid date range')
        if args.command == 'predict':
            validate_model_id(args.model)
        calendar = load_calendar(args.calendar)
        validate_calendar(calendar, args.stock)
        from database.db_connector import DatabaseConnector
        from config.settings import DATABASE_CONFIG
        config = dict(DATABASE_CONFIG, connect_timeout=5, pool_timeout=5, read_timeout=15, write_timeout=15)
        store = ArtifactStore()
        with DatabaseConnector(config) as connector:
            repository = ForecastRepository(connector, store)
            frame = repository.load_market(args.stock, args.start, args.end)
            if args.command == 'train':
                model, payload = train_forecast(frame, args.stock, calendar)
                store.save(model, payload)
            else:
                published = repository.get(args.stock, args.model)
                if published is None:
                    raise ValueError('Published model not found for stock')
                model, metadata = store.load(args.model)
                payload = predict_latest(model, metadata, frame, args.stock, calendar)
                if payload['data_cutoff'] < published['data_cutoff']:
                    raise ValueError('Publication cutoff regressed')
            repository.publish(payload)
        print('Published '+payload['model_id']+' for '+payload['ts_code'])
        return 0
    except Exception:
        # Never print connection exceptions, SQL parameters, or environment values.
        print('Forecast failed: check inputs, history, artifacts and database availability.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
