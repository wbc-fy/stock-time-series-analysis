"""Explicit offline retrospective evaluation; import and help never connect."""
import argparse
import logging
import sys
from analysis.forecast.contracts import validate_date, validate_stock
from analysis.forecast.continuity import load_calendar, validate_calendar
from analysis.forecast.repository import ForecastRepository
from analysis.evaluation.repository import EvaluationRepository
from analysis.evaluation.training import evaluate


def main(argv=None):
    parser = argparse.ArgumentParser(description='Offline retrospective walk-forward evaluation')
    for name in ('stock', 'start', 'end', 'calendar'):
        parser.add_argument('--'+name, required=True)
    args = parser.parse_args(argv)
    previous_logging_disable = logging.root.manager.disable
    try:
        # This offline command has a deliberately minimal public output contract.
        # Drop records (rather than capturing them), including lazy import,
        # connector construction/closure and training diagnostics.
        logging.disable(sys.maxsize)
        validate_stock(args.stock)
        if args.stock != '000001.SZ':
            raise ValueError('Unsupported evaluation stock')
        validate_date(args.start); validate_date(args.end)
        if args.start > args.end:
            raise ValueError('Invalid date range')
        calendar = load_calendar(args.calendar)
        validate_calendar(calendar, args.stock)
        from database.db_connector import DatabaseConnector
        from config.settings import DATABASE_CONFIG
        config = dict(DATABASE_CONFIG, connect_timeout=5, pool_timeout=5, read_timeout=5, write_timeout=5)
        with DatabaseConnector(config) as connector:
            frame = ForecastRepository(connector).load_market(args.stock, args.start, args.end)
            report = evaluate(frame, args.stock, calendar)
            report_id = EvaluationRepository(connector).publish(report)
        print('Published '+report_id+' for '+args.stock)
        return 0
    except Exception:
        print('Evaluation failed: check inputs, history, reports and database availability.', file=sys.stderr)
        return 1
    finally:
        logging.disable(previous_logging_disable)


if __name__ == '__main__':
    sys.exit(main())
