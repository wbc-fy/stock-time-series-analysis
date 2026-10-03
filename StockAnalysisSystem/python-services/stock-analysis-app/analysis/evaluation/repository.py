"""File-first publication with one INSERT transaction and bounded read-only queries."""
from sqlalchemy import text
from analysis.forecast.contracts import validate_stock
from analysis.forecast.repository import DependencyError
from .contracts import validate_report, validate_summary, validate_report_id, parse_summary
from .reports import ReportStore, encode_summary

ANALYSIS_TYPE = 'v08_walk_forward_evaluation'


class EvaluationRepository:
    def __init__(self, connector, store=None):
        self.connector = connector
        self.store = store if store is not None else ReportStore()

    def publish(self, report):
        validate_report(report)
        summary = self.store.save(report)
        validate_summary(summary)
        encoded = encode_summary(summary)
        try:
            with self.connector.session_scope() as session:
                session.execute(text('''INSERT INTO analysis_result
                    (ts_code, analysis_date, analysis_type, result, prediction, confidence)
                    VALUES (:stock, :date, :kind, :result, NULL, NULL)'''),
                    dict(stock=report['ts_code'], date=report['source']['signal_end'],
                         kind=ANALYSIS_TYPE, result=encoded.decode('utf-8')))
        except Exception:
            raise DependencyError('Evaluation database unavailable') from None
        return report['report_id']

    def _rows(self, ts_code, limit, report_id=None):
        validate_stock(ts_code)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError('Limit must be 1..100')
        where = 'analysis_type=:kind AND ts_code=:stock'
        params = dict(kind=ANALYSIS_TYPE, stock=ts_code, limit=limit)
        if report_id is not None:
            validate_report_id(report_id)
            where += " AND JSON_UNQUOTE(JSON_EXTRACT(result,'$.report_id'))=:report_id"
            params['report_id'] = report_id
        try:
            with self.connector.session_scope() as session:
                return session.execute(text('SELECT ts_code, result FROM analysis_result WHERE '
                    + where + ' ORDER BY id DESC LIMIT :limit'), params).mappings().all()
        except Exception:
            # Invalid JSON in historical rows also fails closed, with no fallback scan.
            raise DependencyError('Evaluation database unavailable') from None

    @staticmethod
    def _summary(row, ts_code):
        summary = parse_summary(row['result'])
        if row['ts_code'] != ts_code or summary['ts_code'] != ts_code:
            raise ValueError('Evaluation stock binding mismatch')
        return summary

    def list(self, ts_code, limit=20):
        valid = []
        for row in self._rows(ts_code, limit):
            try:
                valid.append(self._summary(row, ts_code))
            except (ValueError, TypeError, KeyError):
                continue
        return valid

    def get(self, ts_code, report_id):
        rows = self._rows(ts_code, 1, report_id)
        if not rows:
            return None
        try:
            summary = self._summary(rows[0], ts_code)
            if summary['report_id'] != report_id:
                raise ValueError('Evaluation ID binding mismatch')
            return self.store.load(summary)
        except Exception:
            raise DependencyError('Evaluation publication unavailable') from None
