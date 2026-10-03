"""One canonical public ID binds DTOs, files, SQL summaries and HTTP routes."""
import re
import pytest
from tests.test_evaluation_contracts import report


def test_generated_report_has_full_public_id(report):
    assert re.fullmatch(r'eval_[0-9a-f]{32}', report['report_id'])


def test_validator_accepts_full_id():
    from analysis.evaluation.contracts import validate_report_id
    value = 'eval_' + 'a' * 32
    assert validate_report_id(value) == value


@pytest.mark.parametrize('value', ['a' * 32, 'eval_' + 'A' * 32,
    'eval_eval_' + 'a' * 32, 'eval_' + 'a' * 31, '../eval_' + 'a' * 32])
def test_validator_rejects_noncanonical_id(value):
    from analysis.evaluation.contracts import validate_report_id
    with pytest.raises(ValueError):
        validate_report_id(value)


def test_directory_is_exact_report_id(tmp_path, report):
    from analysis.evaluation.reports import ReportStore
    store = ReportStore(tmp_path)
    summary = store.save(report)
    assert summary['report_id'] == report['report_id']
    assert (tmp_path / report['report_id'] / 'report.json').is_file()
    assert store.load(summary) == report


def test_api_uses_exact_full_public_id(report):
    from tests.test_evaluation_api import Evaluations, client
    full_id = 'eval_' + report['report_id'].removeprefix('eval_')
    payload = dict(report, report_id=full_id)
    repo = Evaluations(payload)
    with client(repo) as http:
        response = http.get('/api/analysis/evaluations/000001.SZ/' + full_id)
        assert response.status_code == 200
        assert response.json()['report_id'] == full_id
        assert repo.calls[-1] == ('get', '000001.SZ', full_id)
        assert http.get('/api/analysis/evaluations/000001.SZ/' + full_id[5:]).status_code == 400
