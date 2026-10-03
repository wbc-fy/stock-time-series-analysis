import copy
import os
import subprocess
import stat
from pathlib import Path
from types import SimpleNamespace
import pytest


@pytest.fixture(scope='module')
def report():
    from analysis.evaluation.training import evaluate
    from tests.test_evaluation_protocol import history
    frame, calendar = history()
    return evaluate(frame, '000001.SZ', calendar)


def test_roundtrip_no_overwrite(tmp_path, report):
    from analysis.evaluation.reports import ReportStore
    store = ReportStore(tmp_path / 'reports')
    summary = store.save(report)
    assert 'series' not in summary and 'source_continuity' not in summary
    assert store.load(summary) == report
    original = (store.root / report['report_id'] / 'report.json').read_bytes()
    with pytest.raises(ValueError):
        store.save(report)
    assert (store.root / report['report_id'] / 'report.json').read_bytes() == original


@pytest.mark.parametrize('change', ['missing', 'bytes', 'hash', 'stock', 'projection', 'tamper', 'large', 'duplicate', 'nonfinite'])
def test_load_fails_closed(tmp_path, report, change):
    from analysis.evaluation.reports import ReportStore
    store = ReportStore(tmp_path)
    summary = copy.deepcopy(store.save(report))
    path = tmp_path / report['report_id'] / 'report.json'
    if change == 'missing': path.unlink()
    if change == 'bytes': summary['report_bytes'] += 1
    if change == 'hash': summary['report_sha256'] = '0'*64
    if change == 'stock': summary['ts_code'] = '600000.SH'
    if change == 'projection': summary['created_at'] = '2020-01-01T00:00:00+00:00'
    if change == 'tamper': path.write_bytes(path.read_bytes()+b' ')
    if change == 'large': path.write_bytes(b' '*(2097152+1))
    if change in ('duplicate', 'nonfinite'):
        import hashlib
        raw = path.read_bytes()
        raw = b'{"schema_version":1,'+raw[1:] if change == 'duplicate' else raw.replace(b'"schema_version":1', b'"schema_version":NaN')
        path.write_bytes(raw)
        summary['report_bytes'] = len(raw)
        summary['report_sha256'] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError): store.load(summary)


@pytest.mark.parametrize('failure', ['fsync', 'rename'])
def test_failed_file_operation_never_exposes_report(tmp_path, report, monkeypatch, failure):
    from analysis.evaluation import reports
    def broken(*args, **kwargs): raise OSError('private disk detail')
    if failure == 'fsync': monkeypatch.setattr(reports.os, 'fsync', broken)
    else: monkeypatch.setattr(Path, 'rename', broken)
    with pytest.raises((OSError, ValueError)): reports.ReportStore(tmp_path).save(report)
    assert not (tmp_path / report['report_id']).exists()


def test_reparse_root_and_escape_rejected(tmp_path, report, monkeypatch):
    from analysis.evaluation.reports import ReportStore
    original = Path.lstat
    def reparse(path, *args, **kwargs):
        result = original(path, *args, **kwargs)
        if path == tmp_path:
            return SimpleNamespace(st_mode=result.st_mode, st_file_attributes=stat.FILE_ATTRIBUTE_REPARSE_POINT)
        return result
    monkeypatch.setattr(Path, 'lstat', reparse)
    with pytest.raises(ValueError): ReportStore(tmp_path).save(report)


def test_symlink_ancestor_rejected(tmp_path, report, monkeypatch):
    from analysis.evaluation.reports import ReportStore
    target = tmp_path / 'target'; target.mkdir()
    link = tmp_path / 'link'
    try: link.symlink_to(target, target_is_directory=True)
    except OSError:
        # Native symlinks need an unavailable privilege on this Windows host.
        link.mkdir()
        original = Path.lstat
        def simulated(path, *args, **kwargs):
            result = original(path, *args, **kwargs)
            return SimpleNamespace(st_mode=stat.S_IFLNK) if path == link else result
        monkeypatch.setattr(Path, 'lstat', simulated)
    with pytest.raises(ValueError): ReportStore(link / 'reports').save(report)


def test_native_windows_junction_rejected(tmp_path, report):
    from analysis.evaluation.reports import ReportStore
    assert os.name == 'nt', 'This acceptance test exercises the Windows deployment'
    target = tmp_path / 'junction-target'; target.mkdir()
    link = tmp_path / 'junction'
    assert link.absolute().is_relative_to(tmp_path.absolute())
    quoted_link = str(link).replace("'", "''")
    quoted_target = str(target).replace("'", "''")
    created = subprocess.run(['powershell','-NoProfile','-NonInteractive','-Command',
        "New-Item -ItemType Junction -Path '"+quoted_link+"' -Target '"+quoted_target+"' -ErrorAction Stop | Out-Null"],
        capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    assert created.returncode == 0, 'Native junction fixture creation failed'
    try:
        with pytest.raises(ValueError): ReportStore(link / 'reports').save(report)
    finally:
        removed = subprocess.run(['powershell','-NoProfile','-NonInteractive','-Command',
            "Remove-Item -LiteralPath '"+quoted_link+"' -Force -ErrorAction Stop"],
            capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
        assert removed.returncode == 0, 'Native junction fixture cleanup failed'


def test_encode_size_bound_before_files(tmp_path, report, monkeypatch):
    from analysis.evaluation import reports
    oversized = dict(report, dependency_versions=dict(report['dependency_versions'], numpy='界'*800000))
    monkeypatch.setattr(reports, 'validate_report', lambda value: value)
    with pytest.raises(ValueError, match='size bound'): reports.ReportStore(tmp_path / 'reports').save(oversized)
    assert not (tmp_path / 'reports').exists()


def test_summary_limit_before_directory_creation(tmp_path, report, monkeypatch):
    from analysis.evaluation import reports
    monkeypatch.setattr(reports, 'make_summary', lambda *args: {'note': '界'*22000})
    monkeypatch.setattr(reports, 'validate_summary', lambda value: value)
    with pytest.raises(ValueError, match='byte bound'): reports.ReportStore(tmp_path / 'reports').save(report)
    assert not (tmp_path / 'reports').exists()


def test_resolved_escape_rejected(tmp_path, report, monkeypatch):
    from analysis.evaluation.reports import ReportStore
    root = tmp_path / 'reports'
    original = Path.resolve
    def escape(path, *args, **kwargs):
        if path.name.startswith('eval_'): return tmp_path / 'outside'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(Path, 'resolve', escape)
    with pytest.raises(ValueError, match='escapes'): ReportStore(root).save(report)


def test_load_only_bounded_read(tmp_path, report, monkeypatch):
    from analysis.evaluation.reports import ReportStore, MAX_REPORT_BYTES
    store = ReportStore(tmp_path)
    summary = store.save(report)
    original = Path.open
    sizes = []
    class Reader:
        def __init__(self, file): self.file = file
        def __enter__(self): return self
        def __exit__(self, *args): self.file.close()
        def fileno(self): return self.file.fileno()
        def read(self, size):
            sizes.append(size)
            return self.file.read(size)
    def bounded(path, mode='r', *args, **kwargs):
        file = original(path, mode, *args, **kwargs)
        return Reader(file) if mode == 'rb' else file
    monkeypatch.setattr(Path, 'open', bounded)
    assert store.load(summary) == report
    assert sizes == [MAX_REPORT_BYTES+1]
