"""Immutable, bounded report files under a controlled, non-link root."""
import hashlib
import json
import os
from pathlib import Path
import stat
import uuid
from .contracts import validate_report, validate_summary, parse_report, make_summary

MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_SUMMARY_BYTES = 65535


def encode_summary(summary):
    raw = json.dumps(validate_summary(summary), ensure_ascii=False,
                     allow_nan=False, separators=(',', ':')).encode('utf-8')
    if len(raw) > MAX_SUMMARY_BYTES:
        raise ValueError('Evaluation summary exceeds TEXT byte bound')
    return raw


def _safe_path(path):
    # lstat (not stat) is essential: Windows junctions are reparse points too.
    for candidate in (*reversed(path.parents), path):
        try:
            info = candidate.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT:
            raise ValueError('Evaluation path is not a controlled directory')


class ReportStore:
    def __init__(self, root=None):
        default = Path(__file__).absolute().parents[2] / 'reports' / 'v08'
        self.root = Path(os.path.abspath(root if root is not None else default))

    def _check(self, path):
        _safe_path(self.root)
        _safe_path(path)
        if not path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError('Evaluation path escapes report root')

    def save(self, report):
        raw = json.dumps(validate_report(report), ensure_ascii=False,
                         allow_nan=False, separators=(',', ':')).encode('utf-8')
        if len(raw) > MAX_REPORT_BYTES:
            raise ValueError('Evaluation report exceeds size bound')
        summary = make_summary(report, hashlib.sha256(raw).hexdigest(), len(raw))
        encode_summary(summary)
        target = self.root / report['report_id']
        self._check(target)
        if target.exists():
            raise ValueError('Evaluation report already exists')
        self.root.mkdir(parents=True, exist_ok=True)
        self._check(target)
        temporary = self.root / ('.pending_' + uuid.uuid4().hex)
        temporary.mkdir()
        path = temporary / 'report.json'
        self._check(path)
        # Failed writes remain unadvertised; never clean old reports or DB orphans.
        with path.open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self._check(path)
        self._check(target)
        if target.exists():
            raise ValueError('Evaluation report already exists')
        # Windows rename refuses an existing directory, preserving immutable IDs.
        try:
            temporary.rename(target)
        except FileExistsError:
            raise ValueError('Evaluation report already exists') from None
        return summary

    def load(self, summary):
        validate_summary(summary)
        path = self.root / summary['report_id'] / 'report.json'
        try:
            self._check(path)
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REPORT_BYTES:
                raise ValueError('Invalid evaluation report size or file')
            with path.open('rb') as stream:
                opened = os.fstat(stream.fileno())
                if opened.st_size > MAX_REPORT_BYTES or (info.st_dev, info.st_ino) != (opened.st_dev, opened.st_ino):
                    raise ValueError('Evaluation report changed before read')
                raw = stream.read(MAX_REPORT_BYTES + 1)
                final = os.fstat(stream.fileno())
            self._check(path)
            after = path.lstat()
            if (len(raw) > MAX_REPORT_BYTES or final.st_size != len(raw)
                    or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
                    != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)):
                raise ValueError('Evaluation report changed during read')
            digest = hashlib.sha256(raw).hexdigest()
            if len(raw) != summary['report_bytes'] or digest != summary['report_sha256']:
                raise ValueError('Evaluation report integrity mismatch')
            report = parse_report(raw)
            if make_summary(report, digest, len(raw)) != summary:
                raise ValueError('Evaluation report summary mismatch')
            return report
        except OSError:
            raise ValueError('Evaluation report unavailable') from None
