"""Immutable native-JSON artifacts; no pickle or caller-provided paths."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
import xgboost as xgb
from .contracts import validate_model_id, validate_payload, parse_payload

DEFAULT_ROOT = Path(__file__).resolve().parents[2] / 'models' / 'v07'


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()


class ArtifactStore:
    def __init__(self, root=DEFAULT_ROOT):
        self.root = Path(root).resolve()

    def _path(self, model_id):
        validate_model_id(model_id)
        path = self.root / model_id
        if path.is_symlink() or path.resolve().parent != self.root:
            raise ValueError('Unsafe artifact path')
        return path

    def save(self, model, payload):
        validate_payload(payload)
        target = self._path(payload['model_id'])
        if target.exists():
            raise ValueError('Immutable artifact already exists')
        if model.get_booster().feature_names != payload['feature_names']:
            raise ValueError('Model feature order mismatch')
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = Path(tempfile.mkdtemp(prefix='.pending-', dir=self.root))
        try:
            model.get_booster().set_attr(v07_metadata_hash=_digest(payload))
            model.save_model(temporary / 'model.json')
            with (temporary / 'metadata.json').open('x', encoding='utf-8') as file:
                json.dump(payload, file, allow_nan=False, sort_keys=True)
                file.flush()
                os.fsync(file.fileno())
            # UUID IDs are unique; nonempty target directories cannot be overwritten.
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
        return target

    def load(self, model_id):
        target = self._path(model_id)
        try:
            for name in ('model.json', 'metadata.json'):
                if (target / name).is_symlink():
                    raise ValueError('Unsafe artifact file')
            with (target / 'metadata.json').open(encoding='utf-8') as file:
                payload = parse_payload(file.read())
            if payload['model_id'] != model_id:
                raise ValueError('Artifact identity mismatch')
            model = xgb.XGBRegressor()
            model.load_model(target / 'model.json')
            if (model.get_booster().feature_names != payload['feature_names']
                    or model.get_booster().attr('v07_metadata_hash') != _digest(payload)):
                raise ValueError('Artifact metadata mismatch')
        except (OSError, json.JSONDecodeError, xgb.core.XGBoostError) as exc:
            raise ValueError('Artifact unavailable or damaged') from exc
        return model, payload
