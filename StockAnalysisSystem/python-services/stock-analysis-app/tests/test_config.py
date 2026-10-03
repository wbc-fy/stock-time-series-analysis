# -*- coding: utf-8 -*-
"""
配置模块测试

覆盖：
    - 缺少环境变量时能否正确提示
    - 敏感信息脱敏
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestEnvironmentValidation:
    """环境变量校验测试。"""

    def test_pytest_bootstrap_keeps_missing_variable_validation_testable(self):
        """测试进程应自行满足模块前置条件，再验证其他变量缺失。"""
        assert os.getenv('DB_PASSWORD')
        from config.settings import _require_env
        with pytest.raises(EnvironmentError):
            _require_env('NONEXISTENT_ENV_VAR_FOR_TEST_12345')

    def test_require_env_returns_value_when_present(self, monkeypatch):
        """_require_env 对已设变量应返回其值。"""
        monkeypatch.setenv('TEST_VAR_XYZ', 'hello')
        from config.settings import _require_env
        assert _require_env('TEST_VAR_XYZ') == 'hello'


class TestSecretMasking:
    """敏感信息脱敏测试。"""

    def test_mask_short_value(self):
        from config.settings import mask_secret
        assert mask_secret('ab', 4) == '****'

    def test_mask_long_value(self):
        from config.settings import mask_secret
        result = mask_secret('abcdefgh1234567890', 4)
        assert result == 'abcd' + '*' * 14

    def test_mask_empty_value(self):
        from config.settings import mask_secret
        assert mask_secret('') == '****'
        assert mask_secret(None) == '****'


class TestModelConfig:
    """模型配置完整性测试。"""

    def test_random_seed_exists(self):
        from config.settings import RANDOM_SEED, MODEL_CONFIG
        assert isinstance(RANDOM_SEED, int)
        assert MODEL_CONFIG['random_seed'] == RANDOM_SEED

    def test_xgboost_has_seed(self):
        from config.settings import MODEL_CONFIG
        assert 'random_state' in MODEL_CONFIG['xgboost_params']

    def test_train_ratios_sum_to_one(self):
        from config.settings import MODEL_CONFIG
        total = MODEL_CONFIG['train_ratio'] + MODEL_CONFIG['val_ratio'] + MODEL_CONFIG['test_ratio']
        assert abs(total - 1.0) < 1e-6, f"比例之和 = {total}，应为 1.0"
