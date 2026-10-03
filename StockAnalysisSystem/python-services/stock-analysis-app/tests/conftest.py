"""Pytest-only environment bootstrap for import-time configuration validation."""

import os


os.environ.setdefault('DB_PASSWORD', 'pytest-only-placeholder')
