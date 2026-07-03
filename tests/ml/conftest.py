"""Shared fixtures/config for the ML property tests (Task 27).

Import path
-----------
The ML models are imported as a package (``import ml.models.segmentation``), so
the project root must be on ``sys.path``. There is no root-level pytest config,
so this conftest — the first ``conftest.py`` pytest loads for ``tests/ml`` — puts
the project root at the front of ``sys.path`` for every test in this directory.

No infrastructure required
--------------------------
These tests exercise the segmentation model as a pure function over an in-memory
DataFrame. They do **not** require PostgreSQL, MLflow, Docker, or a network — the
``no_mlflow`` fixture stubs the model's MLflow logging so an outage (or the
absence of a tracking server) can never affect or slow the run.
"""

from __future__ import annotations

import pathlib
import sys

import pytest

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture
def no_mlflow(monkeypatch):
    """Neutralise the segmentation model's MLflow logging.

    The design treats MLflow as best-effort infrastructure and the model already
    swallows MLflow errors, but stubbing ``_log_to_mlflow`` keeps the tests fast,
    deterministic, and guarantees they never touch a tracking server or write a
    local ``mlruns/`` directory — satisfying the "do not require MLflow" rule.
    """
    from ml.models import segmentation

    monkeypatch.setattr(segmentation, "_log_to_mlflow", lambda *a, **k: None)
    return segmentation
