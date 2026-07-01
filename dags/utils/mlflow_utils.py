"""MLflow model-registry helpers.

Thin wrappers over ``mlflow.tracking.MlflowClient`` for registering, promoting,
and comparing models in the registry (Requirements 5.7, 6.4, 7.5). MLflow is
imported lazily so this module is import-safe for Airflow's DAG parser, and the
tracking URI is read from the ``MLFLOW_TRACKING_URI`` environment variable.

The ``production`` stage is used as the promotion target throughout the platform.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

_logger = logging.getLogger("cip.pipeline")

PRODUCTION_STAGE = "production"


def _get_client():
    """Construct an ``MlflowClient`` bound to the configured tracking URI.

    Raises:
        RuntimeError: if ``MLFLOW_TRACKING_URI`` is not set.
    """
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError(
            "MLFLOW_TRACKING_URI environment variable is not set; cannot reach "
            "the MLflow registry."
        )

    from mlflow.tracking import MlflowClient  # Lazy import: optional dependency.

    return MlflowClient(tracking_uri=tracking_uri)


def register_model(
    name: str, run_id: str, metrics: Optional[Dict[str, float]] = None
) -> Optional[str]:
    """Register a run's model artifact under ``name`` in the MLflow registry.

    Args:
        name: registered model name (e.g. ``"customer_segmentation"``).
        run_id: MLflow run id whose ``model`` artifact should be registered.
        metrics: optional metric map stored as tags on the created version for
            later comparison/promotion decisions.

    Returns:
        The new model version string, or ``None`` if registration was skipped
        because MLflow was unreachable (scoring continues regardless per the
        error-handling design).
    """
    try:
        client = _get_client()
        # Ensure the registered model exists before creating a version.
        try:
            client.create_registered_model(name)
        except Exception:
            # Already exists — expected on every run after the first.
            pass

        model_uri = f"runs:/{run_id}/model"
        version = client.create_model_version(
            name=name, source=model_uri, run_id=run_id
        )
        for metric_name, metric_value in (metrics or {}).items():
            client.set_model_version_tag(
                name=name,
                version=version.version,
                key=f"metric_{metric_name}",
                value=str(metric_value),
            )
        _logger.info(
            "Registered model %s version %s (run_id=%s)",
            name,
            version.version,
            run_id,
        )
        return version.version
    except Exception:  # noqa: BLE001 — MLflow outage must not fail scoring.
        _logger.warning(
            "MLflow unavailable; skipped registration of model %s (run_id=%s)",
            name,
            run_id,
            exc_info=True,
        )
        return None


def promote_model(name: str, version: str) -> bool:
    """Promote ``version`` of ``name`` to the ``production`` stage.

    Other production versions are archived so exactly one version holds the
    production tag.

    Returns:
        ``True`` on success, ``False`` if MLflow was unreachable.
    """
    try:
        client = _get_client()
        client.transition_model_version_stage(
            name=name,
            version=version,
            stage=PRODUCTION_STAGE,
            archive_existing_versions=True,
        )
        _logger.info("Promoted model %s version %s to production", name, version)
        return True
    except Exception:  # noqa: BLE001
        _logger.warning(
            "MLflow unavailable; skipped promotion of model %s version %s",
            name,
            version,
            exc_info=True,
        )
        return False


def get_production_model_metrics(name: str) -> Optional[Dict[str, float]]:
    """Return the metrics of the current production version of ``name``.

    Metrics are read from the run associated with the production model version.

    Returns:
        A metric-name → value map, an empty dict if no production version
        exists, or ``None`` if MLflow was unreachable.
    """
    try:
        client = _get_client()
        versions = client.get_latest_versions(name, stages=[PRODUCTION_STAGE])
        if not versions:
            _logger.info("No production version registered for model %s", name)
            return {}

        run_id = versions[0].run_id
        run = client.get_run(run_id)
        return dict(run.data.metrics)
    except Exception:  # noqa: BLE001
        _logger.warning(
            "MLflow unavailable; could not read production metrics for model %s",
            name,
            exc_info=True,
        )
        return None
