"""Great Expectations checkpoint runner and DQ-failure recorder.

``run_checkpoint`` executes a Great Expectations checkpoint through the GE
Python API and, on failure, parses the ``CheckpointResult`` and records each
failing expectation to ``observability.dq_failures`` (Requirements 4.2, 4.3,
4.6).

Great Expectations is imported lazily so that Airflow's DAG parser can import
this module without the (heavy, optional) GE dependency being installed.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date
from typing import Any, Dict, List, Optional

from . import db

_logger = logging.getLogger("cip.pipeline")

# Root of the Great Expectations project inside the Airflow containers.
DEFAULT_GE_CONTEXT_ROOT = os.environ.get(
    "GE_CONTEXT_ROOT_DIR", "/opt/airflow/great_expectations"
)


def write_dq_failure(
    *,
    run_date: Any,
    failure_type: str,
    source_domain: str,
    table_name: str,
    checkpoint_or_test_name: str,
    failing_column: Optional[str] = None,
    failing_expectation: Optional[str] = None,
    sample_failing_rows: Optional[Any] = None,
    conn: Any = None,
) -> None:
    """Insert one failure row into ``observability.dq_failures``.

    All values are bound as query parameters — no string interpolation. This is
    a lightweight, self-contained writer; a dedicated ``dq_writer`` module is
    introduced later (Task 50) for the broader DQ-logging surface.
    """
    sample_json = (
        json.dumps(sample_failing_rows) if sample_failing_rows is not None else None
    )
    db.execute(
        """
        INSERT INTO observability.dq_failures (
            run_date, failure_type, source_domain, table_name,
            checkpoint_or_test_name, failing_column, failing_expectation,
            sample_failing_rows
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        """,
        (
            run_date,
            failure_type,
            source_domain,
            table_name,
            checkpoint_or_test_name,
            failing_column,
            failing_expectation,
            sample_json,
        ),
        conn=conn,
    )


def _parse_failed_expectations(checkpoint_result: Any) -> List[Dict[str, Any]]:
    """Extract failing expectations from a GE ``CheckpointResult``.

    Returns a list of dicts with ``table_name``, ``failing_column`` and
    ``failing_expectation`` keys. Written defensively so that minor GE version
    differences in the result shape do not raise.
    """
    failures: List[Dict[str, Any]] = []

    try:
        validation_results = checkpoint_result.list_validation_results()
    except AttributeError:
        # Fall back to the raw mapping form of the result object.
        run_results = getattr(checkpoint_result, "run_results", {}) or {}
        validation_results = [
            v.get("validation_result", {}) for v in run_results.values()
        ]

    for validation in validation_results:
        meta = _get(validation, "meta", {}) or {}
        batch_kwargs = _get(meta, "batch_kwargs", {}) or {}
        table_name = (
            _get(batch_kwargs, "table", None)
            or _get(meta, "table_name", None)
            or "unknown"
        )
        for expectation in _get(validation, "results", []) or []:
            if _get(expectation, "success", True):
                continue
            config = _get(expectation, "expectation_config", {}) or {}
            kwargs = _get(config, "kwargs", {}) or {}
            failures.append(
                {
                    "table_name": table_name,
                    "failing_column": _get(kwargs, "column", None),
                    "failing_expectation": _get(config, "expectation_type", "unknown"),
                }
            )
    return failures


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from either a mapping or an attribute-bearing object."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def run_checkpoint(
    suite_name: str,
    datasource_name: str,
    *,
    run_date: Optional[Any] = None,
    source_domain: Optional[str] = None,
    context_root_dir: str = DEFAULT_GE_CONTEXT_ROOT,
) -> bool:
    """Run a GE checkpoint; record any failing expectations.

    Args:
        suite_name: expectation suite / checkpoint name (e.g.
            ``"raw_customers_suite"``).
        datasource_name: GE datasource to validate against.
        run_date: pipeline run date recorded on any failure row. Defaults to
            today (UTC-naive) if not supplied.
        source_domain: logical source domain (e.g. ``"crm"``) recorded on
            failures. Defaults to ``datasource_name`` when omitted.
        context_root_dir: root directory of the GE ``FileSystemDataContext``.

    Returns:
        ``True`` if the checkpoint passed, ``False`` otherwise.
    """
    import great_expectations as gx  # Lazy import: heavy, optional dependency.

    if run_date is None:
        run_date = date.today()
    if source_domain is None:
        source_domain = datasource_name

    context = gx.get_context(context_root_dir=context_root_dir)
    result = context.run_checkpoint(
        checkpoint_name=suite_name,
        batch_request={"datasource_name": datasource_name},
    )

    if _get(result, "success", False):
        _logger.info(
            json.dumps({"event": "ge_checkpoint_passed", "checkpoint": suite_name})
        )
        return True

    failures = _parse_failed_expectations(result)
    _logger.error(
        json.dumps(
            {
                "event": "ge_checkpoint_failed",
                "checkpoint": suite_name,
                "datasource": datasource_name,
                "failed_expectations": len(failures),
            }
        )
    )

    for failure in failures:
        try:
            write_dq_failure(
                run_date=run_date,
                failure_type="great_expectations",
                source_domain=source_domain,
                table_name=failure["table_name"],
                checkpoint_or_test_name=suite_name,
                failing_column=failure["failing_column"],
                failing_expectation=failure["failing_expectation"],
            )
        except Exception:  # noqa: BLE001 — logging a DQ failure must not mask it.
            _logger.exception(
                "Failed to persist dq_failure for checkpoint %s", suite_name
            )

    return False
