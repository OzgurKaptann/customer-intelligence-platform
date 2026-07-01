"""SLA-miss handling for Airflow DAGs.

``sla_miss_callback`` is wired to a DAG's ``sla_miss_callback`` argument. When
Airflow detects that the pipeline SLA (06:00 UTC deadline, Requirement 13.3)
has been breached, it invokes this callback, which records an ``sla_miss`` row
in ``observability.pipeline_run_log`` (Requirement 13.4).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any, Optional

from . import db

_logger = logging.getLogger("cip.pipeline")


def _extract_dag_id(context: Any) -> str:
    """Best-effort extraction of the DAG id from an Airflow SLA-miss context."""
    dag = _get(context, "dag")
    if dag is not None:
        dag_id = _get(dag, "dag_id")
        if dag_id:
            return dag_id
    return _get(context, "dag_id") or "unknown"


def _extract_run_date(context: Any) -> Any:
    """Derive the pipeline run date from the Airflow execution/logical date."""
    exec_date = (
        _get(context, "execution_date")
        or _get(context, "logical_date")
        or _get(context, "data_interval_start")
    )
    if isinstance(exec_date, datetime):
        return exec_date.date()
    if isinstance(exec_date, date):
        return exec_date
    return date.today()


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """Read ``key`` from a mapping-style Airflow context or a plain object."""
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def sla_miss_callback(context: Any, *args: Any, **kwargs: Any) -> None:
    """Record an ``sla_miss`` entry in ``observability.pipeline_run_log``.

    Airflow calls SLA-miss callbacks with varying signatures across versions;
    extra positional/keyword arguments are accepted and ignored for
    compatibility. Any error while writing the record is logged rather than
    raised, so SLA handling never crashes the scheduler.
    """
    dag_id = _extract_dag_id(context)
    run_date = _extract_run_date(context)
    breach_at = datetime.now(timezone.utc)

    try:
        db.execute(
            """
            INSERT INTO observability.pipeline_run_log (
                run_date, dag_name, status, started_at, sla_breach_at, notes
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_date,
                dag_id,
                "sla_miss",
                breach_at,
                breach_at,
                "SLA breached: pipeline did not reach success by the configured deadline.",
            ),
        )
        _logger.warning(
            "SLA miss recorded for dag=%s run_date=%s", dag_id, run_date
        )
    except Exception:  # noqa: BLE001 — callback must not raise into the scheduler.
        _logger.exception(
            "Failed to record SLA miss for dag=%s run_date=%s", dag_id, run_date
        )
