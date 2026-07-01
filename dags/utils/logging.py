"""Structured JSON logging emitters for Airflow tasks.

These helpers emit one-line JSON records via the standard :mod:`logging`
module so that pipeline observability (Requirements 2.2, 2.3, 13.2) is uniform
and machine-parseable in the Airflow task logs.

Note: although this module is named ``logging``, Python 3 uses absolute imports,
so ``import logging`` below resolves to the standard library, not this file.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict

# Dedicated logger namespace so these structured records can be filtered or
# routed independently of generic task logging.
_logger = logging.getLogger("cip.pipeline")


def _coerce(value: Any) -> Any:
    """Make common Airflow context values JSON-serializable."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def _emit(event: str, level: int = logging.INFO, **fields: Any) -> Dict[str, Any]:
    """Serialize ``fields`` as a single JSON log line and return the record.

    Returning the record (as a dict) makes the emitters easy to assert on in
    tests without capturing log output.
    """
    record: Dict[str, Any] = {"event": event}
    record.update({key: _coerce(val) for key, val in fields.items()})
    # default=str guards against any unexpected non-serializable value rather
    # than raising inside a logging call.
    _logger.log(level, json.dumps(record, default=str))
    return record


def log_task_start(
    dag_id: str, task_id: str, run_date: Any, attempt: int
) -> Dict[str, Any]:
    """Emit a structured record marking the start of a task attempt."""
    return _emit(
        "task_start",
        dag_id=dag_id,
        task_id=task_id,
        run_date=run_date,
        attempt=attempt,
    )


def log_rows_loaded(
    dag_id: str,
    task_id: str,
    run_date: Any,
    rows: int,
    duration_seconds: float,
) -> Dict[str, Any]:
    """Emit a structured record capturing rows loaded and elapsed time."""
    return _emit(
        "rows_loaded",
        dag_id=dag_id,
        task_id=task_id,
        run_date=run_date,
        rows=rows,
        duration_seconds=duration_seconds,
    )


def log_task_end(
    dag_id: str,
    task_id: str,
    run_date: Any,
    status: str,
    duration_seconds: float,
) -> Dict[str, Any]:
    """Emit a structured record marking task completion.

    A ``status`` other than ``"success"`` is logged at WARNING so failures stand
    out in the Airflow task log.
    """
    level = logging.INFO if status == "success" else logging.WARNING
    return _emit(
        "task_end",
        level=level,
        dag_id=dag_id,
        task_id=task_id,
        run_date=run_date,
        status=status,
        duration_seconds=duration_seconds,
    )
