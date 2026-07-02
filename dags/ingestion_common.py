"""Shared factory for the five source-domain ingestion DAGs (Task 11).

Every ingestion DAG (CRM, Events, Orders, Campaigns, Tickets) has an identical
shape — only the source domain, target ``raw`` table, and generator upsert
function differ — so the common structure lives here and each
``dags/ingest_*.py`` file is a thin one-line call to
:func:`build_ingestion_dag`.

Per the design (Airflow DAG Design → Retry and SLA Configuration) every DAG:

* runs daily at ``0 2 * * *`` (02:00 UTC) with ``catchup=False``;
* retries a failed task 3 times with a 5-minute delay
  (``execution_timeout`` = 45 min);
* logs task start, then loads only its own domain, then logs end with the row
  count (Requirements 2.2, 2.3);
* writes ``running`` → ``success`` state to ``observability.pipeline_run_log``
  around the load, and ``failed`` via the DAG ``on_failure_callback``
  (Requirements 2.4, 13.1, 13.2);
* stops downstream tasks on failure via ``trigger_rule=ALL_SUCCESS`` (default).

Design guard-rails honoured here:

* Generator modules (``customers``, ``orders`` …) and ``psycopg2``/``faker`` are
  imported **lazily inside the task callables**, never at DAG-parse time, so the
  Airflow scheduler can import this module cheaply and safely.
* All SQL is parameterised through :mod:`utils.db`; no value is string-formatted
  into a query (NFR-6.4).
"""

from __future__ import annotations

import functools
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from utils import db
from utils import logging as cip_logging

# Structured logger shared with the rest of the pipeline (utils.logging emits on
# the same namespace).
_logger = logging.getLogger("cip.pipeline")

# A fixed, historical start_date keeps scheduling deterministic; catchup=False
# means no backfill runs are created for the gap.
_START_DATE = datetime(2024, 1, 1)

# Retry/SLA configuration from the design's "Retry and SLA Configuration" table.
_DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "cip",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


# ---------------------------------------------------------------------------
# Generator import plumbing
# ---------------------------------------------------------------------------
def _ensure_generator_on_path() -> None:
    """Make the flat ``generator/`` modules importable from the DAG runtime.

    The generator package uses sibling-style imports (``import customers``), so
    its directory must sit directly on ``sys.path``. Candidate locations are
    tried in order: an explicit ``CIP_GENERATOR_PATH`` override, the conventional
    container mount ``/opt/airflow/generator``, and the repo-relative
    ``../generator`` (dags/ and generator/ are siblings).
    """
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.environ.get("CIP_GENERATOR_PATH"),
        "/opt/airflow/generator",
        os.path.join(os.path.dirname(here), "generator"),
    ]
    for path in candidates:
        if path and os.path.isdir(path) and path not in sys.path:
            sys.path.insert(0, path)


def _fetch_customer_ids(conn) -> list[str]:
    """Return every ``customer_id`` from ``raw.customers`` for FK-bearing loads.

    Orders, Events, and Tickets reference customers, so their upserts need the
    existing customer population to preserve referential integrity
    (Requirement 1.6). ``raw.customers`` is populated by the initial data-seed
    and refreshed daily by ``ingest_crm``.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT customer_id FROM raw.customers")
        rows = cur.fetchall()
    ids = [r[0] for r in rows]
    if not ids:
        raise RuntimeError(
            "raw.customers is empty — run ingest_crm (or the data-generator) "
            "before ingesting a customer-referencing domain."
        )
    return ids


# ---------------------------------------------------------------------------
# Per-domain loaders — each ensures its tables, upserts, and returns a row count
# ---------------------------------------------------------------------------
def _load_crm(conn, run_date: date) -> int:
    _ensure_generator_on_path()
    import customers

    customers.ensure_tables(conn)
    n = _env_int("SEED_CUSTOMERS", 100_000)
    ids = customers.generate_customers(conn, n, run_date=run_date)
    return len(ids)


def _load_orders(conn, run_date: date) -> int:
    _ensure_generator_on_path()
    import orders

    orders.ensure_tables(conn)
    n = _env_int("SEED_ORDERS", 250_000)
    customer_ids = _fetch_customer_ids(conn)
    order_count, item_count = orders.generate_orders(
        conn, customer_ids, n, run_date=run_date
    )
    # Report every row written to the Raw Zone (orders + their line items).
    return order_count + item_count


def _load_events(conn, run_date: date) -> int:
    _ensure_generator_on_path()
    import events

    events.ensure_tables(conn)
    n = _env_int("SEED_EVENTS", 1_000_000)
    customer_ids = _fetch_customer_ids(conn)
    return events.generate_events(conn, customer_ids, n=n, run_date=run_date)


def _load_campaigns(conn, run_date: date) -> int:
    _ensure_generator_on_path()
    import campaigns

    campaigns.ensure_tables(conn)
    n = _env_int("SEED_CAMPAIGNS", 1_000)
    return campaigns.generate_campaigns(conn, n=n, run_date=run_date)


def _load_tickets(conn, run_date: date) -> int:
    _ensure_generator_on_path()
    import tickets

    tickets.ensure_tables(conn)
    n = _env_int("SEED_TICKETS", 50_000)
    customer_ids = _fetch_customer_ids(conn)
    return tickets.generate_tickets(conn, customer_ids, n, run_date=run_date)


# Registry mapping a domain to its DAG id, target table, and loader.
_DOMAINS: Dict[str, Dict[str, Any]] = {
    "crm": {"dag_id": "ingest_crm", "table": "raw.customers", "loader": _load_crm},
    "events": {"dag_id": "ingest_events", "table": "raw.events", "loader": _load_events},
    "orders": {"dag_id": "ingest_orders", "table": "raw.orders", "loader": _load_orders},
    "campaigns": {
        "dag_id": "ingest_campaigns",
        "table": "raw.campaigns",
        "loader": _load_campaigns,
    },
    "tickets": {"dag_id": "ingest_tickets", "table": "raw.tickets", "loader": _load_tickets},
}


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _env_int(name: str, default: int) -> int:
    """Read a non-negative integer from the environment, else ``default``."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = int(raw)
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def _run_date_from_context(context: Dict[str, Any]) -> date:
    """Derive the pipeline run date from the Airflow task context."""
    ds = context.get("ds")
    if ds:
        return datetime.strptime(ds, "%Y-%m-%d").date()
    logical = context.get("logical_date") or context.get("execution_date")
    if isinstance(logical, datetime):
        return logical.date()
    return datetime.now(timezone.utc).date()


# ---------------------------------------------------------------------------
# pipeline_run_log writers (Requirements 13.1, 13.2, 2.4)
# ---------------------------------------------------------------------------
def _record_run_start(dag_id: str, run_date: date, started_at: datetime) -> int:
    """Insert a ``running`` row and return its ``log_id``."""
    row = db.execute(
        """
        INSERT INTO observability.pipeline_run_log
            (run_date, dag_name, status, started_at)
        VALUES (%s, %s, %s, %s)
        RETURNING log_id
        """,
        (run_date, dag_id, "running", started_at),
        fetch="one",
    )
    return row[0]


def _record_run_success(
    log_id: int, rows: int, duration_seconds: float, completed_at: datetime
) -> None:
    """Update the ``running`` row for ``log_id`` to ``success`` with metrics."""
    db.execute(
        """
        UPDATE observability.pipeline_run_log
        SET status = %s,
            completed_at = %s,
            duration_seconds = %s,
            rows_ingested = %s
        WHERE log_id = %s
        """,
        ("success", completed_at, int(duration_seconds), rows, log_id),
    )


def _mark_run_failed(dag_id: str, run_date: date) -> None:
    """Flip any in-flight ``running`` row to ``failed`` (else insert one)."""
    now = datetime.now(timezone.utc)
    updated = db.execute(
        """
        UPDATE observability.pipeline_run_log
        SET status = %s,
            completed_at = %s
        WHERE dag_name = %s AND run_date = %s AND status = %s
        """,
        ("failed", now, dag_id, run_date, "running"),
    )
    if not updated:
        db.execute(
            """
            INSERT INTO observability.pipeline_run_log
                (run_date, dag_name, status, started_at, completed_at, notes)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                run_date,
                dag_id,
                "failed",
                now,
                now,
                "Ingestion task failed after all retries.",
            ),
        )


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def _log_start(dag_id: str, domain: str, table: str, **context: Any) -> None:
    """Log the run start, source domain, and target table (Requirement 2.2)."""
    ti = context["ti"]
    run_date = _run_date_from_context(context)
    cip_logging.log_task_start(dag_id, ti.task_id, run_date, ti.try_number)
    _logger.info(
        "Ingestion started: domain=%s target_table=%s run_date=%s",
        domain,
        table,
        run_date,
    )


def _run_load(
    dag_id: str,
    domain: str,
    loader: Callable[[Any, date], int],
    **context: Any,
) -> int:
    """Load one domain: running → generate (single txn) → success.

    Returns the row count so the ``log_end`` task can report it via XCom.
    """
    ti = context["ti"]
    run_date = _run_date_from_context(context)
    started_at = datetime.now(timezone.utc)

    # Persist the running state up-front (its own committed transaction) so the
    # in-flight run is observable even if generation later fails.
    log_id = _record_run_start(dag_id, run_date, started_at)

    conn = db.get_connection()
    try:
        conn.autocommit = False
        rows = loader(conn, run_date)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
    _record_run_success(log_id, rows, duration_seconds, datetime.now(timezone.utc))
    cip_logging.log_rows_loaded(
        dag_id, ti.task_id, run_date, rows, duration_seconds
    )
    ti.xcom_push(key="duration_seconds", value=duration_seconds)
    return rows


def _log_end(dag_id: str, domain: str, load_task_id: str, **context: Any) -> None:
    """Log completion with the row count loaded (Requirement 2.3)."""
    ti = context["ti"]
    run_date = _run_date_from_context(context)
    rows = ti.xcom_pull(task_ids=load_task_id)
    rows = int(rows) if rows is not None else 0
    duration = ti.xcom_pull(task_ids=load_task_id, key="duration_seconds") or 0.0
    cip_logging.log_rows_loaded(dag_id, ti.task_id, run_date, rows, float(duration))
    cip_logging.log_task_end(dag_id, ti.task_id, run_date, "success", float(duration))


def _on_dag_failure(context: Any) -> None:
    """DAG ``on_failure_callback``: record ``failed`` in pipeline_run_log.

    Must never raise into the scheduler, so any error while writing is logged.
    """
    dag = context.get("dag") if isinstance(context, dict) else getattr(context, "dag", None)
    dag_id = getattr(dag, "dag_id", None) or "unknown"
    run_date = _run_date_from_context(context if isinstance(context, dict) else {})
    try:
        _mark_run_failed(dag_id, run_date)
        _logger.warning("Recorded failed run for dag=%s run_date=%s", dag_id, run_date)
    except Exception:  # noqa: BLE001 — callback must not crash the scheduler.
        _logger.exception(
            "Failed to record failed run for dag=%s run_date=%s", dag_id, run_date
        )


# ---------------------------------------------------------------------------
# DAG factory
# ---------------------------------------------------------------------------
def build_ingestion_dag(domain: str) -> DAG:
    """Build the ingestion DAG for ``domain`` (one of the five source domains).

    Args:
        domain: registry key — ``crm``, ``events``, ``orders``, ``campaigns``,
            or ``tickets``.

    Returns:
        A fully wired :class:`airflow.DAG` with ``log_start`` → ``load_{domain}``
        → ``log_end``.
    """
    if domain not in _DOMAINS:
        raise KeyError(
            f"Unknown ingestion domain {domain!r}; expected one of {sorted(_DOMAINS)}"
        )

    cfg = _DOMAINS[domain]
    dag_id = cfg["dag_id"]
    table = cfg["table"]
    loader = cfg["loader"]
    load_task_id = f"load_{domain}"

    dag = DAG(
        dag_id=dag_id,
        description=f"Daily idempotent upsert of the {domain} source domain into {table}.",
        schedule_interval="0 2 * * *",
        start_date=_START_DATE,
        catchup=False,
        max_active_runs=1,
        default_args=_DEFAULT_ARGS,
        on_failure_callback=_on_dag_failure,
        tags=["ingestion", domain],
    )

    with dag:
        log_start = PythonOperator(
            task_id="log_start",
            python_callable=functools.partial(_log_start, dag_id, domain, table),
        )
        load = PythonOperator(
            task_id=load_task_id,
            python_callable=functools.partial(_run_load, dag_id, domain, loader),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )
        log_end = PythonOperator(
            task_id="log_end",
            python_callable=functools.partial(
                _log_end, dag_id, domain, load_task_id
            ),
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        log_start >> load >> log_end

    return dag
