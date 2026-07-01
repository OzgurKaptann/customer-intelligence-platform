"""Airflow shared utilities for the Customer Intelligence Platform.

This package holds lightweight, import-safe helpers used across the platform's
DAGs (Task 6). Modules here MUST NOT open database connections, contact MLflow,
or import heavy optional dependencies (great_expectations, mlflow) at import
time — every such side effect is deferred to the point of use so that Airflow's
DAG parser can import these modules cheaply and safely.

Configuration is read exclusively from environment variables; no hosts, ports,
or credentials are hard-coded.
"""

__all__ = [
    "db",
    "dbt_runner",
    "ge_runner",
    "logging",
    "mlflow_utils",
    "sla",
]
