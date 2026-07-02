# =============================================================================
# Customer Intelligence Platform — Airflow image
# -----------------------------------------------------------------------------
# The stock Airflow image ships without Great Expectations or a PostgreSQL
# driver, so the GE quality-gate checkpoints (Task 12) cannot execute on it.
# Layer the pinned dependencies GE needs on top of the pinned upstream image:
#   - great-expectations : runs the raw-table expectation suites / checkpoints
#   - SQLAlchemy         : GE's SqlAlchemyExecutionEngine (pinned to the 1.4.x
#                          release Airflow 2.7.3 uses, so GE does not upgrade it)
#   - psycopg2-binary    : PostgreSQL driver for the GE datasource connection
# =============================================================================
FROM apache/airflow:2.7.3-python3.11

RUN pip install --no-cache-dir \
    "great-expectations==0.18.19" \
    "SQLAlchemy==1.4.49" \
    "psycopg2-binary==2.9.9"
