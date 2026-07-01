# =============================================================================
# Customer Intelligence Platform — MLflow tracking server image
# -----------------------------------------------------------------------------
# The upstream MLflow image ships without a PostgreSQL driver, so pointing
# --backend-store-uri at Postgres fails at startup. Layer psycopg2-binary on top
# of the pinned upstream image so the server can reach the `mlflow` database.
# =============================================================================
FROM ghcr.io/mlflow/mlflow:v2.9.2

RUN pip install --no-cache-dir psycopg2-binary==2.9.9
