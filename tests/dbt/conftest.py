"""Fixtures for the dbt staging property tests (Task 16).

Isolation strategy
------------------
The staging deduplication property test runs the **real** ``stg_crm__customers``
model SQL against an isolated PostgreSQL database:

* If ``CIP_TEST_DATABASE_URL`` is set, that database is used as-is (it is assumed
  to be a throwaway/test database — the ``raw`` schema is dropped and recreated
  around the test).
* Otherwise a throwaway ``postgres:15.6-alpine`` container (the image pinned by
  the design) is started for the test session and removed on teardown.

If neither a test URL nor Docker is available, the test is skipped with a clear
message rather than touching a developer's database.

Running the model without a dbt runtime
---------------------------------------
Rather than reimplementing the deduplication logic (which would let the test pass
even if the model regressed), the fixtures load the actual model file and render
its two Jinja constructs — ``{{ config(...) }}`` and ``{{ source('crm',
'customers') }}`` — so the model's own SQL is what executes. No dbt install, dbt
deps, or network access is required.
"""

from __future__ import annotations

import pathlib
import re
import subprocess
import time
import uuid

import psycopg2
import pytest

_PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
_MODEL_PATH = (
    _PROJECT_ROOT / "dbt" / "models" / "staging" / "crm" / "stg_crm__customers.sql"
)

# The raw source table the test populates with intentionally duplicated PKs.
# It is created WITHOUT a primary key on customer_id so that duplicate
# customer_id values (the whole point of the invariant) can be inserted — this
# models the append-only Raw Zone semantics (Requirement 1.7).
RAW_CUSTOMERS = "raw.customers"

_CONFIG_RE = re.compile(r"\{\{\s*config\([^}]*\)\s*\}\}")
_SOURCE_RE = re.compile(r"\{\{\s*source\([^}]*\)\s*\}\}")


def render_stg_crm_customers(source_relation: str = RAW_CUSTOMERS) -> str:
    """Render the real ``stg_crm__customers`` model into executable SQL.

    Strips the ``config()`` call and replaces the ``source()`` reference with a
    concrete relation, leaving the model's deduplication logic untouched.
    """
    sql = _MODEL_PATH.read_text(encoding="utf-8")
    sql = _CONFIG_RE.sub("", sql)
    sql = _SOURCE_RE.sub(source_relation, sql)
    return sql.strip()


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "version"], capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def _wait_until_ready(dsn: str, timeout: float = 60.0) -> None:
    """Block until ``dsn`` accepts connections or ``timeout`` elapses."""
    deadline = time.time() + timeout
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return
        except psycopg2.OperationalError as exc:  # server still starting
            last_error = exc
            time.sleep(1.0)
    raise RuntimeError(f"PostgreSQL did not become ready within {timeout}s: {last_error}")


def _published_host_port(container: str) -> str:
    """Return the ephemeral host port mapped to container port 5432."""
    out = subprocess.run(
        ["docker", "port", container, "5432"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    first = out.splitlines()[0]
    return first.rsplit(":", 1)[1]


@pytest.fixture(scope="session")
def pg_dsn():
    """Yield a DSN for an isolated test PostgreSQL database."""
    import os

    env_url = os.environ.get("CIP_TEST_DATABASE_URL")
    if env_url:
        _wait_until_ready(env_url)
        yield env_url
        return

    if not _docker_available():
        pytest.skip(
            "No test database available: set CIP_TEST_DATABASE_URL or install/start Docker."
        )

    container = f"cip-test-pg-{uuid.uuid4().hex[:8]}"
    try:
        subprocess.run(
            [
                "docker", "run", "-d", "--name", container,
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=cip_test",
                "-p", "127.0.0.1::5432",
                "postgres:15.6-alpine",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - env issue
        pytest.skip(f"Could not start test PostgreSQL container: {exc.stderr}")

    try:
        port = _published_host_port(container)
        dsn = f"postgresql://postgres:postgres@127.0.0.1:{port}/cip_test"
        _wait_until_ready(dsn)
        yield dsn
    finally:
        subprocess.run(["docker", "rm", "-f", container], capture_output=True)


@pytest.fixture
def raw_customers_conn(pg_dsn):
    """A psycopg2 connection with a fresh, PK-less ``raw.customers`` table.

    The ``raw`` schema is dropped before and after the test so nothing leaks
    between tests or persists past the run. ``customer_id`` intentionally has no
    primary key so the test can insert duplicate ids with differing
    ``_ingested_at`` values.
    """
    connection = psycopg2.connect(pg_dsn)
    connection.autocommit = True
    try:
        with connection.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE")
            cur.execute("CREATE SCHEMA raw")
            cur.execute(
                """
                CREATE TABLE raw.customers (
                    customer_id         VARCHAR(36)  NOT NULL,
                    name                VARCHAR(255) NOT NULL,
                    email               VARCHAR(255) NOT NULL,
                    acquisition_channel VARCHAR(50)  NOT NULL,
                    country_code        CHAR(2)      NOT NULL,
                    account_created_at  DATE         NOT NULL,
                    _ingested_at        TIMESTAMPTZ  NOT NULL,
                    _run_date           DATE         NOT NULL
                )
                """
            )
        yield connection
    finally:
        with connection.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE")
        connection.close()
