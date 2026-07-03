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
_MART_360_PATH = (
    _PROJECT_ROOT / "dbt" / "models" / "marts" / "mart_customer_360.sql"
)
_INTERMEDIATE_DIR = _PROJECT_ROOT / "dbt" / "models" / "intermediate"
_INT_SESSIONS_PATH = _INTERMEDIATE_DIR / "int_sessions__with_duration.sql"
_INT_ORDERS_ITEMS_PATH = _INTERMEDIATE_DIR / "int_orders__with_items.sql"
_INT_CUSTOMER_ORDERS_PATH = _INTERMEDIATE_DIR / "int_customer_orders__aggregated.sql"

# The schema the intermediate-layer property test (Task 23) materializes its
# input relations (stg_* / int_* dependencies) into. Isolated per the fixture.
INTERMEDIATE_SCHEMA = "cip_test_int"

# The raw source table the test populates with intentionally duplicated PKs.
# It is created WITHOUT a primary key on customer_id so that duplicate
# customer_id values (the whole point of the invariant) can be inserted — this
# models the append-only Raw Zone semantics (Requirement 1.7).
RAW_CUSTOMERS = "raw.customers"

_CONFIG_RE = re.compile(r"\{\{\s*config\([^}]*\)\s*\}\}")
_SOURCE_RE = re.compile(r"\{\{\s*source\([^}]*\)\s*\}\}")
_REF_RE = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
_AUDIT_RE = re.compile(r"\{\{\s*audit_columns\(\)\s*\}\}")
_RUN_DATE_RE = re.compile(r"\{\{\s*get_run_date\(\)\s*\}\}")

# The schema the mart_customer_360 property test materializes its input
# relations (int_* / stg_* dependencies) into. Isolated per the fixture below.
MART_360_SCHEMA = "cip_test_mart"


def render_stg_crm_customers(source_relation: str = RAW_CUSTOMERS) -> str:
    """Render the real ``stg_crm__customers`` model into executable SQL.

    Strips the ``config()`` call and replaces the ``source()`` reference with a
    concrete relation, leaving the model's deduplication logic untouched.
    """
    sql = _MODEL_PATH.read_text(encoding="utf-8")
    sql = _CONFIG_RE.sub("", sql)
    sql = _SOURCE_RE.sub(source_relation, sql)
    return sql.strip()


def render_mart_customer_360(
    run_date: str, schema: str = MART_360_SCHEMA
) -> str:
    """Render the real ``mart_customer_360`` model into executable SQL.

    The model's own RFM logic (the ``NTILE`` quintiles, the ``rfm_score`` string
    assembly, the ``recency_days`` cap, and the inactive-customer defaults) is
    left completely untouched — only the dbt Jinja glue is resolved:

    * ``config(...)`` is stripped.
    * every ``ref('...')`` is rewritten to ``<schema>.<model_name>`` so the
      test's input relations are used.
    * ``get_run_date()`` becomes a literal date cast and ``audit_columns()``
      becomes ``<date cast> as _run_date`` — matching the macros exactly.

    Running the real SQL (rather than reimplementing the scoring) means the test
    fails if the model's invariants ever regress.
    """
    date_expr = f"cast('{run_date}' as date)"
    sql = _MART_360_PATH.read_text(encoding="utf-8")
    sql = _CONFIG_RE.sub("", sql)
    sql = _REF_RE.sub(lambda m: f"{schema}.{m.group(1)}", sql)
    sql = _AUDIT_RE.sub(f"{date_expr} as _run_date", sql)
    sql = _RUN_DATE_RE.sub(date_expr, sql)
    return sql.strip()


def _render_intermediate_model(
    path: pathlib.Path, run_date: str, schema: str = INTERMEDIATE_SCHEMA
) -> str:
    """Render an intermediate-layer model file into executable SQL.

    The model's own derivation logic (session boundaries, item_count and
    avg_item_value_usd arithmetic, the trailing-window ``FILTER`` clauses) is left
    completely untouched — only the dbt Jinja glue is resolved, identically to
    ``render_mart_customer_360``:

    * ``config(...)`` is stripped.
    * every ``ref('...')`` is rewritten to ``<schema>.<model_name>`` so the
      test's input relations are used.
    * ``get_run_date()`` becomes a literal date cast and ``audit_columns()``
      becomes ``<date cast> as _run_date`` — matching the macros exactly.

    Running the real SQL (rather than reimplementing the derivations) means the
    test fails if a model's consistency invariants ever regress.
    """
    date_expr = f"cast('{run_date}' as date)"
    sql = path.read_text(encoding="utf-8")
    sql = _CONFIG_RE.sub("", sql)
    sql = _REF_RE.sub(lambda m: f"{schema}.{m.group(1)}", sql)
    sql = _AUDIT_RE.sub(f"{date_expr} as _run_date", sql)
    sql = _RUN_DATE_RE.sub(date_expr, sql)
    return sql.strip()


def render_int_sessions_with_duration(
    run_date: str, schema: str = INTERMEDIATE_SCHEMA
) -> str:
    """Render the real ``int_sessions__with_duration`` model into SQL."""
    return _render_intermediate_model(_INT_SESSIONS_PATH, run_date, schema)


def render_int_orders_with_items(
    run_date: str, schema: str = INTERMEDIATE_SCHEMA
) -> str:
    """Render the real ``int_orders__with_items`` model into SQL."""
    return _render_intermediate_model(_INT_ORDERS_ITEMS_PATH, run_date, schema)


def render_int_customer_orders_aggregated(
    run_date: str, schema: str = INTERMEDIATE_SCHEMA
) -> str:
    """Render the real ``int_customer_orders__aggregated`` model into SQL."""
    return _render_intermediate_model(_INT_CUSTOMER_ORDERS_PATH, run_date, schema)


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


@pytest.fixture
def mart_360_conn(pg_dsn):
    """A psycopg2 connection with empty ``mart_customer_360`` input relations.

    Creates the four ``int_customer_*`` aggregates, ``int_customers__enriched``,
    and ``stg_campaigns__campaigns`` — restricted to the columns the mart
    actually consumes — inside an isolated ``cip_test_mart`` schema. The schema
    is dropped before and after the test so nothing leaks between examples or
    persists past the run. Tables are truncated by the test between hypothesis
    examples.
    """
    connection = psycopg2.connect(pg_dsn)
    connection.autocommit = True
    schema = MART_360_SCHEMA
    try:
        with connection.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(
                f"""
                CREATE TABLE {schema}.int_customers__enriched (
                    customer_id          VARCHAR(36)  NOT NULL,
                    acquisition_channel  VARCHAR(50)  NOT NULL,
                    customer_tenure_days INTEGER      NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.int_customer_orders__aggregated (
                    customer_id          VARCHAR(36)   NOT NULL,
                    total_order_count    INTEGER,
                    total_spend_usd      NUMERIC(14, 2),
                    order_frequency_365d INTEGER,
                    total_spend_365d_usd NUMERIC(14, 2),
                    order_count_last_30d INTEGER,
                    order_count_prior_30d INTEGER,
                    days_since_last_order INTEGER
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.int_customer_events__aggregated (
                    customer_id           VARCHAR(36) NOT NULL,
                    most_recent_event_date DATE,
                    days_since_last_event  INTEGER
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.int_customer_tickets__aggregated (
                    customer_id      VARCHAR(36) NOT NULL,
                    open_ticket_count INTEGER
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.stg_campaigns__campaigns (
                    campaign_id     VARCHAR(36)   NOT NULL,
                    campaign_date   DATE          NOT NULL,
                    daily_spend_usd NUMERIC(12, 2) NOT NULL
                )
                """
            )
        yield connection
    finally:
        with connection.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        connection.close()


@pytest.fixture
def intermediate_conn(pg_dsn):
    """A psycopg2 connection with empty intermediate-model input relations.

    Creates only the columns the three intermediate models under test actually
    consume, inside an isolated ``cip_test_int`` schema:

    * ``stg_events__events``      → input to ``int_sessions__with_duration``
    * ``stg_orders__orders`` and
      ``stg_orders__order_items``  → inputs to ``int_orders__with_items``
    * ``int_orders__with_items``   → input to ``int_customer_orders__aggregated``

    The schema is dropped before and after the test so nothing leaks between
    examples or persists past the run. Tables are truncated by the test between
    hypothesis examples.
    """
    connection = psycopg2.connect(pg_dsn)
    connection.autocommit = True
    schema = INTERMEDIATE_SCHEMA
    try:
        with connection.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute(f"CREATE SCHEMA {schema}")
            cur.execute(
                f"""
                CREATE TABLE {schema}.stg_events__events (
                    session_id  VARCHAR(36) NOT NULL,
                    occurred_at TIMESTAMPTZ NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.stg_orders__orders (
                    order_id         VARCHAR(36)   NOT NULL,
                    customer_id      VARCHAR(36)   NOT NULL,
                    order_status     VARCHAR(20)   NOT NULL,
                    total_amount_usd NUMERIC(12, 2) NOT NULL,
                    ordered_at       TIMESTAMPTZ   NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.stg_orders__order_items (
                    order_item_id VARCHAR(36) NOT NULL,
                    order_id      VARCHAR(36) NOT NULL
                )
                """
            )
            cur.execute(
                f"""
                CREATE TABLE {schema}.int_orders__with_items (
                    customer_id      VARCHAR(36)   NOT NULL,
                    total_amount_usd NUMERIC(12, 2) NOT NULL,
                    ordered_at       TIMESTAMPTZ   NOT NULL
                )
                """
            )
        yield connection
    finally:
        with connection.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        connection.close()
