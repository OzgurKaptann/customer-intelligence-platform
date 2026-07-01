"""Shared fixtures for the synthetic data generator unit tests (Task 10).

Isolation strategy
------------------
Every test runs against an **isolated** PostgreSQL instance:

* If ``CIP_TEST_DATABASE_URL`` is set, that database is used as-is (it is
  assumed to be a throwaway/test database — the ``raw`` schema is dropped and
  recreated around each test).
* Otherwise a throwaway ``postgres:15.6-alpine`` container (the image pinned by
  the design) is started for the test session and removed on teardown.

If neither a test URL nor Docker is available, the whole suite is skipped with a
clear message rather than touching a developer's database.

Cleanup
-------
The ``conn`` fixture drops and recreates the ``raw`` schema before and after
every test, so no generated data survives a test run regardless of which backend
is used.

Small datasets
--------------
The generators default to production volumes (100K customers, ≥1M events, ...).
Those defaults are overridable per call, so ``generate_all`` drives every domain
generator at tiny sizes for fast, deterministic tests while exercising the exact
same code paths.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys
import time
import uuid
from datetime import date

import psycopg2
import pytest

# The generator modules import each other as top-level modules (``import
# customers`` etc.), so the generator directory must be importable directly.
_GENERATOR_DIR = pathlib.Path(__file__).resolve().parents[2] / "generator"
if str(_GENERATOR_DIR) not in sys.path:
    sys.path.insert(0, str(_GENERATOR_DIR))


def _docker_available() -> bool:
    try:
        result = subprocess.run(
            ["docker", "version"],
            capture_output=True,
            timeout=30,
        )
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
    # e.g. "0.0.0.0:49158\n[::]:49158" — take the port of the first mapping.
    first = out.splitlines()[0]
    return first.rsplit(":", 1)[1]


@pytest.fixture(scope="session")
def pg_dsn():
    """Yield a DSN for an isolated test PostgreSQL database.

    Prefers ``CIP_TEST_DATABASE_URL``; otherwise starts (and later removes) a
    throwaway ``postgres:15.6-alpine`` container.
    """
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
def conn(pg_dsn):
    """A psycopg2 connection with a freshly-empty ``raw`` schema.

    The schema is dropped before and after each test so generated data never
    leaks between tests or persists past the run.
    """
    connection = psycopg2.connect(pg_dsn)
    connection.autocommit = True
    try:
        with connection.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE")
        yield connection
    finally:
        with connection.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS raw CASCADE")
        connection.close()


@pytest.fixture
def generate_all():
    """Return a helper that runs every domain generator at small sizes.

    The helper mirrors ``generator/main.run()``'s ordering (customers → orders →
    events → campaigns → tickets) but at tiny, overridden volumes. It returns the
    generated ``customer_id`` list.
    """
    import campaigns
    import customers
    import events
    import orders
    import tickets

    def _run(
        connection,
        *,
        n_customers: int = 200,
        n_orders: int = 300,
        n_events: int = 400,
        n_campaigns: int = 120,
        n_tickets: int = 200,
        run_date: date | None = None,
    ) -> list[str]:
        if run_date is None:
            run_date = date(2026, 7, 1)

        customers.ensure_tables(connection)
        orders.ensure_tables(connection)
        events.ensure_tables(connection)
        campaigns.ensure_tables(connection)
        tickets.ensure_tables(connection)

        ids = customers.generate_customers(connection, n_customers, run_date=run_date)
        orders.generate_orders(connection, ids, n_orders, run_date=run_date)
        # Override the ≥1M event floor for a fast test while exercising the same
        # code path; n_max is widened so n is not clamped down.
        events.generate_events(
            connection,
            ids,
            n_min=n_events,
            n_max=max(n_events, n_customers) + 1,
            n=n_events,
            run_date=run_date,
        )
        # Override the 500–2000 campaign range for a small deterministic count.
        campaigns.generate_campaigns(
            connection,
            n_min=n_campaigns,
            n_max=n_campaigns,
            n=n_campaigns,
            run_date=run_date,
        )
        tickets.generate_tickets(connection, ids, n_tickets, run_date=run_date)
        return ids

    return _run


def table_count(connection, table: str) -> int:
    """Return ``COUNT(*)`` for a fully-qualified ``raw`` table."""
    with connection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
