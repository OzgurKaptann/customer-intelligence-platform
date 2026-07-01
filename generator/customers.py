"""Synthetic CRM customer generation for the Raw Zone (Task 8).

Generates the ``raw.customers`` population with Faker and upserts it into
PostgreSQL using ``ON CONFLICT (customer_id) DO UPDATE`` so that re-running the
generator is idempotent (Requirement 1.1, 1.7, 1.8).

Determinism
-----------
Idempotent re-runs (Property 3 / Requirement 1.8) require that the *same*
primary keys are produced on every run. ``uuid.uuid4()`` draws from
``os.urandom`` and cannot be seeded, so customer IDs are derived from a seeded
``random.Random`` instance instead. Faker is likewise seeded per instance so
names are reproducible.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from faker import Faker
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The five acquisition channels defined by Requirement 1.1.
ACQUISITION_CHANNELS = ("organic", "paid_search", "social", "referral", "direct")

# Fixed seed → deterministic customer IDs, names, and attributes across re-runs.
_SEED = 800_001

# DDL for the CRM raw table. Idempotent (IF NOT EXISTS) and column-for-column
# aligned with the design data model so it coexists with the Task 20 DDL. No
# foreign keys are declared on raw tables — the Raw Zone is an append-only
# landing zone and integrity is asserted post-load by ``integrity.py``.
_CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id         VARCHAR(36)  PRIMARY KEY,
    name                VARCHAR(255) NOT NULL,
    email               VARCHAR(255) NOT NULL UNIQUE,
    acquisition_channel VARCHAR(50)  NOT NULL,
    country_code        CHAR(2)      NOT NULL,
    account_created_at  DATE         NOT NULL,
    _ingested_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _run_date           DATE         NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_customers_country_code ON raw.customers (country_code);
CREATE INDEX IF NOT EXISTS ix_raw_customers_run_date     ON raw.customers (_run_date);
"""

_UPSERT_SQL = """
INSERT INTO raw.customers
    (customer_id, name, email, acquisition_channel, country_code, account_created_at, _run_date)
VALUES %s
ON CONFLICT (customer_id) DO UPDATE SET
    name                = EXCLUDED.name,
    email               = EXCLUDED.email,
    acquisition_channel = EXCLUDED.acquisition_channel,
    country_code        = EXCLUDED.country_code,
    account_created_at  = EXCLUDED.account_created_at,
    _run_date           = EXCLUDED._run_date
"""


def ensure_tables(conn) -> None:
    """Create the ``raw`` schema and ``raw.customers`` table if absent."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def _det_uuid(rng: random.Random) -> str:
    """Return a deterministic UUID4-formatted string from a seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate_customers(
    conn,
    n: int = 100_000,
    *,
    run_date: date | None = None,
    batch_size: int = 5_000,
) -> list[str]:
    """Generate and upsert ``n`` synthetic customers into ``raw.customers``.

    Args:
        conn: an open psycopg2 connection. The caller owns the transaction; this
            function never commits or rolls back.
        n: number of customer records to generate (default 100,000).
        run_date: pipeline run-date partition value; defaults to today (UTC).
        batch_size: rows per ``execute_values`` round-trip.

    Returns:
        The list of generated ``customer_id`` values, for use as foreign keys by
        downstream domain generators (orders, etc.).
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    rng = random.Random(_SEED)
    fake = Faker()
    fake.seed_instance(_SEED)

    customer_ids: list[str] = []
    batch: list[tuple] = []
    inserted = 0

    with conn.cursor() as cur:
        for i in range(n):
            customer_id = _det_uuid(rng)
            name = fake.name()
            # Append the row index to guarantee email uniqueness at 100K scale,
            # where Faker's own provider pool would otherwise be exhausted.
            local = "".join(ch for ch in name.lower() if ch.isalnum()) or "user"
            email = f"{local}.{i}@example.com"
            channel = rng.choice(ACQUISITION_CHANNELS)
            country_code = fake.country_code(representation="alpha-2")
            account_created_at = run_date - timedelta(days=rng.randint(1, 1825))

            customer_ids.append(customer_id)
            batch.append(
                (customer_id, name, email, channel, country_code, account_created_at, run_date)
            )

            if len(batch) >= batch_size:
                execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
                inserted += len(batch)
                batch.clear()

        if batch:
            execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
            inserted += len(batch)

    log.info("Upserted %d customers into raw.customers", inserted)
    return customer_ids
