"""Synthetic support ticket generation for the Raw Zone (Task 9).

Generates ``raw.tickets`` and upserts them keyed on ``ticket_id`` for idempotent
re-runs (Requirement 1.8). Every ticket references a valid customer, preserving
referential integrity back to ``raw.customers`` (Requirement 1.6).

Invariants enforced at generation (Requirement 1.5):

* ``description`` contains at least 10 words.
* ``status`` ∈ ``{open, in_progress, closed}`` and ``priority`` ∈
  ``{low, medium, high}``.
* ``resolved_at`` is populated **only** when ``status = closed``; it is NULL for
  every other status.

Determinism: IDs, text, and timestamps come from a seeded ``random.Random`` (and
a per-instance-seeded Faker) so repeated runs reproduce the same rows.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from faker import Faker
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The three statuses and priorities defined by Requirement 1.5.
STATUSES = ("open", "in_progress", "closed")
PRIORITIES = ("low", "medium", "high")

# Fixed seed distinct from the other domain generators → deterministic IDs.
_SEED = 500_001

# Minimum words required in a ticket description (Requirement 1.5).
_MIN_DESCRIPTION_WORDS = 10

# Tickets are created within this many days before the run date.
_WINDOW_DAYS = 365

_CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.tickets (
    ticket_id    VARCHAR(36)  PRIMARY KEY,
    customer_id  VARCHAR(36)  NOT NULL,
    subject      VARCHAR(500) NOT NULL,
    description  TEXT         NOT NULL,
    status       VARCHAR(20)  NOT NULL,
    priority     VARCHAR(10)  NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    resolved_at  TIMESTAMPTZ,
    _ingested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _run_date    DATE         NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_tickets_customer_id ON raw.tickets (customer_id);
CREATE INDEX IF NOT EXISTS ix_raw_tickets_status      ON raw.tickets (status);
CREATE INDEX IF NOT EXISTS ix_raw_tickets_created_at  ON raw.tickets (created_at);
CREATE INDEX IF NOT EXISTS ix_raw_tickets_run_date    ON raw.tickets (_run_date);
"""

_UPSERT_SQL = """
INSERT INTO raw.tickets
    (ticket_id, customer_id, subject, description, status, priority, created_at, resolved_at, _run_date)
VALUES %s
ON CONFLICT (ticket_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    subject     = EXCLUDED.subject,
    description = EXCLUDED.description,
    status      = EXCLUDED.status,
    priority    = EXCLUDED.priority,
    created_at  = EXCLUDED.created_at,
    resolved_at = EXCLUDED.resolved_at,
    _run_date   = EXCLUDED._run_date
"""


def ensure_tables(conn) -> None:
    """Create the ``raw`` schema and ``raw.tickets`` table if absent."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def _det_uuid(rng: random.Random) -> str:
    """Return a deterministic UUID4-formatted string from a seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _description(fake: Faker) -> str:
    """Return a free-text description guaranteed to have ≥ 10 words."""
    words: list[str] = []
    # Accumulate sentences until the 10-word minimum is comfortably met.
    while len(words) < _MIN_DESCRIPTION_WORDS:
        words.extend(fake.sentence(nb_words=8).split())
    return " ".join(words)


def generate_tickets(
    conn,
    customer_ids: list[str],
    n: int = 50_000,
    *,
    run_date: date | None = None,
    batch_size: int = 5_000,
) -> int:
    """Generate and upsert ``n`` support tickets into ``raw.tickets``.

    Args:
        conn: an open psycopg2 connection; the caller owns the transaction.
        customer_ids: valid customer IDs to reference, guaranteeing referential
            integrity back to ``raw.customers``.
        n: number of tickets to generate (default 50,000).
        run_date: pipeline run-date partition value; defaults to today (UTC).
        batch_size: rows per ``execute_values`` round-trip.

    Returns:
        The number of ticket rows written.

    Raises:
        ValueError: if ``customer_ids`` is empty.
    """
    if not customer_ids:
        raise ValueError("customer_ids is empty; generate customers before tickets")

    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    rng = random.Random(_SEED)
    fake = Faker()
    fake.seed_instance(_SEED)

    batch: list[tuple] = []
    written = 0

    with conn.cursor() as cur:
        for _ in range(n):
            ticket_id = _det_uuid(rng)
            customer_id = rng.choice(customer_ids)
            subject = fake.sentence(nb_words=6).rstrip(".")
            description = _description(fake)
            status = rng.choice(STATUSES)
            priority = rng.choice(PRIORITIES)
            created_at = datetime.now(timezone.utc) - timedelta(
                days=rng.randint(0, _WINDOW_DAYS), seconds=rng.randint(0, 86_399)
            )
            # resolved_at is populated only for closed tickets (Requirement 1.5).
            resolved_at = (
                created_at + timedelta(hours=rng.randint(1, 240))
                if status == "closed"
                else None
            )

            batch.append(
                (
                    ticket_id,
                    customer_id,
                    subject,
                    description,
                    status,
                    priority,
                    created_at,
                    resolved_at,
                    run_date,
                )
            )

            if len(batch) >= batch_size:
                execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
                written += len(batch)
                batch.clear()

        if batch:
            execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
            written += len(batch)

    log.info("Upserted %d tickets into raw.tickets", written)
    return written
