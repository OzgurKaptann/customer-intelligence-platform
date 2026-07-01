"""Synthetic clickstream event generation for the Raw Zone (Task 9).

Generates ``raw.events`` and upserts them into PostgreSQL keyed on ``event_id``
for idempotent re-runs (Requirement 1.8). Events are distributed across the
whole customer population such that **every customer receives at least one
event** (Requirement 1.2): the first pass emits exactly one event per customer,
and the remaining budget is scattered across randomly chosen customers.

Determinism
-----------
Idempotent re-runs (Property 3 / Requirement 1.8) require identical primary keys
on every run, so event IDs, session IDs, and all attributes are drawn from a
seeded ``random.Random`` instance. Faker is deliberately avoided in the hot loop
— at 1M–5M rows the per-row cost matters, so page URLs are assembled from a
fixed path pool instead.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# Clickstream vocabularies (Requirement 1.2). event_type covers the page-view /
# click / session lifecycle described in the glossary; device_type is the three
# surfaces named in the design data model.
EVENT_TYPES = (
    "page_view",
    "click",
    "session_start",
    "session_end",
    "add_to_cart",
    "purchase",
)
DEVICE_TYPES = ("desktop", "mobile", "tablet")

# Fixed URL path pool → deterministic, Faker-free page_url construction.
_PAGE_PATHS = (
    "/",
    "/products",
    "/products/detail",
    "/cart",
    "/checkout",
    "/search",
    "/account",
    "/support",
    "/blog",
    "/pricing",
)
_BASE_URL = "https://shop.example.com"

# Fixed seed distinct from the other domain generators → deterministic event IDs.
_SEED = 900_001

# New sessions start roughly every few events, producing multi-event sessions.
_NEW_SESSION_PROB = 0.35

# Events fall within this many days before the run date.
_WINDOW_DAYS = 90

_CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events (
    event_id     VARCHAR(36)  PRIMARY KEY,
    session_id   VARCHAR(36)  NOT NULL,
    customer_id  VARCHAR(36)  NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    page_url     TEXT         NOT NULL,
    device_type  VARCHAR(50)  NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    _ingested_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    _run_date    DATE         NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_events_customer_id ON raw.events (customer_id);
CREATE INDEX IF NOT EXISTS ix_raw_events_session_id  ON raw.events (session_id);
CREATE INDEX IF NOT EXISTS ix_raw_events_occurred_at ON raw.events (occurred_at);
CREATE INDEX IF NOT EXISTS ix_raw_events_run_date    ON raw.events (_run_date);
"""

_UPSERT_SQL = """
INSERT INTO raw.events
    (event_id, session_id, customer_id, event_type, page_url, device_type, occurred_at, _run_date)
VALUES %s
ON CONFLICT (event_id) DO UPDATE SET
    session_id  = EXCLUDED.session_id,
    customer_id = EXCLUDED.customer_id,
    event_type  = EXCLUDED.event_type,
    page_url    = EXCLUDED.page_url,
    device_type = EXCLUDED.device_type,
    occurred_at = EXCLUDED.occurred_at,
    _run_date   = EXCLUDED._run_date
"""


def ensure_tables(conn) -> None:
    """Create the ``raw`` schema and ``raw.events`` table if absent."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def _det_uuid(rng: random.Random) -> str:
    """Return a deterministic UUID4-formatted string from a seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _make_event(
    rng: random.Random, customer_id: str, session_id: str, run_date: date
) -> tuple:
    """Build one event row for ``customer_id`` within ``session_id``."""
    event_type = rng.choice(EVENT_TYPES)
    page_url = _BASE_URL + rng.choice(_PAGE_PATHS)
    device_type = rng.choice(DEVICE_TYPES)
    occurred_at = datetime.now(timezone.utc) - timedelta(
        days=rng.randint(0, _WINDOW_DAYS), seconds=rng.randint(0, 86_399)
    )
    return (
        _det_uuid(rng),
        session_id,
        customer_id,
        event_type,
        page_url,
        device_type,
        occurred_at,
        run_date,
    )


def generate_events(
    conn,
    customer_ids: list[str],
    n_min: int = 1_000_000,
    n_max: int = 5_000_000,
    *,
    n: int | None = None,
    run_date: date | None = None,
    batch_size: int = 5_000,
) -> int:
    """Generate and upsert events into ``raw.events``.

    The total number of events is ``n`` when provided, otherwise ``n_min``. The
    value is clamped to ``[n_min, n_max]`` and then raised, if necessary, to at
    least ``len(customer_ids)`` so that every customer is guaranteed ≥1 event
    (Requirement 1.2).

    Args:
        conn: an open psycopg2 connection; the caller owns the transaction.
        customer_ids: valid customer IDs to reference, guaranteeing referential
            integrity back to ``raw.customers``.
        n_min: lower bound of the event volume (default 1,000,000).
        n_max: upper bound of the event volume (default 5,000,000).
        n: explicit event count override (e.g. from ``SEED_EVENTS``); clamped
            into ``[n_min, n_max]`` when supplied.
        run_date: pipeline run-date partition value; defaults to today (UTC).
        batch_size: rows per ``execute_values`` round-trip.

    Returns:
        The number of event rows written.

    Raises:
        ValueError: if ``customer_ids`` is empty.
    """
    if not customer_ids:
        raise ValueError("customer_ids is empty; generate customers before events")

    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    total = n_min if n is None else n
    total = max(n_min, min(total, n_max))
    # Every customer must receive at least one event (Requirement 1.2).
    total = max(total, len(customer_ids))

    rng = random.Random(_SEED)

    batch: list[tuple] = []
    written = 0

    with conn.cursor() as cur:
        # Pass 1 — one event per customer guarantees full coverage.
        for customer_id in customer_ids:
            session_id = _det_uuid(rng)
            batch.append(_make_event(rng, customer_id, session_id, run_date))
            if len(batch) >= batch_size:
                execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
                written += len(batch)
                batch.clear()

        # Pass 2 — scatter the remaining budget across random customers,
        # occasionally reusing a session so sessions span multiple events.
        remaining = total - len(customer_ids)
        session_id = _det_uuid(rng)
        for _ in range(remaining):
            if rng.random() < _NEW_SESSION_PROB:
                session_id = _det_uuid(rng)
            customer_id = rng.choice(customer_ids)
            batch.append(_make_event(rng, customer_id, session_id, run_date))
            if len(batch) >= batch_size:
                execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
                written += len(batch)
                batch.clear()

        if batch:
            execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
            written += len(batch)

    log.info("Upserted %d events into raw.events", written)
    return written
