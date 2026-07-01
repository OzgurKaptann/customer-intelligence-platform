"""Synthetic order and order-item generation for the Raw Zone (Task 8).

Generates ``raw.orders`` and their child ``raw.order_items`` and upserts both
inside the caller's transaction. Each order carries 1–10 line items with a
positive quantity and a non-negative unit price; ``total_amount_usd`` is the
exact sum of ``quantity * unit_price_usd`` across the order's items
(Requirement 1.3). Upserts key on ``order_id`` and ``order_item_id`` for
idempotent re-runs (Requirement 1.8).

Determinism: IDs and attributes come from a seeded ``random.Random`` so repeated
runs reproduce the same primary keys and therefore the same row counts.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The four order statuses defined by Requirement 1.3.
ORDER_STATUSES = ("completed", "pending", "cancelled", "refunded")

# Fixed seed distinct from the customers seed → deterministic order IDs.
_SEED = 250_001

_MIN_ITEMS = 1
_MAX_ITEMS = 10
_TWO_PLACES = Decimal("0.01")

# Idempotent, design-aligned DDL. Raw tables carry no DB-level foreign keys;
# referential integrity is asserted post-load by ``integrity.py``.
_CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id         VARCHAR(36)   PRIMARY KEY,
    customer_id      VARCHAR(36)   NOT NULL,
    order_status     VARCHAR(20)   NOT NULL,
    total_amount_usd NUMERIC(12,2) NOT NULL,
    ordered_at       TIMESTAMPTZ   NOT NULL,
    _ingested_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    _run_date        DATE          NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_orders_customer_id ON raw.orders (customer_id);
CREATE INDEX IF NOT EXISTS ix_raw_orders_ordered_at  ON raw.orders (ordered_at);
CREATE INDEX IF NOT EXISTS ix_raw_orders_run_date    ON raw.orders (_run_date);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id  VARCHAR(36)   PRIMARY KEY,
    order_id       VARCHAR(36)   NOT NULL,
    product_id     VARCHAR(36)   NOT NULL,
    quantity       INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price_usd NUMERIC(10,2) NOT NULL CHECK (unit_price_usd >= 0),
    _ingested_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    _run_date      DATE          NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_raw_order_items_order_id ON raw.order_items (order_id);
"""

_UPSERT_ORDERS_SQL = """
INSERT INTO raw.orders
    (order_id, customer_id, order_status, total_amount_usd, ordered_at, _run_date)
VALUES %s
ON CONFLICT (order_id) DO UPDATE SET
    customer_id      = EXCLUDED.customer_id,
    order_status     = EXCLUDED.order_status,
    total_amount_usd = EXCLUDED.total_amount_usd,
    ordered_at       = EXCLUDED.ordered_at,
    _run_date        = EXCLUDED._run_date
"""

_UPSERT_ITEMS_SQL = """
INSERT INTO raw.order_items
    (order_item_id, order_id, product_id, quantity, unit_price_usd, _run_date)
VALUES %s
ON CONFLICT (order_item_id) DO UPDATE SET
    order_id       = EXCLUDED.order_id,
    product_id     = EXCLUDED.product_id,
    quantity       = EXCLUDED.quantity,
    unit_price_usd = EXCLUDED.unit_price_usd,
    _run_date      = EXCLUDED._run_date
"""


def ensure_tables(conn) -> None:
    """Create the ``raw`` schema and order tables if absent."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def _det_uuid(rng: random.Random) -> str:
    """Return a deterministic UUID4-formatted string from a seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _flush(cur, order_rows: list[tuple], item_rows: list[tuple], page_size: int) -> None:
    """Upsert buffered orders then their items (parents before children)."""
    if order_rows:
        execute_values(cur, _UPSERT_ORDERS_SQL, order_rows, page_size=page_size)
    if item_rows:
        execute_values(cur, _UPSERT_ITEMS_SQL, item_rows, page_size=page_size)


def generate_orders(
    conn,
    customer_ids: list[str],
    n: int = 250_000,
    *,
    run_date: date | None = None,
    batch_size: int = 2_000,
) -> tuple[int, int]:
    """Generate and upsert ``n`` orders (with line items) into the Raw Zone.

    Args:
        conn: an open psycopg2 connection; the caller owns the transaction.
        customer_ids: valid customer IDs to reference, guaranteeing referential
            integrity back to ``raw.customers``.
        n: number of orders to generate (default 250,000).
        run_date: pipeline run-date partition value; defaults to today (UTC).
        batch_size: orders per ``execute_values`` round-trip (items flush with
            their parent orders).

    Returns:
        ``(order_count, order_item_count)`` actually written.

    Raises:
        ValueError: if ``customer_ids`` is empty.
    """
    if not customer_ids:
        raise ValueError("customer_ids is empty; generate customers before orders")

    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    rng = random.Random(_SEED)

    order_rows: list[tuple] = []
    item_rows: list[tuple] = []
    order_count = 0
    item_count = 0

    with conn.cursor() as cur:
        for _ in range(n):
            order_id = _det_uuid(rng)
            customer_id = rng.choice(customer_ids)
            status = rng.choice(ORDER_STATUSES)
            ordered_at = datetime.now(timezone.utc) - timedelta(
                days=rng.randint(0, 730), seconds=rng.randint(0, 86_399)
            )

            num_items = rng.randint(_MIN_ITEMS, _MAX_ITEMS)
            total = Decimal("0.00")
            for _ in range(num_items):
                quantity = rng.randint(1, 5)
                unit_price = Decimal(str(round(rng.uniform(1.0, 500.0), 2)))
                total += unit_price * quantity
                item_rows.append(
                    (_det_uuid(rng), order_id, _det_uuid(rng), quantity, unit_price, run_date)
                )
            total = total.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)

            order_rows.append((order_id, customer_id, status, total, ordered_at, run_date))

            if len(order_rows) >= batch_size:
                _flush(cur, order_rows, item_rows, batch_size)
                order_count += len(order_rows)
                item_count += len(item_rows)
                order_rows.clear()
                item_rows.clear()

        if order_rows or item_rows:
            _flush(cur, order_rows, item_rows, batch_size)
            order_count += len(order_rows)
            item_count += len(item_rows)

    log.info(
        "Upserted %d orders and %d order_items into raw", order_count, item_count
    )
    return order_count, item_count
