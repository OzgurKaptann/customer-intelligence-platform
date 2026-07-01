"""Post-generation referential-integrity assertions.

Requirement 1.6 mandates a post-generation check that every foreign key resolves
to a valid ``raw.customers`` record and returns zero orphans. With Task 9
implemented, the CRM → Orders → Order Items → Events → Tickets domains are all
generated, so every customer-referencing relationship is checked here:

* ``raw.orders.customer_id``    → ``raw.customers.customer_id``
* ``raw.order_items.order_id``  → ``raw.orders.order_id``
* ``raw.events.customer_id``    → ``raw.customers.customer_id``
* ``raw.tickets.customer_id``   → ``raw.customers.customer_id``

Only relationships for domains that have actually been generated should be
listed; campaigns carry no customer foreign key and so have no orphan check.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# Each entry: (human-readable relationship, COUNT-of-orphans query).
_ORPHAN_CHECKS: tuple[tuple[str, str], ...] = (
    (
        "raw.orders.customer_id -> raw.customers.customer_id",
        """
        SELECT COUNT(*)
        FROM raw.orders o
        LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    ),
    (
        "raw.order_items.order_id -> raw.orders.order_id",
        """
        SELECT COUNT(*)
        FROM raw.order_items i
        LEFT JOIN raw.orders o ON i.order_id = o.order_id
        WHERE o.order_id IS NULL
        """,
    ),
    (
        "raw.events.customer_id -> raw.customers.customer_id",
        """
        SELECT COUNT(*)
        FROM raw.events e
        LEFT JOIN raw.customers c ON e.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    ),
    (
        "raw.tickets.customer_id -> raw.customers.customer_id",
        """
        SELECT COUNT(*)
        FROM raw.tickets t
        LEFT JOIN raw.customers c ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """,
    ),
)


def assert_referential_integrity(conn) -> None:
    """Assert zero orphan foreign keys across the implemented raw domains.

    Args:
        conn: an open psycopg2 connection.

    Raises:
        AssertionError: if any orphan foreign keys are found. The message lists
            every failing relationship and its orphan count.
    """
    failures: list[str] = []

    with conn.cursor() as cur:
        for relationship, query in _ORPHAN_CHECKS:
            cur.execute(query)
            orphan_count = cur.fetchone()[0]
            if orphan_count > 0:
                failures.append(f"{relationship}: {orphan_count} orphan(s)")
            else:
                log.info("Referential integrity OK: %s", relationship)

    if failures:
        raise AssertionError(
            "Referential integrity check failed: " + "; ".join(failures)
        )
