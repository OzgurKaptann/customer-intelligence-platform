"""Referential integrity of generated data (Task 10).

Covers Requirement 1.6 / Property 1: after a full generation run, every foreign
key across events, orders (and order items), and tickets resolves to a valid
customer, i.e. zero orphan foreign keys.
"""

from __future__ import annotations

import integrity

# Each orphan check: (label, COUNT-of-orphans SQL).
_ORPHAN_QUERIES = {
    "orders": """
        SELECT COUNT(*) FROM raw.orders o
        LEFT JOIN raw.customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,
    "order_items": """
        SELECT COUNT(*) FROM raw.order_items i
        LEFT JOIN raw.orders o ON i.order_id = o.order_id
        WHERE o.order_id IS NULL
    """,
    "events": """
        SELECT COUNT(*) FROM raw.events e
        LEFT JOIN raw.customers c ON e.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,
    "tickets": """
        SELECT COUNT(*) FROM raw.tickets t
        LEFT JOIN raw.customers c ON t.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """,
}


def test_assert_referential_integrity_passes(conn, generate_all):
    """The generator's own post-load assertion does not raise on generated data."""
    generate_all(conn)
    # Raises AssertionError if any orphan FK exists.
    integrity.assert_referential_integrity(conn)


def test_zero_orphan_foreign_keys(conn, generate_all):
    """Directly verify zero orphans for each customer-referencing relationship."""
    generate_all(conn)

    with conn.cursor() as cur:
        for label, query in _ORPHAN_QUERIES.items():
            cur.execute(query)
            orphans = cur.fetchone()[0]
            assert orphans == 0, f"{label} has {orphans} orphan foreign key(s)"
