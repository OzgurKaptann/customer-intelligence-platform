"""Customer generation invariants (Task 10).

Covers Requirement 1.1: an exact customer row count after generation and unique
customer emails. The production default is 100,000 customers; the same invariant
is validated here at a small, fast volume.
"""

from __future__ import annotations

from datetime import date

import customers

RUN_DATE = date(2026, 7, 1)


def test_customer_row_count_is_exact(conn):
    """Generating N customers yields exactly N rows in raw.customers."""
    n = 1_000
    customers.ensure_tables(conn)
    ids = customers.generate_customers(conn, n, run_date=RUN_DATE)

    assert len(ids) == n
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.customers")
        assert cur.fetchone()[0] == n


def test_customer_emails_are_unique(conn):
    """Every generated customer has a distinct email address."""
    n = 1_000
    customers.ensure_tables(conn)
    customers.generate_customers(conn, n, run_date=RUN_DATE)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(DISTINCT email) FROM raw.customers")
        total, distinct = cur.fetchone()

    assert total == n
    assert distinct == n


def test_customer_ids_are_unique(conn):
    """Primary keys are unique, so no upsert collisions inflate/deflate counts."""
    n = 500
    customers.ensure_tables(conn)
    customers.generate_customers(conn, n, run_date=RUN_DATE)

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT customer_id) FROM raw.customers")
        assert cur.fetchone()[0] == n
