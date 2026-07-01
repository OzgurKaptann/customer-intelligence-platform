"""Idempotent data generation (Task 10).

Covers Requirement 1.8 / Property 3: running the generator a second time for the
same run date upserts on each domain's primary key rather than inserting
duplicates, so every raw table's row count is unchanged on the second run.
"""

from __future__ import annotations

from conftest import table_count

_RAW_TABLES = (
    "raw.customers",
    "raw.orders",
    "raw.order_items",
    "raw.events",
    "raw.campaigns",
    "raw.tickets",
)


def test_second_run_does_not_change_row_counts(conn, generate_all):
    """A second full generation run leaves every raw table's row count identical."""
    generate_all(conn)
    first = {t: table_count(conn, t) for t in _RAW_TABLES}

    # Sanity: the first run actually produced data in every table.
    for table, count in first.items():
        assert count > 0, f"{table} unexpectedly empty after first run"

    generate_all(conn)
    second = {t: table_count(conn, t) for t in _RAW_TABLES}

    assert second == first
