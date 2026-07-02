"""Property test: staging deduplication invariant (Task 16).

**Property 4 — Staging Deduplication Preserves Exactly One Row Per Primary Key**
Validates Requirement 3.1: "WHERE duplicate records share the same primary key,
the record with the latest ingestion timestamp SHALL be retained."

Approach
--------
``hypothesis`` generates lists of ``(customer_id, version)`` pairs — deliberately
including duplicate ``customer_id`` values. Each version maps to a distinct
``_ingested_at`` timestamp, so every duplicated id has a single, unambiguous
"latest" row. The real ``stg_crm__customers`` model SQL is then executed against
an isolated PostgreSQL database and two invariants are asserted:

1. ``COUNT(*) == COUNT(DISTINCT customer_id)`` — exactly one row per PK.
2. For every ``customer_id``, the surviving ``ingested_at`` equals the maximum
   ``_ingested_at`` that was ingested for that id.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import render_stg_crm_customers

# A fixed epoch; version N maps to base + N minutes, guaranteeing distinct
# _ingested_at values within any single customer_id group.
_BASE_TS = datetime(2026, 1, 1, tzinfo=timezone.utc)
_ACCOUNT_DATE = "2025-06-15"
_RUN_DATE = "2026-07-01"


def _customer_id(index: int) -> str:
    """Deterministic UUID-shaped id from a small pool, fitting VARCHAR(36)."""
    return f"00000000-0000-4000-8000-{index:012d}"


# Pairs draw from a small id pool so duplicates arise naturally; the version
# component distinguishes ingestion events for the same customer.
_pairs = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=9),
        st.integers(min_value=0, max_value=200),
    ),
    min_size=1,
    max_size=60,
)


@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(pairs=_pairs)
def test_staging_dedup_keeps_one_latest_row_per_pk(raw_customers_conn, pairs):
    conn = raw_customers_conn

    # De-duplicate (id, version) so each customer_id maps every version to a
    # distinct _ingested_at — the invariant is defined for *different*
    # timestamps, so ties are excluded by construction.
    unique_pairs = set(pairs)

    # Expected latest version (and thus latest timestamp) per customer_id.
    latest_version: dict[int, int] = {}
    for idx, version in unique_pairs:
        if version > latest_version.get(idx, -1):
            latest_version[idx] = version

    rows = [
        (
            _customer_id(idx),
            "Test Name",
            f"user-{idx}-{version}@example.test",
            "organic",
            "US",
            _ACCOUNT_DATE,
            _BASE_TS + timedelta(minutes=version),
            _RUN_DATE,
        )
        for idx, version in unique_pairs
    ]

    with conn.cursor() as cur:
        cur.execute("TRUNCATE raw.customers")
        cur.executemany(
            """
            INSERT INTO raw.customers (
                customer_id, name, email, acquisition_channel, country_code,
                account_created_at, _ingested_at, _run_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

        model_sql = render_stg_crm_customers()
        cur.execute(
            f"SELECT customer_id, ingested_at FROM (\n{model_sql}\n) AS stg"
        )
        output = cur.fetchall()

    output_ids = [row[0] for row in output]

    # Invariant 1: exactly one row per customer_id (no duplicate PKs).
    assert len(output_ids) == len(set(output_ids)), "duplicate customer_id in output"
    assert len(output_ids) == len(latest_version), "row count != distinct input ids"

    # Invariant 2: the surviving row is the latest-ingested one for each id.
    for customer_id, ingested_at in output:
        idx = int(customer_id.rsplit("-", 1)[1])
        expected = _BASE_TS + timedelta(minutes=latest_version[idx])
        assert ingested_at == expected, (
            f"{customer_id}: surviving ingested_at {ingested_at} "
            f"is not the latest {expected}"
        )
