"""Property test: RFM score invariants (Task 22).

**Property 6 — RFM Score Invariants** and **Property 7 — Inactive Customers
Receive the Correct Default Scores**.

Validates Requirements 3.5, 5.1, 5.2, 5.5:

* ``recency_score``, ``frequency_score``, ``monetary_score`` are integers in
  the range [0, 5].
* ``rfm_score`` matches the pattern ``R{r}F{f}M{m}`` and its components match
  the individual dimension scores exactly.
* ``recency_days`` never exceeds 999 (the design cap).
* Customers with no orders in the trailing 365 days
  (``order_frequency_365d = 0``) receive ``rfm_score = 'R0F0M0'`` and all three
  dimension scores of 0.
* The model output is deterministic for the same input.

Approach
--------
``hypothesis`` generates a population of synthetic customers with varying order
frequencies, recency, and spend — including inactive customers (no trailing
365-day orders) and out-of-range recency values that must be capped. The rows
are loaded into the mart's real input relations and the **real**
``mart_customer_360`` model SQL is executed against an isolated PostgreSQL
database (see ``conftest.render_mart_customer_360``). The scoring logic under
test is the model's own — the test never reimplements it.
"""

from __future__ import annotations

import re
from decimal import Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import MART_360_SCHEMA, render_mart_customer_360

_RUN_DATE = "2026-07-01"
_RFM_RE = re.compile(r"^R([0-5])F([0-5])M([0-5])$")


def _customer_id(index: int) -> str:
    """Deterministic UUID-shaped id, fitting VARCHAR(36)."""
    return f"00000000-0000-4000-8000-{index:012d}"


# Each customer: an order frequency (0 = no trailing-365-day orders), a recency
# that deliberately includes values above the 999 cap and NULL, and a spend.
# ``has_order_row`` exercises both inactive representations — a customer with no
# order aggregate row at all, and one present with order_frequency_365d = 0.
_customer = st.fixed_dictionaries(
    {
        "order_frequency_365d": st.integers(min_value=0, max_value=40),
        "days_since_last_order": st.one_of(
            st.none(), st.integers(min_value=0, max_value=5000)
        ),
        "total_spend_365d_usd": st.decimals(
            min_value=Decimal("0"),
            max_value=Decimal("100000"),
            places=2,
            allow_nan=False,
            allow_infinity=False,
        ),
        "has_order_row": st.booleans(),
    }
)


def _run_model(conn) -> list[tuple]:
    """Execute the real mart_customer_360 SQL and return its rows, ordered."""
    model_sql = render_mart_customer_360(_RUN_DATE)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT customer_id, recency_score, frequency_score, monetary_score,"
            " rfm_score, recency_days, order_frequency_365d"
            f" FROM (\n{model_sql}\n) AS mart ORDER BY customer_id"
        )
        return cur.fetchall()


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(customers=st.lists(_customer, min_size=1, max_size=40))
def test_rfm_score_invariants(mart_360_conn, customers):
    conn = mart_360_conn
    schema = MART_360_SCHEMA

    enriched_rows = []
    order_rows = []
    for idx, c in enumerate(customers):
        cid = _customer_id(idx)
        enriched_rows.append((cid, "organic", 365))

        freq = c["order_frequency_365d"]
        # A customer is active only if an order aggregate row exists with
        # freq >= 1, so active customers must have a row. Inactive customers get
        # a row only when has_order_row is True (covering the explicit-zero
        # path); otherwise no row (covering the COALESCE-to-zero path).
        if freq >= 1 or c["has_order_row"]:
            order_rows.append(
                (
                    cid,
                    freq,  # total_order_count (value irrelevant to RFM)
                    c["total_spend_365d_usd"],  # total_spend_usd
                    freq,  # order_frequency_365d
                    c["total_spend_365d_usd"],  # total_spend_365d_usd
                    0,  # order_count_last_30d
                    0,  # order_count_prior_30d
                    c["days_since_last_order"],
                )
            )

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {schema}.int_customers__enriched")
        cur.execute(f"TRUNCATE {schema}.int_customer_orders__aggregated")
        cur.executemany(
            f"INSERT INTO {schema}.int_customers__enriched"
            " (customer_id, acquisition_channel, customer_tenure_days)"
            " VALUES (%s, %s, %s)",
            enriched_rows,
        )
        cur.executemany(
            f"INSERT INTO {schema}.int_customer_orders__aggregated"
            " (customer_id, total_order_count, total_spend_usd,"
            " order_frequency_365d, total_spend_365d_usd, order_count_last_30d,"
            " order_count_prior_30d, days_since_last_order)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            order_rows,
        )

    rows = _run_model(conn)

    assert len(rows) == len(customers), "one output row expected per customer"

    for (
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        rfm_score,
        recency_days,
        order_frequency_365d,
    ) in rows:
        # Property 6: dimension scores are integers in [0, 5].
        for name, score in (
            ("recency_score", recency_score),
            ("frequency_score", frequency_score),
            ("monetary_score", monetary_score),
        ):
            assert isinstance(score, int), f"{customer_id}: {name} not integer"
            assert 0 <= score <= 5, f"{customer_id}: {name}={score} out of [0,5]"

        # Property 6: rfm_score matches R{r}F{f}M{m} and agrees with components.
        match = _RFM_RE.match(rfm_score)
        assert match, f"{customer_id}: rfm_score {rfm_score!r} malformed"
        assert (int(match.group(1)), int(match.group(2)), int(match.group(3))) == (
            recency_score,
            frequency_score,
            monetary_score,
        ), f"{customer_id}: rfm_score {rfm_score!r} disagrees with components"

        # Property 6: recency_days is capped at 999.
        assert recency_days <= 999, f"{customer_id}: recency_days {recency_days} > 999"

        # Property 7: inactive customers (no trailing-365-day orders) default.
        if order_frequency_365d == 0:
            assert rfm_score == "R0F0M0", (
                f"{customer_id}: inactive but rfm_score {rfm_score!r} != R0F0M0"
            )
            assert (recency_score, frequency_score, monetary_score) == (0, 0, 0), (
                f"{customer_id}: inactive but dimension scores are not all 0"
            )

    # Determinism: re-running the model on identical input yields identical rows.
    assert _run_model(conn) == rows, "model output is not deterministic"
