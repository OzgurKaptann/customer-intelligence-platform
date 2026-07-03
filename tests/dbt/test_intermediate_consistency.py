"""Property test: intermediate derived field consistency (Task 23).

**Property 5 — Derived Intermediate Fields Are Mathematically Consistent**

Validates Requirement 3.3: derived fields in the intermediate layer are computed
consistently from their inputs. The three models under test are exercised with
their **real** SQL (rendered by ``conftest``) against an isolated PostgreSQL
database, so the derivations under test are the models' own — the test never
reimplements them, only recomputes the expected values independently in Python.

Invariants asserted
--------------------
``int_sessions__with_duration``
    * ``session_duration_seconds == (max(occurred_at) - min(occurred_at))`` in
      seconds for the session's events.
    * ``session_duration_seconds >= 0`` for every session.

``int_orders__with_items``
    * ``item_count == number of order_items joined to the order``.
    * ``avg_item_value_usd == round(total_amount_usd / item_count, 2)`` (the model
      casts to ``numeric(10, 2)``).
    * ``item_count >= 1`` for every emitted order (INNER JOIN guarantees this).

``int_customer_orders__aggregated``
    * ``total_order_count`` and the trailing 365-day / rolling 30-day window
      counts (``order_frequency_365d``, ``order_count_last_30d``,
      ``order_count_prior_30d``) match the counts implied by the input orders'
      ``ordered_at`` dates relative to the run date.
    * ``days_since_last_order == run_date - max(ordered_at)::date``.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import (
    INTERMEDIATE_SCHEMA,
    render_int_customer_orders_aggregated,
    render_int_orders_with_items,
    render_int_sessions_with_duration,
)

_RUN_DATE = date(2026, 7, 1)
_RUN_DATE_ISO = _RUN_DATE.isoformat()
_SCHEMA = INTERMEDIATE_SCHEMA


def _id(kind: int, index: int) -> str:
    """Deterministic UUID-shaped id fitting VARCHAR(36).

    ``kind`` namespaces the entity (order / customer / item / session) so ids
    never collide across tables; ``index`` distinguishes rows within an entity.
    """
    return f"{kind:08d}-0000-4000-8000-{index:012d}"


# ---------------------------------------------------------------------------
# int_orders__with_items
# ---------------------------------------------------------------------------

# Each order carries a total amount and a positive number of line items.
_order = st.fixed_dictionaries(
    {
        "total_amount_usd": st.decimals(
            min_value=Decimal("0.00"),
            max_value=Decimal("50000.00"),
            places=2,
            allow_nan=False,
            allow_infinity=False,
        ),
        "item_count": st.integers(min_value=1, max_value=10),
    }
)


@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(orders=st.lists(_order, min_size=1, max_size=25))
def test_int_orders_with_items_consistency(intermediate_conn, orders):
    conn = intermediate_conn
    ordered_at = datetime.combine(_RUN_DATE, time(12, 0), tzinfo=timezone.utc)

    order_rows = []
    item_rows = []
    expected: dict[str, tuple[int, Decimal]] = {}
    item_seq = 0
    for idx, o in enumerate(orders):
        order_id = _id(0, idx)
        total = o["total_amount_usd"]
        count = o["item_count"]
        order_rows.append((order_id, _id(2, idx), "delivered", total, ordered_at))
        for _ in range(count):
            item_rows.append((_id(1, item_seq), order_id))
            item_seq += 1
        # avg_item_value_usd is cast to numeric(10, 2): round half-up to 2 places.
        avg = (total / count).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        expected[order_id] = (count, avg)

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_SCHEMA}.stg_orders__orders")
        cur.execute(f"TRUNCATE {_SCHEMA}.stg_orders__order_items")
        cur.executemany(
            f"INSERT INTO {_SCHEMA}.stg_orders__orders"
            " (order_id, customer_id, order_status, total_amount_usd, ordered_at)"
            " VALUES (%s, %s, %s, %s, %s)",
            order_rows,
        )
        cur.executemany(
            f"INSERT INTO {_SCHEMA}.stg_orders__order_items"
            " (order_item_id, order_id) VALUES (%s, %s)",
            item_rows,
        )

        model_sql = render_int_orders_with_items(_RUN_DATE_ISO)
        cur.execute(
            "SELECT order_id, item_count, avg_item_value_usd, total_amount_usd"
            f" FROM (\n{model_sql}\n) AS m"
        )
        rows = cur.fetchall()

    assert len(rows) == len(orders), "one output row expected per order"

    for order_id, item_count, avg_item_value_usd, total_amount_usd in rows:
        exp_count, exp_avg = expected[order_id]
        assert item_count >= 1, f"{order_id}: item_count {item_count} < 1"
        assert item_count == exp_count, (
            f"{order_id}: item_count {item_count} != {exp_count} line items"
        )
        assert avg_item_value_usd == exp_avg, (
            f"{order_id}: avg_item_value_usd {avg_item_value_usd} != {exp_avg}"
        )
        # Cross-check the model's own arithmetic: avg == total / count (rounded).
        recomputed = (total_amount_usd / item_count).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        assert avg_item_value_usd == recomputed, (
            f"{order_id}: avg_item_value_usd {avg_item_value_usd} inconsistent with"
            f" total {total_amount_usd} / count {item_count}"
        )


# ---------------------------------------------------------------------------
# int_sessions__with_duration
# ---------------------------------------------------------------------------

# Each session is a set of event offsets (seconds) from a fixed session epoch.
_SESSION_EPOCH = datetime(2026, 6, 1, 8, 0, tzinfo=timezone.utc)
_session = st.lists(
    st.integers(min_value=0, max_value=86_400),
    min_size=1,
    max_size=12,
)


@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(sessions=st.lists(_session, min_size=1, max_size=20))
def test_int_sessions_duration_consistency(intermediate_conn, sessions):
    conn = intermediate_conn

    event_rows = []
    expected: dict[str, float] = {}
    for idx, offsets in enumerate(sessions):
        session_id = _id(5, idx)
        for offset in offsets:
            event_rows.append(
                (session_id, _SESSION_EPOCH + timedelta(seconds=offset))
            )
        expected[session_id] = float(max(offsets) - min(offsets))

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_SCHEMA}.stg_events__events")
        cur.executemany(
            f"INSERT INTO {_SCHEMA}.stg_events__events"
            " (session_id, occurred_at) VALUES (%s, %s)",
            event_rows,
        )

        model_sql = render_int_sessions_with_duration(_RUN_DATE_ISO)
        cur.execute(
            "SELECT session_id, session_start, session_end,"
            f" session_duration_seconds FROM (\n{model_sql}\n) AS m"
        )
        rows = cur.fetchall()

    assert len(rows) == len(sessions), "one output row expected per session"

    for session_id, session_start, session_end, duration in rows:
        duration = float(duration)
        assert duration >= 0, f"{session_id}: duration {duration} < 0"
        assert duration == expected[session_id], (
            f"{session_id}: session_duration_seconds {duration} !="
            f" {expected[session_id]}"
        )
        # Cross-check against the model's own start/end boundaries.
        span = (session_end - session_start).total_seconds()
        assert duration == span, (
            f"{session_id}: duration {duration} != end-start span {span}"
        )


# ---------------------------------------------------------------------------
# int_customer_orders__aggregated
# ---------------------------------------------------------------------------

# Each customer has a list of order day-offsets (days before the run date) with a
# spend amount. Offsets span the trailing-365-day boundary and the 30/60-day
# rolling windows so every FILTER branch is exercised.
_cust_order = st.tuples(
    st.integers(min_value=0, max_value=500),  # days before run date
    st.decimals(
        min_value=Decimal("0.00"),
        max_value=Decimal("10000.00"),
        places=2,
        allow_nan=False,
        allow_infinity=False,
    ),
)
_customer = st.lists(_cust_order, min_size=1, max_size=15)


@settings(
    max_examples=50,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(customers=st.lists(_customer, min_size=1, max_size=15))
def test_int_customer_orders_aggregated_windows(intermediate_conn, customers):
    conn = intermediate_conn

    order_rows = []
    expected: dict[str, dict[str, int]] = {}
    for idx, cust_orders in enumerate(customers):
        cid = _id(2, idx)
        freq_365 = last_30 = prior_30 = 0
        min_offset = None
        for offset_days, amount in cust_orders:
            ordered_at = datetime.combine(
                _RUN_DATE - timedelta(days=offset_days),
                time(12, 0),
                tzinfo=timezone.utc,
            )
            order_rows.append((cid, amount, ordered_at))
            # Recompute the model's date-based window predicates independently.
            if offset_days <= 365:
                freq_365 += 1
            if offset_days <= 30:
                last_30 += 1
            if 30 < offset_days <= 60:
                prior_30 += 1
            min_offset = offset_days if min_offset is None else min(min_offset, offset_days)
        expected[cid] = {
            "total_order_count": len(cust_orders),
            "order_frequency_365d": freq_365,
            "order_count_last_30d": last_30,
            "order_count_prior_30d": prior_30,
            "days_since_last_order": min_offset,
        }

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_SCHEMA}.int_orders__with_items")
        cur.executemany(
            f"INSERT INTO {_SCHEMA}.int_orders__with_items"
            " (customer_id, total_amount_usd, ordered_at) VALUES (%s, %s, %s)",
            order_rows,
        )

        model_sql = render_int_customer_orders_aggregated(_RUN_DATE_ISO)
        cur.execute(
            "SELECT customer_id, total_order_count, order_frequency_365d,"
            " order_count_last_30d, order_count_prior_30d, days_since_last_order"
            f" FROM (\n{model_sql}\n) AS m"
        )
        rows = cur.fetchall()

    assert len(rows) == len(customers), "one output row expected per customer"

    for (
        customer_id,
        total_order_count,
        order_frequency_365d,
        order_count_last_30d,
        order_count_prior_30d,
        days_since_last_order,
    ) in rows:
        exp = expected[customer_id]
        assert total_order_count == exp["total_order_count"], (
            f"{customer_id}: total_order_count {total_order_count}"
            f" != {exp['total_order_count']}"
        )
        assert order_frequency_365d == exp["order_frequency_365d"], (
            f"{customer_id}: order_frequency_365d {order_frequency_365d}"
            f" != {exp['order_frequency_365d']}"
        )
        assert order_count_last_30d == exp["order_count_last_30d"], (
            f"{customer_id}: order_count_last_30d {order_count_last_30d}"
            f" != {exp['order_count_last_30d']}"
        )
        assert order_count_prior_30d == exp["order_count_prior_30d"], (
            f"{customer_id}: order_count_prior_30d {order_count_prior_30d}"
            f" != {exp['order_count_prior_30d']}"
        )
        assert days_since_last_order == exp["days_since_last_order"], (
            f"{customer_id}: days_since_last_order {days_since_last_order}"
            f" != {exp['days_since_last_order']}"
        )
        # The rolling windows are subsets of the trailing-365-day window.
        assert order_count_last_30d <= order_frequency_365d
        assert order_count_prior_30d <= order_frequency_365d
