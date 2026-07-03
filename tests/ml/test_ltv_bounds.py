"""Property test: LTV model output bounds (Task 29).

**Property 8 (partial) — LTV Score Is Non-Negative.**

Validates Requirements 6.2, 6.6, and 6.7 for
``ml.models.ltv.train_and_score``:

* **6.2** — every customer present in the feature frame receives a
  non-negative, non-null ``ltv_score`` (minimum ``0.00``), across spend
  histories that include zeros and very large values.
* **6.6** — a customer with no order history whose ``acquisition_channel``
  cohort has at least one training observation receives that cohort's mean LTV
  (never ``0`` when the cohort mean is positive, never null).
* **6.7** — a customer with no order history whose cohort has no training
  observation receives exactly ``0.00``.

Shape invariants (the model's return contract) are asserted throughout: exactly
one output row per input customer, in input ``customer_id`` order.

Approach
--------
``hypothesis`` builds ``mart_customer_360``-shaped feature frames mixing
order-history customers (varied, including zero and very large spend) with
cold-start customers (no order history). The **real**
``ml.models.ltv.train_and_score`` is executed against each frame; the test only
asserts invariants on the model's own output and never reimplements its scoring.

No MLflow, PostgreSQL, DuckDB, Docker, or network access is required: the model
runs as a pure function over an in-memory DataFrame and the ``no_mlflow_ltv``
fixture stubs its MLflow logging so a tracking-server outage (or its absence)
can never affect or slow the run.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

# Constants are read from the model under test so the test tracks the model's
# own contract (target/cohort columns, the LTV floor, the split threshold)
# rather than a hand-copied duplicate that could drift.
from ml.models.ltv import (
    ALLTIME_ORDER_COLUMN,
    ALLTIME_SPEND_COLUMN,
    CHANNEL_COLUMN,
    MIN_LTV,
    MIN_SAMPLES_FOR_SPLIT,
    TARGET_COLUMN,
)

_RUN_DATE = "2026-07-01"

# A small, fixed channel vocabulary so cold-start customers can reliably share
# (or not share) an acquisition-channel cohort with order-history customers.
_CHANNELS = ["organic", "paid_search", "social", "referral", "direct"]

# Spend values deliberately span zero and very large magnitudes (Requirement 6.2
# asks for both). Kept finite and non-negative — the raw source column is a
# NUMERIC(14,2) >= 0.
_spend = st.one_of(
    st.just(0.0),
    st.floats(min_value=0.0, max_value=5_000_000.0, allow_nan=False, allow_infinity=False),
)


@pytest.fixture
def no_mlflow_ltv(monkeypatch):
    """Neutralise the LTV model's MLflow logging.

    ``ml.models.ltv`` already swallows MLflow errors (MLflow is best-effort
    infrastructure), but stubbing ``_log_to_mlflow`` keeps the tests fast and
    deterministic and guarantees they never reach a tracking server or write a
    local ``mlruns/`` directory — satisfying the "do not require MLflow" rule.
    """
    from ml.models import ltv

    monkeypatch.setattr(ltv, "_log_to_mlflow", lambda *a, **k: None)
    return ltv


def _customer_id(index: int) -> str:
    """Deterministic UUID-shaped id that fits ``VARCHAR(36)``."""
    return f"00000000-0000-4000-8000-{index:012d}"


def _history_row(index: int, channel: str, spend_365: float, spend_all: float) -> Dict:
    """A customer with order history: ``total_order_count >= 1``."""
    return {
        "customer_id": _customer_id(index),
        CHANNEL_COLUMN: channel,
        "order_frequency_365d": 1,
        "customer_tenure_days": 365,
        TARGET_COLUMN: spend_365,
        # All-time spend is at least the trailing-365-day spend by construction.
        ALLTIME_SPEND_COLUMN: max(spend_all, spend_365),
        ALLTIME_ORDER_COLUMN: 1,
    }


def _coldstart_row(index: int, channel: str) -> Dict:
    """A customer with no order history: ``total_order_count == 0``."""
    return {
        "customer_id": _customer_id(index),
        CHANNEL_COLUMN: channel,
        "order_frequency_365d": 0,
        "customer_tenure_days": 30,
        TARGET_COLUMN: 0.0,
        ALLTIME_SPEND_COLUMN: 0.0,
        ALLTIME_ORDER_COLUMN: 0,
    }


def _assert_output_contract(result: pd.DataFrame, features: pd.DataFrame) -> None:
    """Shape + bounds invariants shared by every case (Requirement 6.2)."""
    # Exactly one output row per input customer, preserving input order.
    assert len(result) == len(features), "expected one output row per customer"
    assert list(result["customer_id"]) == list(features["customer_id"]), (
        "output customer_ids must match input rows in order"
    )
    scores = result["ltv_score"]
    # Non-null and non-negative for every customer.
    assert scores.notna().all(), "ltv_score must never be null"
    assert (scores >= MIN_LTV).all(), (
        f"every ltv_score must be >= {MIN_LTV}; got min {scores.min()}"
    )


@st.composite
def _population(draw) -> List[Dict]:
    """A shuffled mix of order-history and cold-start customers."""
    n_history = draw(st.integers(min_value=0, max_value=12))
    n_coldstart = draw(st.integers(min_value=0, max_value=12))
    assume(n_history + n_coldstart >= 1)  # at least one customer to score

    rows: List[Dict] = []
    idx = 0
    for _ in range(n_history):
        channel = draw(st.sampled_from(_CHANNELS))
        spend_365 = draw(_spend)
        spend_all = draw(_spend)
        rows.append(_history_row(idx, channel, spend_365, spend_all))
        idx += 1
    for _ in range(n_coldstart):
        channel = draw(st.sampled_from(_CHANNELS))
        rows.append(_coldstart_row(idx, channel))
        idx += 1

    return draw(st.permutations(rows))


@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(customers=_population())
def test_ltv_score_non_negative_and_shape(no_mlflow_ltv, customers):
    """Every customer gets a non-null, non-negative score; shape preserved."""
    ltv = no_mlflow_ltv
    features = pd.DataFrame(customers)

    result = ltv.train_and_score(features, _RUN_DATE)

    _assert_output_contract(result, features)


@settings(deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    # A handful of order-history customers in one channel, each with a strictly
    # positive trailing-365-day spend, so the channel's cohort mean is > 0.
    history_spends=st.lists(
        st.floats(min_value=1.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=MIN_SAMPLES_FOR_SPLIT - 1,
    ),
)
def test_cold_start_with_cohort_returns_cohort_mean(no_mlflow_ltv, history_spends):
    """No-history customer + cohort present => the cohort's mean LTV (6.6).

    The order-history block is kept below :data:`MIN_SAMPLES_FOR_SPLIT` so the
    model trains in-sample (no held-out split); the ``organic`` cohort mean is
    then exactly the mean of the block's trailing-365-day spends, and the
    cold-start ``organic`` customer must receive that value — never ``0`` and
    never null.
    """
    ltv = no_mlflow_ltv
    cohort = "organic"

    rows: List[Dict] = [
        _history_row(i, cohort, spend, spend) for i, spend in enumerate(history_spends)
    ]
    coldstart_id = _customer_id(len(rows))
    rows.append(_coldstart_row(len(rows), cohort))
    features = pd.DataFrame(rows)

    result = ltv.train_and_score(features, _RUN_DATE)
    _assert_output_contract(result, features)

    expected_mean = float(pd.Series(history_spends).clip(lower=MIN_LTV).mean())
    score = result.set_index("customer_id").loc[coldstart_id, "ltv_score"]
    assert score == pytest.approx(expected_mean, rel=1e-6, abs=1e-6), (
        f"cold-start cohort customer should get cohort mean {expected_mean}, got {score}"
    )
    # A positive cohort mean is never collapsed to the 0.00 floor.
    assert score > MIN_LTV, "positive cohort mean must not be flattened to 0"


@settings(deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(
    history_spends=st.lists(
        st.floats(min_value=1.0, max_value=1_000_000.0, allow_nan=False, allow_infinity=False),
        min_size=1,
        max_size=MIN_SAMPLES_FOR_SPLIT - 1,
    ),
)
def test_cold_start_without_cohort_returns_zero(no_mlflow_ltv, history_spends):
    """No-history customer + no cohort => exactly 0.00 (6.7).

    All order-history customers sit in ``organic``; the cold-start customer sits
    in ``referral``, a channel with no training observation, so its baseline is
    the ``0.00`` floor rather than null or a borrowed mean.
    """
    ltv = no_mlflow_ltv

    rows: List[Dict] = [
        _history_row(i, "organic", spend, spend) for i, spend in enumerate(history_spends)
    ]
    coldstart_id = _customer_id(len(rows))
    rows.append(_coldstart_row(len(rows), "referral"))
    features = pd.DataFrame(rows)

    result = ltv.train_and_score(features, _RUN_DATE)
    _assert_output_contract(result, features)

    score = result.set_index("customer_id").loc[coldstart_id, "ltv_score"]
    assert score == pytest.approx(float(MIN_LTV)), (
        f"cold-start customer with no cohort should get {MIN_LTV}, got {score}"
    )


@settings(deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(n_coldstart=st.integers(min_value=1, max_value=40))
def test_all_cold_start_population_scores_zero(no_mlflow_ltv, n_coldstart):
    """An all-cold-start population (no training data) scores every row 0.00.

    With no order-history customers there are no cohort means to derive, so every
    customer keeps the ``0.00`` floor — still non-null and non-negative (6.7).
    """
    ltv = no_mlflow_ltv
    rows = [_coldstart_row(i, _CHANNELS[i % len(_CHANNELS)]) for i in range(n_coldstart)]
    features = pd.DataFrame(rows)

    result = ltv.train_and_score(features, _RUN_DATE)
    _assert_output_contract(result, features)

    assert (result["ltv_score"] == float(MIN_LTV)).all(), (
        "all cold-start, no cohort => every ltv_score is the 0.00 floor"
    )
