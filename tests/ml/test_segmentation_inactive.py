"""Property test: segmentation model inactive customers (Task 27).

**Property 7 — Inactive Customers Receive the Correct Default Scores.**

Validates Requirement 5.5 (and, in passing, 5.4):

* Every customer with ``order_frequency_365d == 0`` receives
  ``segment_label == "Inactive"``.
* Every *active* customer (``order_frequency_365d >= 1``) receives a non-empty
  ``segment_label`` that is **not** ``"Inactive"``.
* The model emits exactly one output row per input customer, in input order.
* The set of distinct labels produced on any run stays within the valid range of
  4–9 labels (the k ∈ {4..8} cluster range plus the reserved ``"Inactive"``).

Approach
--------
``hypothesis`` builds customer feature frames with varying proportions of
inactive customers — from all-inactive to a healthy active majority — and the
**real** ``ml.models.segmentation.train_and_score`` is executed against each. The
test never reimplements the labelling logic; it only asserts the invariants on
the model's own output.

No MLflow, PostgreSQL, Docker, or network access is required: the model operates
on an in-memory DataFrame and the ``no_mlflow`` fixture stubs its MLflow logging
(see ``conftest``).
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

# The activity threshold column and the reserved label are read from the model
# under test so the test tracks the model's own contract rather than a copy.
from ml.models.segmentation import ACTIVITY_COLUMN, INACTIVE_LABEL, K_RANGE, RFM_COLUMNS

_RUN_DATE = "2026-07-01"

# Valid distinct-label count: the cluster range {4..8} for active customers plus
# the reserved "Inactive" label => between min(K_RANGE) and max(K_RANGE)+1.
_MIN_LABELS = min(K_RANGE)
_MAX_LABELS = max(K_RANGE) + 1

# k-means needs at least ``min(K_RANGE)`` active customers before any k in
# K_RANGE satisfies ``k < n_samples`` and a clustering can be scored. Active
# populations are therefore generated as either empty or at least this large.
_MIN_ACTIVE = min(K_RANGE) + 1


def _customer_id(index: int) -> str:
    """Deterministic UUID-shaped id that fits ``VARCHAR(36)``."""
    return f"00000000-0000-4000-8000-{index:012d}"


_rfm_tuple = st.tuples(
    st.integers(min_value=1, max_value=5),
    st.integers(min_value=1, max_value=5),
    st.integers(min_value=1, max_value=5),
)


@st.composite
def _population(draw) -> List[Dict]:
    """A shuffled mix of active and inactive customers.

    ``n_active`` is drawn as either 0 or ``>= _MIN_ACTIVE`` so that, whenever
    active customers exist, there are always enough of them for the k-means model
    to form its minimum number of clusters. Active customers carry an RFM profile
    in the 1–5 quintile space; inactive customers carry none (their RFM columns
    are ignored by the model, which filters on ``order_frequency_365d``).
    """
    n_active = draw(
        st.one_of(st.just(0), st.integers(min_value=_MIN_ACTIVE, max_value=40))
    )
    n_inactive = draw(st.integers(min_value=0, max_value=30))
    assume(n_active + n_inactive >= 1)  # at least one customer to score

    actives = draw(st.lists(_rfm_tuple, min_size=n_active, max_size=n_active))
    customers: List[Dict] = [{"active": True, "rfm": rfm} for rfm in actives]
    customers += [{"active": False, "rfm": (0, 0, 0)} for _ in range(n_inactive)]
    return draw(st.permutations(customers))


def _build_features(customers: List[Dict]) -> pd.DataFrame:
    """Turn a population list into a ``mart_customer_360``-shaped feature frame."""
    rows = []
    for idx, c in enumerate(customers):
        r, f, m = c["rfm"]
        rows.append(
            {
                "customer_id": _customer_id(idx),
                # order_frequency_365d is the sole activity signal the model uses:
                # 0 => Inactive; any positive value => active.
                ACTIVITY_COLUMN: (f if c["active"] else 0),
                RFM_COLUMNS[0]: r,
                RFM_COLUMNS[1]: f,
                RFM_COLUMNS[2]: m,
            }
        )
    return pd.DataFrame(rows)


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(customers=_population())
def test_inactive_and_active_segment_labels(no_mlflow, customers):
    """Inactive => "Inactive"; active => a non-empty, non-Inactive label."""
    segmentation = no_mlflow
    features = _build_features(customers)

    # Distinct active RFM profiles must be >= 2 for a silhouette to be defined;
    # this only rejects the rare degenerate case where every active customer
    # shares one profile, which is not what this property is about.
    active = features[features[ACTIVITY_COLUMN] > 0]
    if len(active) > 0:
        assume(active[list(RFM_COLUMNS)].drop_duplicates().shape[0] >= 2)

    result = segmentation.train_and_score(features, _RUN_DATE)

    # Exactly one output row per input customer, preserving input order.
    assert len(result) == len(features), "expected one output row per customer"
    assert list(result["customer_id"]) == list(features["customer_id"]), (
        "output customer_ids must match input rows in order"
    )

    freq_by_id = dict(zip(features["customer_id"], features[ACTIVITY_COLUMN]))
    for customer_id, label in zip(result["customer_id"], result["segment_label"]):
        if freq_by_id[customer_id] == 0:
            # Requirement 5.5: zero trailing-365-day orders => Inactive.
            assert label == INACTIVE_LABEL, (
                f"{customer_id}: inactive but got label {label!r}"
            )
        else:
            # Requirement 5.4: active customers get a real, non-Inactive label.
            assert isinstance(label, str) and label != "", (
                f"{customer_id}: active but label is empty"
            )
            assert label != INACTIVE_LABEL, (
                f"{customer_id}: active but got the reserved Inactive label"
            )

    # The reserved label appears iff at least one inactive customer was present.
    labels = set(result["segment_label"])
    has_inactive = any(v == 0 for v in freq_by_id.values())
    assert (INACTIVE_LABEL in labels) == has_inactive

    # Distinct labels never exceed the valid ceiling (k max + Inactive).
    assert len(labels) <= _MAX_LABELS, f"too many distinct labels: {labels}"


@settings(deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(n_inactive=st.integers(min_value=1, max_value=50))
def test_all_inactive_population(no_mlflow, n_inactive):
    """An all-inactive population is labelled entirely ``Inactive``."""
    segmentation = no_mlflow
    features = _build_features([{"active": False, "rfm": (0, 0, 0)}] * n_inactive)

    result = segmentation.train_and_score(features, _RUN_DATE)

    assert len(result) == n_inactive, "one output row per customer"
    assert set(result["segment_label"]) == {INACTIVE_LABEL}


@settings(
    max_examples=15,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(n_inactive=st.integers(min_value=1, max_value=30))
def test_distinct_label_count_in_valid_range(no_mlflow, n_inactive):
    """Distinct labels fall in [4, 9]: k active clusters plus ``Inactive``.

    A well-spread active block (the full 5x5x5 RFM quintile grid) guarantees the
    model can form and fill its chosen number of clusters, so the distinct active
    labels equal k ∈ {4..8}; adding the always-present inactive customers keeps
    the total within the 4–9 range asserted by Property 7.
    """
    segmentation = no_mlflow

    customers: List[Dict] = [
        {"active": True, "rfm": (r, f, m)}
        for r in range(1, 6)
        for f in range(1, 6)
        for m in range(1, 6)
    ]
    customers += [{"active": False, "rfm": (0, 0, 0)} for _ in range(n_inactive)]
    features = _build_features(customers)

    result = segmentation.train_and_score(features, _RUN_DATE)

    labels = set(result["segment_label"])
    assert INACTIVE_LABEL in labels, "inactive customers were present"
    assert _MIN_LABELS <= len(labels) <= _MAX_LABELS, (
        f"distinct label count {len(labels)} outside [{_MIN_LABELS}, {_MAX_LABELS}]"
    )
