"""Feature engineering for the ML scoring pipeline (Task 25).

This sub-package provides the shared feature-engineering utilities used by every
downstream model (segmentation, LTV, churn, anomaly, NLP). Per the design's *ML
Feature Engineering Strategy*, ``marts.mart_customer_360`` is the single feature
source; it is exported through an in-process DuckDB database to a Parquet-backed
relation and returned to model code as a pandas ``DataFrame``.

Public API
----------
* :func:`ml.features.feature_store.load_customer_features` — load the per-customer
  feature matrix for a run date.
* :func:`ml.features.transformers.log_transform_monetary` — log1p skewed spend.
* :func:`ml.features.transformers.onehot_encode` — one-hot encode categoricals.
* :func:`ml.features.transformers.clip_outliers` — clip a column to bounds.
"""

from __future__ import annotations

from ml.features.feature_store import (
    FEATURE_COLUMNS,
    load_customer_features,
)
from ml.features.transformers import (
    CATEGORICAL_COLUMNS,
    MONETARY_COLUMNS,
    clip_outliers,
    log_transform_monetary,
    onehot_encode,
)

__all__ = [
    "FEATURE_COLUMNS",
    "load_customer_features",
    "CATEGORICAL_COLUMNS",
    "MONETARY_COLUMNS",
    "clip_outliers",
    "log_transform_monetary",
    "onehot_encode",
]
