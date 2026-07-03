"""Pure feature transformations for the ML scoring pipeline (Task 25).

These helpers shape the raw ``mart_customer_360`` feature frame into model-ready
inputs. They implement the three transformations named in the design's *ML
Feature Engineering Strategy*:

* :func:`log_transform_monetary` — ``log1p`` the right-skewed monetary columns so
  linear/tree models see a better-conditioned target and features.
* :func:`onehot_encode` — expand categorical columns (``acquisition_channel``,
  ``segment_label``) into indicator columns for LTV / churn models.
* :func:`clip_outliers` — clip a column to ``[lower, upper]`` bounds (e.g. the
  recency cap).

Contract: every function returns a **new** ``DataFrame`` and never mutates its
input. Columns that are not the target of a transformation are preserved
unchanged and in their original order.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Sequence

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from numbers import Real

# Right-skewed monetary columns from mart_customer_360 that benefit from a log
# transform (Requirement 5.1 / design Feature Engineering Strategy).
MONETARY_COLUMNS: tuple[str, ...] = ("total_spend_365d_usd", "total_spend_usd")

# Categorical feature columns one-hot encoded for the LTV and churn models.
# ``segment_label`` is produced by the segmentation model and joined in before
# encoding; it is not present in the raw mart export.
CATEGORICAL_COLUMNS: tuple[str, ...] = ("acquisition_channel", "segment_label")

# Suffix appended to log-transformed columns so the original values are retained.
_LOG_SUFFIX = "_log1p"


def log_transform_monetary(
    df: pd.DataFrame,
    columns: Sequence[str] = MONETARY_COLUMNS,
) -> pd.DataFrame:
    """Add ``log1p``-transformed copies of skewed monetary columns.

    For each column ``c`` in ``columns`` a new column ``c_log1p`` is appended
    containing ``log(1 + c)``. ``log1p`` is used (rather than ``log``) so that a
    zero-spend customer maps to ``0.0`` instead of ``-inf``. The original columns
    are left untouched.

    Args:
        df: input feature frame.
        columns: monetary columns to transform. Defaults to
            :data:`MONETARY_COLUMNS`. Columns absent from ``df`` are skipped.

    Returns:
        A new ``DataFrame`` with the added ``*_log1p`` columns.
    """
    result = df.copy()
    for column in columns:
        if column not in result.columns:
            continue
        values = pd.to_numeric(result[column], errors="coerce")
        result[f"{column}{_LOG_SUFFIX}"] = np.log1p(values)
    return result


def onehot_encode(
    df: pd.DataFrame,
    columns: Sequence[str] = CATEGORICAL_COLUMNS,
    *,
    ignore_missing: bool = True,
    drop_first: bool = False,
) -> pd.DataFrame:
    """One-hot encode categorical ``columns`` into indicator columns.

    Each source column ``c`` is replaced by ``c_<value>`` indicator columns
    (``uint8``), matching :func:`pandas.get_dummies` naming. Encoded categories
    are sorted for deterministic column ordering across runs. Columns not listed
    in ``columns`` are preserved unchanged.

    Args:
        df: input feature frame.
        columns: categorical columns to encode. Defaults to
            :data:`CATEGORICAL_COLUMNS`.
        ignore_missing: when ``True`` (default), silently skip requested columns
            that are absent from ``df`` (e.g. ``segment_label`` before the
            segmentation model has run). When ``False``, a missing column raises.
        drop_first: drop the first indicator per column to avoid collinearity.

    Returns:
        A new ``DataFrame`` with the requested columns one-hot encoded.

    Raises:
        KeyError: if ``ignore_missing`` is ``False`` and a requested column is
            not present in ``df``.
    """
    present: list[str] = []
    for column in columns:
        if column in df.columns:
            present.append(column)
        elif not ignore_missing:
            raise KeyError(
                f"Column {column!r} requested for one-hot encoding is not present "
                f"in the DataFrame (columns: {list(df.columns)})."
            )

    if not present:
        return df.copy()

    return pd.get_dummies(
        df,
        columns=present,
        prefix=present,
        prefix_sep="_",
        drop_first=drop_first,
        dtype=np.uint8,
    )


def clip_outliers(
    df: pd.DataFrame,
    column: str,
    lower: Optional["Real"] = None,
    upper: Optional["Real"] = None,
) -> pd.DataFrame:
    """Clip a numeric ``column`` to the closed interval ``[lower, upper]``.

    Values below ``lower`` are raised to ``lower`` and values above ``upper`` are
    lowered to ``upper``. A ``None`` bound leaves that side unbounded. This is
    used, for example, to enforce the recency cap on ``recency_days``.

    Args:
        df: input feature frame.
        column: name of the column to clip.
        lower: inclusive lower bound, or ``None`` for no lower bound.
        upper: inclusive upper bound, or ``None`` for no upper bound.

    Returns:
        A new ``DataFrame`` with ``column`` clipped in place of the original.

    Raises:
        KeyError: if ``column`` is not present in ``df``.
        ValueError: if both bounds are ``None`` or ``lower > upper``.
    """
    if column not in df.columns:
        raise KeyError(f"Column {column!r} is not present in the DataFrame.")
    if lower is None and upper is None:
        raise ValueError("At least one of `lower` or `upper` must be provided.")
    if lower is not None and upper is not None and lower > upper:
        raise ValueError(f"lower ({lower}) must not exceed upper ({upper}).")

    result = df.copy()
    result[column] = pd.to_numeric(result[column], errors="coerce").clip(
        lower=lower, upper=upper
    )
    return result
