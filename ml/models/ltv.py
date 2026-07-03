"""Customer Lifetime Value (LTV) scoring model (Task 28).

This module implements the LTV_Model described in the design's *AI/ML
Architecture* and Requirement 6. It predicts a **12-month forward LTV in USD**
for every customer in ``mart_customer_360`` and returns a non-negative
``ltv_score`` per customer.

Behaviour (Requirements 6.1–6.7):

* **Target** — ``total_spend_365d_usd`` is used as the training target: the
  customer's trailing 365-day spend serves as the historical proxy for 12-month
  forward LTV (design "LTV Model" table). Predictions are therefore in USD.
* **Features** — derived from ``mart_customer_360`` (Requirement 6.1):
  ``order_frequency_365d``, ``avg_order_value`` (derived safely from all-time
  spend / order count — see :func:`_derive_avg_order_value`), ``customer_tenure_days``,
  ``acquisition_channel`` (one-hot) and, when present, ``segment_label`` (one-hot).
* **Model** — ``GradientBoostingRegressor`` trained on customers that have an
  order history. An 80/20 train/validation split stratified by
  ``acquisition_channel`` is used whenever there are enough samples to hold out
  at least 20% (Requirement 6.3); the split falls back to a plain random split if
  stratification is not possible, and is skipped entirely for very small inputs.
* **Non-negativity** — predictions are clipped with ``np.clip(predictions, 0,
  None)`` so every ``ltv_score`` is ≥ ``0.00`` (Requirement 6.2).
* **Cold start** — a customer with no order history is not scored by the model.
  If its ``acquisition_channel`` cohort has at least one training observation it
  receives that cohort's mean LTV (Requirement 6.6); otherwise it receives
  ``0.00`` (Requirement 6.7). Never null.
* **MLflow** — logs ``algorithm``, ``n_estimators``, ``learning_rate``,
  ``feature_list``, ``train_date_range``, validation ``rmse`` / ``mae`` and a run
  timestamp to the ``customer_ltv`` experiment (Requirement 6.3). The new model
  is registered under ``customer_ltv`` and promoted to ``production`` only if its
  RMSE is strictly lower than the current production model's; otherwise the
  current production model is retained and the comparison is logged
  (Requirement 6.4).

``train_and_score`` returns a ``DataFrame`` with ``customer_id`` and
``ltv_score`` columns, one row per input customer, in input order.

MLflow is treated as best-effort infrastructure: an MLflow outage is logged as a
warning but never fails scoring. The returned ``ltv_score`` values do not depend
on MLflow availability, consistent with the platform's error-handling design.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

if TYPE_CHECKING:  # Keep runtime imports light for Airflow's DAG parser.
    from sklearn.ensemble import GradientBoostingRegressor

# The pipeline/Airflow task logger — the comparison and fallback warnings are
# surfaced in the Orchestrator run log via this logger (Requirement 6.4).
log = logging.getLogger("cip.pipeline")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
ALGORITHM = "gradient_boosting_regressor"
RANDOM_STATE = 42
N_ESTIMATORS = 200
LEARNING_RATE = 0.05
MAX_DEPTH = 3

#: Fraction of samples held out for validation (Requirement 6.3 — ≥ 20%).
VALIDATION_FRACTION = 0.20

#: Below this many order-history customers, the held-out split is skipped and
#: metrics are reported in-sample (there are too few rows to hold out a
#: meaningful 20% validation set). ``5`` guarantees ≥ 1 validation row at 20%.
MIN_SAMPLES_FOR_SPLIT = 5

#: Target column — trailing 365-day spend as the 12-month forward LTV proxy.
TARGET_COLUMN = "total_spend_365d_usd"

#: Base numeric feature columns fed to the regressor (before one-hot encoding).
#: ``avg_order_value`` is derived (see :func:`_derive_avg_order_value`).
BASE_NUMERIC_FEATURES: Tuple[str, ...] = (
    "order_frequency_365d",
    "avg_order_value",
    "customer_tenure_days",
)

#: Categorical feature columns one-hot encoded when present. ``segment_label`` is
#: produced by the segmentation model and may be joined in upstream; it is
#: optional and used only if the caller supplies it (Requirement 6.1).
CATEGORICAL_FEATURES: Tuple[str, ...] = ("acquisition_channel", "segment_label")

#: Column identifying a customer's acquisition-channel cohort for cold-start.
CHANNEL_COLUMN = "acquisition_channel"

#: All-time spend / order-count columns used to derive ``avg_order_value`` and to
#: identify customers with no order history (``total_order_count == 0``).
ALLTIME_SPEND_COLUMN = "total_spend_usd"
ALLTIME_ORDER_COLUMN = "total_order_count"

#: Columns that must be present on the input frame (``segment_label`` is optional).
REQUIRED_COLUMNS: Tuple[str, ...] = (
    "customer_id",
    CHANNEL_COLUMN,
    "order_frequency_365d",
    "customer_tenure_days",
    TARGET_COLUMN,
    ALLTIME_SPEND_COLUMN,
    ALLTIME_ORDER_COLUMN,
)

#: Cold-start / minimum LTV floor (Requirements 6.2, 6.7).
MIN_LTV = 0.0

#: MLflow experiment and registered-model name (design MLflow Registry table).
EXPERIMENT_NAME = "customer_ltv"
REGISTERED_MODEL_NAME = "customer_ltv"
PRODUCTION_STAGE = "production"


def _coerce_run_date(run_date: Union[str, "date", "datetime"]) -> date:
    """Coerce ``run_date`` to a :class:`datetime.date`.

    Accepts a ``date``, a ``datetime``, or a strict ISO ``YYYY-MM-DD`` string.
    Mirrors the coercion used by :mod:`ml.features.feature_store` so callers can
    pass the same run-date value through the pipeline.
    """
    if isinstance(run_date, datetime):
        return run_date.date()
    if isinstance(run_date, date):
        return run_date
    if isinstance(run_date, str):
        return date.fromisoformat(run_date)
    raise TypeError(
        "run_date must be a datetime.date, datetime.datetime, or an ISO "
        f"'YYYY-MM-DD' string; got {type(run_date).__name__}."
    )


def _numeric(series: pd.Series) -> pd.Series:
    """Coerce ``series`` to numeric, mapping unparseable values to ``0``."""
    return pd.to_numeric(series, errors="coerce").fillna(0.0)


def _derive_avg_order_value(df: pd.DataFrame) -> pd.Series:
    """Derive per-customer average order value in USD, safe against zero orders.

    ``mart_customer_360`` does not expose an average-order-value column, so it is
    derived from all-time totals: ``total_spend_usd / total_order_count``. The
    all-time ratio (rather than the trailing-365-day ratio) is used deliberately
    to avoid target leakage — the target ``total_spend_365d_usd`` equals
    ``order_frequency_365d * avg_order_value_365d`` by construction, so a 365-day
    average would reconstruct the target. Customers with zero all-time orders get
    ``0.0`` (they are handled by the cold-start path and never reach the model).
    """
    spend = _numeric(df[ALLTIME_SPEND_COLUMN])
    orders = _numeric(df[ALLTIME_ORDER_COLUMN])
    # Divide only where orders > 0; NULLIF-style guard against divide-by-zero.
    avg = np.divide(
        spend.to_numpy(dtype=float),
        orders.to_numpy(dtype=float),
        out=np.zeros(len(df), dtype=float),
        where=orders.to_numpy(dtype=float) > 0,
    )
    return pd.Series(avg, index=df.index)


def _build_feature_matrix(
    model_rows: pd.DataFrame,
) -> Tuple[pd.DataFrame, List[str]]:
    """Assemble the one-hot encoded feature matrix for the model rows.

    Returns the numeric feature ``DataFrame`` (all columns float) and the ordered
    ``feature_list`` used for MLflow logging. Encoding the full model population in
    one call guarantees the train and predict matrices share identical columns.
    """
    from ml.features import onehot_encode

    base = pd.DataFrame(index=model_rows.index)
    base["order_frequency_365d"] = _numeric(model_rows["order_frequency_365d"])
    base["avg_order_value"] = _derive_avg_order_value(model_rows)
    base["customer_tenure_days"] = _numeric(model_rows["customer_tenure_days"])

    # Attach the categorical columns that are actually present, then one-hot them.
    categoricals = [c for c in CATEGORICAL_FEATURES if c in model_rows.columns]
    for column in categoricals:
        base[column] = model_rows[column].astype("string").fillna("unknown")

    encoded = onehot_encode(base, columns=categoricals, ignore_missing=True)

    # All feature columns are cast to float for a stable, model-friendly dtype.
    encoded = encoded.astype(float)
    feature_list = list(encoded.columns)
    return encoded, feature_list


def _split_train_val(
    features: pd.DataFrame,
    target: pd.Series,
    stratify_on: pd.Series,
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]]:
    """Split into train/validation, stratified by ``stratify_on`` when possible.

    Returns ``(X_train, X_val, y_train, y_val)`` or ``None`` when there are too
    few samples to hold out a meaningful validation set
    (:data:`MIN_SAMPLES_FOR_SPLIT`). A stratified split is attempted first
    (Requirement 6.3); if stratification is infeasible (e.g. a channel with a
    single member) it falls back to a plain random split.
    """
    from sklearn.model_selection import train_test_split

    if len(features) < MIN_SAMPLES_FOR_SPLIT:
        return None

    try:
        return train_test_split(
            features,
            target,
            test_size=VALIDATION_FRACTION,
            random_state=RANDOM_STATE,
            stratify=stratify_on,
        )
    except ValueError:
        # Stratification failed (a class too small for the requested split);
        # fall back to an unstratified split so validation still happens.
        log.warning(
            "LTV: stratified split by %s not possible; using a random 80/20 "
            "split instead.",
            CHANNEL_COLUMN,
        )
        return train_test_split(
            features,
            target,
            test_size=VALIDATION_FRACTION,
            random_state=RANDOM_STATE,
        )


def _fit_regressor(
    X_train: pd.DataFrame, y_train: pd.Series
) -> "GradientBoostingRegressor":
    """Fit a deterministic ``GradientBoostingRegressor`` on the training rows."""
    from sklearn.ensemble import GradientBoostingRegressor

    model = GradientBoostingRegressor(
        n_estimators=N_ESTIMATORS,
        learning_rate=LEARNING_RATE,
        max_depth=MAX_DEPTH,
        random_state=RANDOM_STATE,
    )
    model.fit(X_train, y_train)
    return model


def _rmse_mae(y_true: pd.Series, y_pred: np.ndarray) -> Tuple[float, float]:
    """Return ``(rmse, mae)`` for predictions ``y_pred`` against ``y_true``."""
    from sklearn.metrics import mean_absolute_error, mean_squared_error

    # np.sqrt(mean_squared_error(...)) is used rather than squared=False for
    # compatibility across scikit-learn versions.
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    mae = float(mean_absolute_error(y_true, y_pred))
    return rmse, mae


def _train_date_range(run_date: date) -> str:
    """Human-readable training date range: the trailing 365-day target window."""
    start = run_date - timedelta(days=365)
    return f"{start.isoformat()}/{run_date.isoformat()}"


def _production_rmse(client, model_name: str) -> Optional[float]:
    """Return the RMSE of the current production model, or ``None`` if absent.

    Reads the ``rmse`` metric of the registered version currently holding the
    ``production`` stage. Any lookup problem is treated as "no production RMSE"
    so a fresh registry (or an unreadable run) does not block registration.
    """
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
    except Exception:  # noqa: BLE001 — registry unreadable; treat as no prod.
        return None

    prod = [
        mv
        for mv in versions
        if PRODUCTION_STAGE.lower() == (mv.current_stage or "").lower()
    ]
    if not prod:
        return None

    # If multiple somehow hold the tag, compare against the best (lowest) RMSE.
    rmses: List[float] = []
    for mv in prod:
        try:
            metrics = client.get_run(mv.run_id).data.metrics
            if "rmse" in metrics:
                rmses.append(float(metrics["rmse"]))
        except Exception:  # noqa: BLE001 — skip unreadable versions.
            continue
    return min(rmses) if rmses else None


def _promote_latest_to_production(client, model_name: str) -> None:
    """Transition the newest registered version to ``production`` (archive rest)."""
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        return
    latest = max(versions, key=lambda mv: int(mv.version))
    client.transition_model_version_stage(
        name=model_name,
        version=latest.version,
        stage=PRODUCTION_STAGE,
        archive_existing_versions=True,
    )
    log.info(
        "Promoted %s version %s to %s.",
        model_name,
        latest.version,
        PRODUCTION_STAGE,
    )


def _log_to_mlflow(
    model: "GradientBoostingRegressor",
    rmse: Optional[float],
    mae: Optional[float],
    in_sample: bool,
    training_sample_count: int,
    feature_list: List[str],
    run_date: date,
    mlflow_tracking_uri: Optional[str],
) -> None:
    """Log params/metrics and conditionally register+promote the LTV model.

    Best-effort: any MLflow error is caught and logged as a warning so a
    tracking/registry outage never fails scoring (design error handling). Logs
    ``algorithm``, ``n_estimators``, ``learning_rate``, ``feature_list``,
    ``train_date_range``, ``rmse`` / ``mae`` and a run timestamp (Requirement
    6.3). Registration + promotion happens only when the new RMSE is strictly
    lower than the current production RMSE (Requirement 6.4).
    """
    try:
        import mlflow
        import mlflow.sklearn
        from mlflow.tracking import MlflowClient

        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment(EXPERIMENT_NAME)

        client = MlflowClient(tracking_uri=mlflow_tracking_uri or None)
        prod_rmse = _production_rmse(client, REGISTERED_MODEL_NAME)
        # Register when there is no production model yet, or the new model's RMSE
        # is strictly lower (Requirement 6.4). Without a validation RMSE we cannot
        # justify replacing production, so we log the run but do not register.
        should_register = (
            rmse is not None
            and (prod_rmse is None or rmse < prod_rmse)
        )

        with mlflow.start_run(run_name=f"ltv_{run_date.isoformat()}"):
            mlflow.log_param("algorithm", ALGORITHM)
            mlflow.log_param("n_estimators", N_ESTIMATORS)
            mlflow.log_param("learning_rate", LEARNING_RATE)
            mlflow.log_param("max_depth", MAX_DEPTH)
            mlflow.log_param("feature_list", json.dumps(feature_list))
            mlflow.log_param("train_date_range", _train_date_range(run_date))
            mlflow.log_param("validation", "in_sample" if in_sample else "holdout")
            mlflow.log_metric("training_sample_count", training_sample_count)
            if rmse is not None:
                mlflow.log_metric("rmse", rmse)
            if mae is not None:
                mlflow.log_metric("mae", mae)
            # Run timestamp (Requirement 6.3). MLflow records start_time, but the
            # design lists an explicit timestamp among the logged fields.
            mlflow.set_tag("run_timestamp", datetime.now(timezone.utc).isoformat())
            mlflow.set_tag("run_date", run_date.isoformat())

            if should_register:
                mlflow.sklearn.log_model(
                    model,
                    artifact_path="model",
                    registered_model_name=REGISTERED_MODEL_NAME,
                )
            else:
                # Log the artifact for traceability but do not register it.
                mlflow.sklearn.log_model(model, artifact_path="model")

        if should_register:
            _promote_latest_to_production(client, REGISTERED_MODEL_NAME)
            log.info(
                "LTV: registered + promoted new model (RMSE=%.4f%s).",
                rmse,
                "" if prod_rmse is None else f" < production RMSE={prod_rmse:.4f}",
            )
        else:
            log.info(
                "LTV: retaining current production model; new RMSE=%s not lower "
                "than production RMSE=%s.",
                "n/a" if rmse is None else f"{rmse:.4f}",
                "n/a" if prod_rmse is None else f"{prod_rmse:.4f}",
            )
    except Exception:  # noqa: BLE001 — MLflow outage must not fail scoring.
        log.warning(
            "MLflow unavailable; LTV run was not logged/registered.",
            exc_info=True,
        )


def train_and_score(
    features_df: pd.DataFrame,
    run_date: Union[str, "date", "datetime"],
    mlflow_tracking_uri: Optional[str] = None,
) -> pd.DataFrame:
    """Train the LTV model and assign a non-negative ``ltv_score`` per customer.

    Args:
        features_df: per-customer feature frame from ``mart_customer_360`` (as
            returned by :func:`ml.features.load_customer_features`). Must contain
            :data:`REQUIRED_COLUMNS`; ``segment_label`` is used when present.
        run_date: the pipeline run date (``date``/``datetime`` or ISO string);
            used for MLflow run naming/tagging and the training date range.
        mlflow_tracking_uri: MLflow tracking URI. When omitted, MLflow uses its
            ambient configuration (e.g. the ``MLFLOW_TRACKING_URI`` env var).

    Returns:
        A ``DataFrame`` with columns ``customer_id`` and ``ltv_score`` — one row
        per input customer, in input order. Every ``ltv_score`` is ≥ ``0.00``.
        Customers with no order history receive their acquisition-channel cohort
        mean (or ``0.00`` if the cohort has no training observations).

    Raises:
        KeyError: if a required column is missing from ``features_df``.
        TypeError: if ``run_date`` is not a supported type.
        ValueError: if ``run_date`` is an invalid date string.
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in features_df.columns]
    if missing:
        raise KeyError(
            f"features_df is missing required columns: {sorted(missing)}."
        )

    resolved_run_date = _coerce_run_date(run_date)

    # ltv_score defaults to the minimum floor; model / cohort values fill it in.
    result = pd.DataFrame(
        {
            "customer_id": features_df["customer_id"].to_numpy(),
            "ltv_score": float(MIN_LTV),
        },
        index=features_df.index,
    )

    # Customers with no all-time order history are handled by the cold-start
    # path; all others ("has history") are scored by the trained model.
    order_count = _numeric(features_df[ALLTIME_ORDER_COLUMN])
    history_mask = order_count > 0
    n_history = int(history_mask.sum())
    log.info(
        "LTV: %d order-history / %d total customers for run_date=%s",
        n_history,
        len(features_df),
        resolved_run_date.isoformat(),
    )

    if n_history == 0:
        # No training data at all — no cohort means are derivable; every customer
        # (all cold-start) keeps the 0.00 floor (Requirement 6.7).
        log.warning(
            "LTV: no order-history customers for run_date=%s; all ltv_score=%.2f.",
            resolved_run_date.isoformat(),
            MIN_LTV,
        )
        return result.reset_index(drop=True)

    history_rows = features_df.loc[history_mask]
    target = _numeric(history_rows[TARGET_COLUMN]).clip(lower=MIN_LTV)
    features, feature_list = _build_feature_matrix(history_rows)
    channel = history_rows[CHANNEL_COLUMN].astype("string").fillna("unknown")

    split = _split_train_val(features, target, channel)
    if split is None:
        # Too few samples to hold out a validation set: train on all, report
        # in-sample metrics so the run is still logged (Requirement 6.3 applies
        # "when enough samples exist").
        log.warning(
            "LTV: only %d order-history customer(s); skipping held-out "
            "validation and reporting in-sample metrics.",
            n_history,
        )
        model = _fit_regressor(features, target)
        preds_train = model.predict(features)
        rmse, mae = _rmse_mae(target, preds_train)
        in_sample = True
        train_channels = channel
        train_target = target
    else:
        X_train, X_val, y_train, y_val = split
        model = _fit_regressor(X_train, y_train)
        val_preds = np.clip(model.predict(X_val), MIN_LTV, None)
        rmse, mae = _rmse_mae(y_val, val_preds)
        in_sample = False
        # Cohort means are computed from the training split only (Requirement 6.6).
        train_channels = channel.loc[X_train.index]
        train_target = target.loc[X_train.index]

    # Score every order-history customer with the model, clipped to ≥ 0.00.
    history_preds = np.clip(model.predict(features), MIN_LTV, None)
    result.loc[history_mask, "ltv_score"] = history_preds

    # Cold-start: assign each no-history customer its acquisition-channel cohort
    # mean from the training data, or 0.00 when the cohort has no observations.
    cohort_means: Dict[str, float] = (
        train_target.groupby(train_channels).mean().clip(lower=MIN_LTV).to_dict()
    )
    coldstart_mask = ~history_mask
    n_coldstart = int(coldstart_mask.sum())
    if n_coldstart:
        coldstart_channels = (
            features_df.loc[coldstart_mask, CHANNEL_COLUMN]
            .astype("string")
            .fillna("unknown")
        )
        result.loc[coldstart_mask, "ltv_score"] = [
            cohort_means.get(ch, MIN_LTV) for ch in coldstart_channels
        ]
        log.info(
            "LTV: assigned cohort-mean baselines to %d cold-start customer(s) "
            "across %d channel cohort(s).",
            n_coldstart,
            len(cohort_means),
        )

    # Final guard: enforce the non-negativity invariant for every customer
    # (Requirement 6.2), regardless of the path taken above.
    result["ltv_score"] = result["ltv_score"].clip(lower=MIN_LTV)

    _log_to_mlflow(
        model=model,
        rmse=rmse,
        mae=mae,
        in_sample=in_sample,
        training_sample_count=n_history,
        feature_list=feature_list,
        run_date=resolved_run_date,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    return result.reset_index(drop=True)
