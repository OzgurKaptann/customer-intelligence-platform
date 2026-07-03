"""Churn Risk Scoring model — Random Forest classifier (Task 30).

This module implements the Churn_Model described in the design's *AI/ML
Architecture* and Requirement 7. It predicts a per-customer churn probability in
``[0.0, 1.0]`` and assigns a discrete risk tier.

Behaviour (Requirements 7.1–7.8):

* **Active customers only** — the input is filtered to rows where ``is_active``
  is true before *both* training and scoring (Requirement 7.7). Inactive
  customers are not scored by this model; they are absent from the returned
  frame and the caller (``ml.scoring``) leaves their prior values untouched.
* **Features** — derived from ``mart_customer_360`` (Requirement 7.2):
  ``days_since_last_order``, ``days_since_last_event``, ``open_ticket_count``,
  ``order_frequency_trend`` and, when present, ``segment_label`` (one-hot).
* **Churn label** — Requirement 7.1 defines churn as *no Order or Event activity
  in the 90 days following the score date*. A single ``mart_customer_360``
  snapshot has no forward window, so a **historical proxy** is used: a customer
  is labelled churned (``1``) when they have had no Order **and** no Event in the
  trailing :data:`CHURN_WINDOW_DAYS`-day window (a missing "days since" value is
  treated as "no such activity"). This is the proxy named in the design's Churn
  Model table and is documented here so the leakage relationship between the
  recency features and the label is explicit.
* **Model** — ``RandomForestClassifier``; the churn probability is
  ``predict_proba[:, 1]`` (Requirement 7.1). An 80/20 split **stratified by the
  churn label** is used when there are enough samples, falling back to an
  unstratified split, and to in-sample training for very small inputs.
* **Risk tiers** — ``churn_risk_tier`` is ``"Low"`` for ``score < 0.33``,
  ``"Medium"`` for ``0.33 ≤ score < 0.67`` and ``"High"`` for ``score ≥ 0.67``
  (Requirement 7.3) — see :func:`assign_risk_tier`.
* **Validation** — AUC-ROC is computed on the held-out set **only when both
  classes are present** there; precision and recall (at a 0.5 decision
  threshold) are logged alongside it (Requirement 7.4).
* **MLflow** — logs ``algorithm``, AUC-ROC, precision, recall, per-feature
  ``feature_importances`` (JSON map), the feature list and a run timestamp to the
  ``customer_churn`` experiment. The model is registered under ``customer_churn``
  and promoted to ``production`` **only** when the validation AUC-ROC exceeds
  :data:`AUC_ROC_THRESHOLD`; otherwise the current production model is retained
  and a WARNING is logged (Requirements 7.4–7.6). MLflow is best-effort: an
  outage is logged as a warning but never changes the returned scores.
* **Failure fallback** — any unexpected error raised while training or scoring is
  caught, logged at ERROR level, and ``None`` is returned so the caller can
  retain the most recent prior day's churn values (Requirement 7.8). A missing
  required column is a wiring/contract error and is raised as :class:`KeyError`
  *before* the fallback boundary rather than silently degrading to ``None``.

``train_and_score`` returns a ``DataFrame`` with ``customer_id``,
``churn_score`` and ``churn_risk_tier`` columns — one row per *active* input
customer, in input order — or ``None`` on failure.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

if TYPE_CHECKING:  # Keep runtime imports light for Airflow's DAG parser.
    from sklearn.ensemble import RandomForestClassifier

# The pipeline/Airflow task logger — the quality-gate, fallback and promotion
# messages are surfaced in the Orchestrator run log via this logger
# (Requirements 7.6, 7.8).
log = logging.getLogger("cip.pipeline")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
ALGORITHM = "random_forest_classifier"
RANDOM_STATE = 42
N_ESTIMATORS = 200
MAX_DEPTH = 12

#: Fraction of samples held out for validation (Requirement 7.4).
VALIDATION_FRACTION = 0.20

#: Below this many active customers, the held-out split is skipped and metrics
#: are reported in-sample. ``10`` guarantees ≥ 2 validation rows at 20%.
MIN_SAMPLES_FOR_SPLIT = 10

#: Trailing window (days) used to derive the historical churn-label proxy.
CHURN_WINDOW_DAYS = 90

#: Sentinel for a missing "days since last {order,event}" value. Such customers
#: have no recent activity of that kind, so a large value (well beyond the
#: trailing 365-day active window) is the faithful numeric stand-in.
MISSING_RECENCY_FILL = 999

#: Column marking a customer as active (Requirement 7.7). Only active customers
#: are trained on and scored.
ACTIVITY_COLUMN = "is_active"

#: Base numeric feature columns fed to the classifier (before one-hot encoding).
BASE_NUMERIC_FEATURES: Tuple[str, ...] = (
    "days_since_last_order",
    "days_since_last_event",
    "open_ticket_count",
    "order_frequency_trend",
)

#: Categorical feature columns one-hot encoded when present. ``segment_label`` is
#: produced by the segmentation model and joined in upstream; it is optional and
#: used only if the caller supplies it (Requirement 7.2).
CATEGORICAL_FEATURES: Tuple[str, ...] = ("segment_label",)

#: Recency columns used to build the churn-label proxy (see module docstring).
RECENCY_ORDER_COLUMN = "days_since_last_order"
RECENCY_EVENT_COLUMN = "days_since_last_event"

#: Columns that must be present on the input frame (``segment_label`` optional).
REQUIRED_COLUMNS: Tuple[str, ...] = ("customer_id", ACTIVITY_COLUMN, *BASE_NUMERIC_FEATURES)

# ---------------------------------------------------------------------------
# Risk tiers (Requirement 7.3)
# ---------------------------------------------------------------------------
TIER_LOW = "Low"
TIER_MEDIUM = "Medium"
TIER_HIGH = "High"

#: score < TIER_LOW_MAX -> Low; score >= TIER_HIGH_MIN -> High; else Medium.
TIER_LOW_MAX = 0.33
TIER_HIGH_MIN = 0.67

#: Churn-probability bounds (Requirement 7.1).
SCORE_MIN = 0.0
SCORE_MAX = 1.0

#: Registry gate: register + promote only when validation AUC-ROC exceeds this
#: (Requirement 7.5); otherwise retain current + WARNING (Requirement 7.6).
AUC_ROC_THRESHOLD = 0.70

#: MLflow experiment and registered-model name (design MLflow Registry table).
EXPERIMENT_NAME = "customer_churn"
REGISTERED_MODEL_NAME = "customer_churn"
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
    """Coerce ``series`` to numeric, leaving unparseable values as ``NaN``."""
    return pd.to_numeric(series, errors="coerce")


def _active_mask(series: pd.Series) -> pd.Series:
    """Boolean mask of active customers, robust to bool/int/string encodings.

    ``is_active`` is a PostgreSQL ``BOOLEAN`` and normally arrives as a NumPy
    bool via the DuckDB/Parquet feature snapshot, but the mart could be read
    through a path that yields ``1``/``0`` or ``"true"``/``"false"``; all of
    those are normalised here so the active-only filter (Requirement 7.7) is
    reliable.
    """
    if series.dtype == bool:
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return _numeric(series).fillna(0) > 0
    truthy = {"true", "t", "1", "yes", "y"}
    return series.astype("string").str.strip().str.lower().isin(truthy)


def assign_risk_tier(scores: Union[np.ndarray, pd.Series]) -> np.ndarray:
    """Map churn probabilities to risk-tier labels (Requirement 7.3).

    Returns an object array where each score maps to exactly one tier with no
    gaps or overlaps: ``"Low"`` for ``score < 0.33``, ``"High"`` for
    ``score >= 0.67`` and ``"Medium"`` for everything in ``[0.33, 0.67)``.
    """
    values = np.asarray(scores, dtype=float)
    tiers = np.full(values.shape, TIER_MEDIUM, dtype=object)
    tiers[values < TIER_LOW_MAX] = TIER_LOW
    tiers[values >= TIER_HIGH_MIN] = TIER_HIGH
    return tiers


def _churn_label(active_rows: pd.DataFrame) -> pd.Series:
    """Build the historical churn-label proxy for the active customers.

    Requirement 7.1 defines churn over the 90 days *following* the score date;
    a single mart snapshot has no forward window, so the trailing
    :data:`CHURN_WINDOW_DAYS`-day window is used as the documented proxy: a
    customer is churned (``1``) when they have had **no Order and no Event** in
    that window. A missing "days since" value means no such activity is on
    record and is therefore treated as "not recent" (churned on that axis).
    """
    dso = _numeric(active_rows[RECENCY_ORDER_COLUMN])
    dse = _numeric(active_rows[RECENCY_EVENT_COLUMN])
    no_recent_order = dso.isna() | (dso >= CHURN_WINDOW_DAYS)
    no_recent_event = dse.isna() | (dse >= CHURN_WINDOW_DAYS)
    return (no_recent_order & no_recent_event).astype(int)


def _build_feature_matrix(active_rows: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Assemble the one-hot encoded feature matrix for the active customers.

    Returns the numeric feature ``DataFrame`` (all columns float) and the ordered
    ``feature_list`` used for MLflow logging. Encoding the full active population
    in one call guarantees the train and predict matrices share identical
    columns. Missing recency values are filled with :data:`MISSING_RECENCY_FILL`;
    other numeric gaps default to ``0``.
    """
    from ml.features import onehot_encode

    base = pd.DataFrame(index=active_rows.index)
    base["days_since_last_order"] = _numeric(
        active_rows[RECENCY_ORDER_COLUMN]
    ).fillna(MISSING_RECENCY_FILL)
    base["days_since_last_event"] = _numeric(
        active_rows[RECENCY_EVENT_COLUMN]
    ).fillna(MISSING_RECENCY_FILL)
    base["open_ticket_count"] = _numeric(
        active_rows["open_ticket_count"]
    ).fillna(0.0)
    base["order_frequency_trend"] = _numeric(
        active_rows["order_frequency_trend"]
    ).fillna(0.0)

    categoricals = [c for c in CATEGORICAL_FEATURES if c in active_rows.columns]
    for column in categoricals:
        base[column] = active_rows[column].astype("string").fillna("unknown")

    encoded = onehot_encode(base, columns=categoricals, ignore_missing=True)
    encoded = encoded.astype(float)
    return encoded, list(encoded.columns)


def _fit_classifier(
    X_train: pd.DataFrame, y_train: pd.Series
) -> "RandomForestClassifier":
    """Fit a deterministic, class-balanced ``RandomForestClassifier``."""
    from sklearn.ensemble import RandomForestClassifier

    model = RandomForestClassifier(
        n_estimators=N_ESTIMATORS,
        max_depth=MAX_DEPTH,
        random_state=RANDOM_STATE,
        class_weight="balanced",
        n_jobs=-1,
    )
    model.fit(X_train, y_train)
    return model


def _split_train_val(
    features: pd.DataFrame,
    target: pd.Series,
) -> Optional[Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]]:
    """Split into train/validation, stratified by the churn label when possible.

    Returns ``(X_train, X_val, y_train, y_val)`` or ``None`` when there are too
    few samples to hold out a meaningful validation set
    (:data:`MIN_SAMPLES_FOR_SPLIT`). A stratified split is attempted first
    (Requirement 7.4); if stratification is infeasible (a class too small for the
    requested split) it falls back to a plain random split.
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
            stratify=target,
        )
    except ValueError:
        log.warning(
            "Churn: stratified split by churn label not possible; using a "
            "random 80/20 split instead."
        )
        return train_test_split(
            features,
            target,
            test_size=VALIDATION_FRACTION,
            random_state=RANDOM_STATE,
        )


def _validation_metrics(
    model: "RandomForestClassifier",
    X_val: pd.DataFrame,
    y_val: pd.Series,
) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Return ``(auc_roc, precision, recall)`` on the validation set.

    AUC-ROC is only defined when both classes are present in ``y_val``
    (Requirement 7.4); it is ``None`` otherwise. Precision and recall are
    computed at the default 0.5 decision threshold with ``zero_division=0`` so a
    degenerate validation split cannot raise.
    """
    from sklearn.metrics import precision_score, recall_score, roc_auc_score

    proba = model.predict_proba(X_val)[:, 1]
    auc: Optional[float] = None
    if y_val.nunique() >= 2:
        auc = float(roc_auc_score(y_val, proba))

    y_pred = (proba >= 0.5).astype(int)
    precision = float(precision_score(y_val, y_pred, zero_division=0))
    recall = float(recall_score(y_val, y_pred, zero_division=0))
    return auc, precision, recall


def _score(model: "RandomForestClassifier", features: pd.DataFrame) -> np.ndarray:
    """Churn probability ``predict_proba[:, 1]`` clipped to ``[0.0, 1.0]``."""
    proba = model.predict_proba(features)[:, 1]
    return np.clip(proba, SCORE_MIN, SCORE_MAX)


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
        "Promoted %s version %s to %s.", model_name, latest.version, PRODUCTION_STAGE
    )


def _log_to_mlflow(
    model: Optional["RandomForestClassifier"],
    feature_list: List[str],
    auc: Optional[float],
    precision: Optional[float],
    recall: Optional[float],
    feature_importances: Dict[str, float],
    in_sample: bool,
    training_sample_count: int,
    run_date: date,
    mlflow_tracking_uri: Optional[str],
) -> None:
    """Log params/metrics and conditionally register+promote the churn model.

    Best-effort: any MLflow error is caught and logged as a warning so a
    tracking/registry outage never fails scoring (design error handling). Logs
    ``algorithm``, AUC-ROC, ``precision``, ``recall``, the ``feature_importances``
    JSON map and a run timestamp (Requirement 7.4). Registration + promotion
    happens only when a held-out AUC-ROC exceeds :data:`AUC_ROC_THRESHOLD`
    (Requirement 7.5). The AUC quality WARNING (Requirement 7.6) is emitted by
    the caller so it is logged even when MLflow is unavailable.
    """
    try:
        import mlflow
        import mlflow.sklearn
        from mlflow.tracking import MlflowClient

        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment(EXPERIMENT_NAME)

        should_register = (
            model is not None and auc is not None and auc > AUC_ROC_THRESHOLD
        )

        with mlflow.start_run(run_name=f"churn_{run_date.isoformat()}"):
            mlflow.log_param("algorithm", ALGORITHM)
            mlflow.log_param("n_estimators", N_ESTIMATORS)
            mlflow.log_param("max_depth", MAX_DEPTH)
            mlflow.log_param("random_state", RANDOM_STATE)
            mlflow.log_param("feature_list", json.dumps(feature_list))
            mlflow.log_param("validation", "in_sample" if in_sample else "holdout")
            mlflow.log_metric("training_sample_count", training_sample_count)
            if auc is not None:
                mlflow.log_metric("auc_roc", auc)
            if precision is not None:
                mlflow.log_metric("precision", precision)
            if recall is not None:
                mlflow.log_metric("recall", recall)
            if feature_importances:
                # feature_importances is logged as a JSON map artifact so the
                # full key-value mapping is retained (Requirement 7.4).
                mlflow.log_dict(feature_importances, "feature_importances.json")
            # Run timestamp (Requirement 7.4). MLflow records start_time, but the
            # design lists an explicit timestamp among the logged fields.
            mlflow.set_tag("run_timestamp", datetime.now(timezone.utc).isoformat())
            mlflow.set_tag("run_date", run_date.isoformat())

            if model is not None:
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
            _promote_latest_to_production(client=MlflowClient(
                tracking_uri=mlflow_tracking_uri or None
            ), model_name=REGISTERED_MODEL_NAME)
            log.info(
                "Churn: registered + promoted new model (AUC-ROC=%.4f > %.2f).",
                auc,
                AUC_ROC_THRESHOLD,
            )
    except Exception:  # noqa: BLE001 — MLflow outage must not fail scoring.
        log.warning(
            "MLflow unavailable; churn run was not logged/registered.",
            exc_info=True,
        )


def _empty_result() -> pd.DataFrame:
    """An empty, correctly-typed result frame (no active customers to score)."""
    return pd.DataFrame(
        {
            "customer_id": pd.Series([], dtype="object"),
            "churn_score": pd.Series([], dtype="float"),
            "churn_risk_tier": pd.Series([], dtype="object"),
        }
    )


def _train_and_score_impl(
    features_df: pd.DataFrame,
    run_date: date,
    mlflow_tracking_uri: Optional[str],
) -> pd.DataFrame:
    """Core train + score logic for active customers (see :func:`train_and_score`).

    Kept separate from the public entry point so the failure fallback
    (Requirement 7.8) can wrap only the modelling work, leaving contract
    validation to raise loudly.
    """
    active_mask = _active_mask(features_df[ACTIVITY_COLUMN])
    active = features_df.loc[active_mask].reset_index(drop=True)
    n_active = len(active)
    log.info(
        "Churn: %d active / %d total customers for run_date=%s",
        n_active,
        len(features_df),
        run_date.isoformat(),
    )

    if n_active == 0:
        # No active customers to score (Requirement 7.7). This is an empty run,
        # not a failure — return an empty frame rather than None.
        log.warning(
            "Churn: no active customers for run_date=%s; nothing to score.",
            run_date.isoformat(),
        )
        return _empty_result()

    label = _churn_label(active)
    features, feature_list = _build_feature_matrix(active)

    result = pd.DataFrame(
        {
            "customer_id": active["customer_id"].to_numpy(),
            "churn_score": np.float64(SCORE_MIN),
            "churn_risk_tier": TIER_LOW,
        }
    )

    n_classes = int(label.nunique())
    if n_classes < 2:
        # Only one class present: a discriminative classifier cannot be fit and
        # AUC-ROC is undefined ("use AUC-ROC when both classes exist"). Assign the
        # constant probability implied by that single class and skip registration.
        single_class = int(label.iloc[0])
        scores = np.full(n_active, float(single_class))
        log.warning(
            "Churn: only one class present in the churn label (all %s) for "
            "run_date=%s; assigning constant score=%.1f and skipping AUC-ROC "
            "validation and model registration.",
            "churned" if single_class == 1 else "retained",
            run_date.isoformat(),
            float(single_class),
        )
        result["churn_score"] = np.clip(scores, SCORE_MIN, SCORE_MAX)
        result["churn_risk_tier"] = assign_risk_tier(result["churn_score"])
        _log_to_mlflow(
            model=None,
            feature_list=feature_list,
            auc=None,
            precision=None,
            recall=None,
            feature_importances={},
            in_sample=True,
            training_sample_count=n_active,
            run_date=run_date,
            mlflow_tracking_uri=mlflow_tracking_uri,
        )
        return result

    split = _split_train_val(features, label)
    if split is None:
        # Too few samples to hold out a validation set: train on all and report
        # in-sample metrics. Without a held-out AUC we do not register (the gate
        # in Requirement 7.5 is a *validation* AUC).
        log.warning(
            "Churn: only %d active customer(s); skipping held-out validation and "
            "reporting in-sample metrics.",
            n_active,
        )
        model = _fit_classifier(features, label)
        auc, precision, recall = _validation_metrics(model, features, label)
        auc = None  # in-sample AUC must not satisfy the validation gate.
        in_sample = True
    else:
        X_train, X_val, y_train, y_val = split
        model = _fit_classifier(X_train, y_train)
        auc, precision, recall = _validation_metrics(model, X_val, y_val)
        in_sample = False

    scores = _score(model, features)
    result["churn_score"] = scores
    result["churn_risk_tier"] = assign_risk_tier(scores)

    feature_importances = {
        name: round(float(weight), 6)
        for name, weight in zip(feature_list, model.feature_importances_)
    }

    # Emit the model-quality signal independent of MLflow availability so the
    # AUC gate outcome is always visible in the Orchestrator run log.
    if auc is None:
        log.warning(
            "Churn: no held-out AUC-ROC available (in_sample=%s / single-class "
            "validation); model will not be registered this run.",
            in_sample,
        )
    elif auc > AUC_ROC_THRESHOLD:
        log.info("Churn: validation AUC-ROC=%.4f exceeds %.2f.", auc, AUC_ROC_THRESHOLD)
    else:
        # Requirement 7.6: retain the previously registered production model.
        log.warning(
            "Churn: validation AUC-ROC=%.4f did not exceed %.2f; retaining the "
            "previously registered production model.",
            auc,
            AUC_ROC_THRESHOLD,
        )

    _log_to_mlflow(
        model=model,
        feature_list=feature_list,
        auc=auc,
        precision=precision,
        recall=recall,
        feature_importances=feature_importances,
        in_sample=in_sample,
        training_sample_count=n_active,
        run_date=run_date,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    return result


def train_and_score(
    features_df: pd.DataFrame,
    run_date: Union[str, "date", "datetime"],
    mlflow_tracking_uri: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Train the churn model and score every active customer.

    Args:
        features_df: per-customer feature frame from ``mart_customer_360`` (as
            returned by :func:`ml.features.load_customer_features`). Must contain
            :data:`REQUIRED_COLUMNS`; ``segment_label`` is used when present.
        run_date: the pipeline run date (``date``/``datetime`` or ISO string);
            used for MLflow run naming/tagging and the churn-label window.
        mlflow_tracking_uri: MLflow tracking URI. When omitted, MLflow uses its
            ambient configuration (e.g. the ``MLFLOW_TRACKING_URI`` env var).

    Returns:
        A ``DataFrame`` with columns ``customer_id``, ``churn_score`` and
        ``churn_risk_tier`` — one row per **active** input customer, in input
        order. ``churn_score`` is in ``[0.0, 1.0]`` and ``churn_risk_tier`` is one
        of ``"Low"``, ``"Medium"``, ``"High"``. When there are no active
        customers an empty (correctly-typed) frame is returned. Returns ``None``
        if scoring fails, so the caller can retain the prior day's churn values
        (Requirement 7.8).

    Raises:
        KeyError: if a required column is missing from ``features_df``.
        TypeError: if ``run_date`` is not a supported type.
        ValueError: if ``run_date`` is an invalid date string.
    """
    # Contract validation happens before the failure boundary: a missing column
    # is a wiring error the caller must fix, not a transient scoring failure, so
    # it is raised rather than degraded to the None fallback.
    missing = [c for c in REQUIRED_COLUMNS if c not in features_df.columns]
    if missing:
        raise KeyError(
            f"features_df is missing required columns: {sorted(missing)}."
        )

    resolved_run_date = _coerce_run_date(run_date)

    try:
        return _train_and_score_impl(
            features_df, resolved_run_date, mlflow_tracking_uri
        )
    except Exception:  # noqa: BLE001 — Requirement 7.8 failure fallback.
        log.error(
            "Churn scoring run failed for run_date=%s; returning None so the "
            "caller can retain the prior day's churn scores.",
            resolved_run_date.isoformat(),
            exc_info=True,
        )
        return None
