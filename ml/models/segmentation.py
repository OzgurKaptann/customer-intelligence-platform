"""Customer Segmentation model — RFM k-means clustering (Task 26).

This module implements the Segmentation_Model described in the design's *AI/ML
Architecture* and Requirement 5. It clusters customers in the RFM quintile
feature space and assigns each customer a human-readable ``segment_label``.

Behaviour (Requirements 5.1–5.8):

* **Feature space** — the three RFM quintile columns of ``mart_customer_360``:
  ``recency_score``, ``frequency_score``, ``monetary_score`` (each 1–5 for active
  customers). ``StandardScaler`` standardises them before clustering.
* **Model selection** — ``KMeans`` is evaluated for ``k ∈ {4, 5, 6, 7, 8}`` and
  the ``k`` with the highest ``silhouette_score`` is selected. If the silhouette
  scores across all evaluated ``k`` differ by ≤ ``0.01`` the model defaults to
  ``k = 4`` and logs a WARNING to the pipeline (Airflow task) logger.
* **Inactive customers** — customers with ``order_frequency_365d == 0`` bypass
  clustering entirely and receive ``segment_label = "Inactive"`` (Requirement
  5.5). Every *active* customer receives exactly one non-``Inactive`` label
  (Requirement 5.4).
* **Labels** — centroids are ranked by their composite RFM value (recency +
  frequency + monetary in the original quintile scale) and assigned ordered,
  deterministic names from best to worst. ``"Inactive"`` is deliberately **not**
  in the cluster label pool — it is reserved for the zero-order bypass above.
* **MLflow** — logs ``algorithm``, ``k``, ``random_state``, ``silhouette_score``,
  ``inertia`` and a run timestamp to the ``customer_segmentation`` experiment,
  registers the fitted model under ``customer_segmentation`` and promotes the
  registered version with the highest silhouette score to ``production``
  (Requirements 5.6–5.8).

``train_and_score`` returns a ``DataFrame`` with ``customer_id`` and
``segment_label`` columns, one row per input customer, in input order.

MLflow is treated as best-effort infrastructure: an MLflow outage is logged as a
warning but never fails scoring, consistent with the platform's error-handling
design. The returned segment assignments do not depend on MLflow availability.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

if TYPE_CHECKING:  # Keep runtime imports light for Airflow's DAG parser.
    from sklearn.cluster import KMeans

# The pipeline/Airflow task logger — the tie-breaking and promotion warnings are
# surfaced in the Orchestrator run log via this logger (Requirements 5.3, 5.8).
log = logging.getLogger("cip.pipeline")

# ---------------------------------------------------------------------------
# Model configuration
# ---------------------------------------------------------------------------
ALGORITHM = "kmeans"
RANDOM_STATE = 42

#: RFM quintile feature columns used as the clustering feature space (Req 5.3).
RFM_COLUMNS: Tuple[str, ...] = ("recency_score", "frequency_score", "monetary_score")

#: Column whose zero value marks a customer as Inactive (Requirement 5.5).
ACTIVITY_COLUMN = "order_frequency_365d"

#: Cluster counts evaluated, inclusive 4..8 (Requirement 5.3).
K_RANGE: Tuple[int, ...] = (4, 5, 6, 7, 8)

#: Default k when all silhouette scores are within the tie threshold (Req 5.3).
DEFAULT_K = 4

#: Silhouette scores differing by ≤ this collapse to :data:`DEFAULT_K` (Req 5.3).
SILHOUETTE_TIE_THRESHOLD = 0.01

#: Cap on the sample used for silhouette scoring. silhouette_score is O(n^2); at
#: the 100K-customer target a full computation is infeasible, so a fixed random
#: sample (seeded by :data:`RANDOM_STATE`) is used for a stable, fast estimate.
SILHOUETTE_SAMPLE_SIZE = 10_000

#: Label reserved for zero-order customers; never assigned to a cluster.
INACTIVE_LABEL = "Inactive"

#: MLflow experiment and registered-model name (design MLflow Registry table).
EXPERIMENT_NAME = "customer_segmentation"
REGISTERED_MODEL_NAME = "customer_segmentation"
PRODUCTION_STAGE = "production"

#: Ordered (best → worst composite RFM) human-readable cluster labels per k.
#: The rank-0 centroid (highest composite RFM) gets the first label. None of
#: these is ``"Inactive"`` so active customers never receive the Inactive label
#: (Requirement 5.4), which is reserved for the zero-order bypass.
LABELS_BY_K: Dict[int, Tuple[str, ...]] = {
    4: ("Champions", "Loyal", "At-Risk", "Dormant"),
    5: ("Champions", "Loyal", "Potential Loyalist", "At-Risk", "Dormant"),
    6: (
        "Champions",
        "Loyal",
        "Potential Loyalist",
        "Needs Attention",
        "At-Risk",
        "Dormant",
    ),
    7: (
        "Champions",
        "Loyal",
        "Potential Loyalist",
        "Needs Attention",
        "At-Risk",
        "Hibernating",
        "Dormant",
    ),
    8: (
        "Champions",
        "Loyal",
        "Potential Loyalist",
        "Promising",
        "Needs Attention",
        "At-Risk",
        "Hibernating",
        "Dormant",
    ),
}


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


def _rfm_matrix(active: pd.DataFrame) -> np.ndarray:
    """Extract the RFM feature matrix from ``active`` as a float array.

    Values are coerced to numeric; any non-finite entry (unexpected for active
    customers, whose RFM scores are NOT NULL in ``mart_customer_360``) is filled
    with ``0`` so clustering cannot fail on stray nulls.
    """
    frame = active.loc[:, list(RFM_COLUMNS)].apply(
        pd.to_numeric, errors="coerce"
    )
    return frame.fillna(0.0).to_numpy(dtype=float)


def _silhouette(features: np.ndarray, labels: np.ndarray) -> float:
    """Silhouette score for ``labels`` over ``features`` (sampled for scale)."""
    from sklearn.metrics import silhouette_score

    sample_size = (
        SILHOUETTE_SAMPLE_SIZE
        if features.shape[0] > SILHOUETTE_SAMPLE_SIZE
        else None
    )
    return float(
        silhouette_score(
            features,
            labels,
            sample_size=sample_size,
            random_state=RANDOM_STATE,
        )
    )


def _fit_kmeans(features: np.ndarray, k: int) -> "KMeans":
    """Fit a deterministic ``KMeans`` with ``k`` clusters on ``features``."""
    from sklearn.cluster import KMeans

    # n_init is set explicitly for reproducibility across scikit-learn versions.
    return KMeans(n_clusters=k, random_state=RANDOM_STATE, n_init=10).fit(features)


def _select_best_k(
    features: np.ndarray,
) -> Tuple[int, "KMeans", float, float]:
    """Choose the cluster count with the best silhouette score.

    Evaluates every ``k`` in :data:`K_RANGE` for which a silhouette score is
    defined (requires ``k < n_samples``). Returns the selected ``k`` and its
    fitted model, silhouette score and inertia.

    Tie rule (Requirement 5.3): if the silhouette scores across all evaluated
    ``k`` differ by ≤ :data:`SILHOUETTE_TIE_THRESHOLD`, default to
    :data:`DEFAULT_K` and log a WARNING.

    Only ``k`` values that both satisfy ``k < n_samples`` (a silhouette
    prerequisite) and yield ≥ 2 distinct clusters are scored; a ``k`` that
    collapses to a single cluster (e.g. duplicate points) has no defined
    silhouette and is skipped.

    Raises:
        ValueError: if no ``k`` in :data:`K_RANGE` yields a scoreable clustering
            (too few, or indistinguishable, active customers).
    """
    n_samples = features.shape[0]
    candidate_ks = [k for k in K_RANGE if k < n_samples]

    fitted: Dict[int, "KMeans"] = {}
    silhouettes: Dict[int, float] = {}
    for k in candidate_ks:
        model = _fit_kmeans(features, k)
        # silhouette_score is undefined for a single cluster; skip such k.
        if np.unique(model.labels_).size < 2:
            continue
        fitted[k] = model
        silhouettes[k] = _silhouette(features, model.labels_)
        log.info(
            "Segmentation: evaluated k=%d silhouette=%.4f inertia=%.2f",
            k,
            silhouettes[k],
            model.inertia_,
        )

    if not silhouettes:
        raise ValueError(
            f"Cannot segment {n_samples} active customer(s): no k in {K_RANGE} "
            "produces a scoreable clustering (need enough distinguishable RFM "
            "profiles to form at least 2 of the minimum "
            f"{min(K_RANGE)} clusters)."
        )

    scored_ks = sorted(silhouettes)
    spread = max(silhouettes.values()) - min(silhouettes.values())
    if spread <= SILHOUETTE_TIE_THRESHOLD:
        # Prefer the mandated default k=4; fall back to the smallest scoreable k
        # only in the pathological case where k=4 itself was not scoreable.
        best_k = DEFAULT_K if DEFAULT_K in silhouettes else scored_ks[0]
        log.warning(
            "Segmentation silhouette scores across k=%s differ by %.4f "
            "(≤ %.2f); defaulting to k=%d.",
            scored_ks,
            spread,
            SILHOUETTE_TIE_THRESHOLD,
            best_k,
        )
    else:
        best_k = max(silhouettes, key=silhouettes.__getitem__)
        log.info(
            "Segmentation selected k=%d by highest silhouette=%.4f",
            best_k,
            silhouettes[best_k],
        )

    model = fitted[best_k]
    return best_k, model, silhouettes[best_k], float(model.inertia_)


def _cluster_label_map(model: "KMeans", scaler, k: int) -> Dict[int, str]:
    """Map each cluster id to a human-readable label by centroid RFM rank.

    Centroids are inverse-transformed back to the original RFM quintile scale and
    ranked by their composite value (sum of the three RFM dimensions). The
    highest-composite centroid receives the first (best) label from
    :data:`LABELS_BY_K`, producing a deterministic label per centroid rank
    (design "Segment naming convention").
    """
    centroids_original = scaler.inverse_transform(model.cluster_centers_)
    composite = centroids_original.sum(axis=1)
    # Rank clusters best → worst; ``-composite`` sorts descending. ``kind`` is a
    # stable sort so equal-composite centroids get a deterministic tie order.
    ranked_cluster_ids = np.argsort(-composite, kind="stable")

    labels = LABELS_BY_K[k]
    return {
        int(cluster_id): labels[rank]
        for rank, cluster_id in enumerate(ranked_cluster_ids)
    }


def _log_to_mlflow(
    model: "KMeans",
    k: int,
    silhouette: float,
    inertia: float,
    run_date: date,
    mlflow_tracking_uri: Optional[str],
) -> None:
    """Log params/metrics, register and promote the segmentation model.

    Best-effort: any MLflow error is caught and logged as a warning so a
    tracking/registry outage never fails scoring (design error handling). Logs
    ``algorithm``, ``k``, ``random_state``, ``silhouette_score``, ``inertia`` and
    a run timestamp (Requirement 5.6); registers under
    :data:`REGISTERED_MODEL_NAME` and promotes the highest-silhouette version to
    ``production`` (Requirements 5.7, 5.8).
    """
    try:
        import mlflow
        import mlflow.sklearn

        if mlflow_tracking_uri:
            mlflow.set_tracking_uri(mlflow_tracking_uri)
        mlflow.set_experiment(EXPERIMENT_NAME)

        with mlflow.start_run(run_name=f"segmentation_{run_date.isoformat()}"):
            mlflow.log_param("algorithm", ALGORITHM)
            mlflow.log_param("k", k)
            mlflow.log_param("random_state", RANDOM_STATE)
            mlflow.log_metric("silhouette_score", silhouette)
            mlflow.log_metric("inertia", inertia)
            # Run timestamp (Requirement 5.6). MLflow also records start_time, but
            # the design lists an explicit timestamp among the logged fields.
            mlflow.set_tag("run_timestamp", datetime.now(timezone.utc).isoformat())
            mlflow.set_tag("run_date", run_date.isoformat())
            mlflow.sklearn.log_model(
                model,
                artifact_path="model",
                registered_model_name=REGISTERED_MODEL_NAME,
            )
    except Exception:  # noqa: BLE001 — MLflow outage must not fail scoring.
        log.warning(
            "MLflow unavailable; segmentation run was not logged/registered.",
            exc_info=True,
        )
        return

    _promote_best_version(mlflow_tracking_uri)


def _promote_best_version(mlflow_tracking_uri: Optional[str]) -> None:
    """Promote the highest-silhouette registered version to ``production``.

    Reads the ``silhouette_score`` metric of every registered
    ``customer_segmentation`` version and transitions the best one to the
    ``production`` stage, archiving other production versions so exactly one
    holds the tag (Requirement 5.7). If no version currently holds ``production``
    a WARNING is logged before promoting (Requirement 5.8).
    """
    try:
        from mlflow.tracking import MlflowClient

        client = MlflowClient(tracking_uri=mlflow_tracking_uri or None)
        versions = client.search_model_versions(
            f"name='{REGISTERED_MODEL_NAME}'"
        )
        if not versions:
            return

        def _version_silhouette(mv) -> float:
            try:
                metrics = client.get_run(mv.run_id).data.metrics
                return float(metrics.get("silhouette_score", float("-inf")))
            except Exception:  # noqa: BLE001 — treat unreadable runs as lowest.
                return float("-inf")

        best = max(versions, key=_version_silhouette)

        has_production = any(
            PRODUCTION_STAGE.lower() == (mv.current_stage or "").lower()
            for mv in versions
        )
        if not has_production:
            log.warning(
                "No %s version holds the '%s' stage; promoting version %s "
                "(highest silhouette).",
                REGISTERED_MODEL_NAME,
                PRODUCTION_STAGE,
                best.version,
            )

        client.transition_model_version_stage(
            name=REGISTERED_MODEL_NAME,
            version=best.version,
            stage=PRODUCTION_STAGE,
            archive_existing_versions=True,
        )
        log.info(
            "Promoted %s version %s to %s.",
            REGISTERED_MODEL_NAME,
            best.version,
            PRODUCTION_STAGE,
        )
    except Exception:  # noqa: BLE001 — promotion is best-effort.
        log.warning(
            "MLflow unavailable; could not promote a %s production version.",
            REGISTERED_MODEL_NAME,
            exc_info=True,
        )


def train_and_score(
    features_df: pd.DataFrame,
    run_date: Union[str, "date", "datetime"],
    mlflow_tracking_uri: Optional[str] = None,
) -> pd.DataFrame:
    """Train the segmentation model and assign a segment to every customer.

    Args:
        features_df: per-customer feature frame from ``mart_customer_360`` (as
            returned by :func:`ml.features.load_customer_features`). Must contain
            ``customer_id``, the :data:`RFM_COLUMNS`, and :data:`ACTIVITY_COLUMN`.
        run_date: the pipeline run date (``date``/``datetime`` or ISO string);
            used for MLflow run naming/tagging.
        mlflow_tracking_uri: MLflow tracking URI. When omitted, MLflow uses its
            ambient configuration (e.g. the ``MLFLOW_TRACKING_URI`` env var).

    Returns:
        A ``DataFrame`` with columns ``customer_id`` and ``segment_label`` — one
        row per input customer, in input order. Customers with
        ``order_frequency_365d == 0`` are labelled ``"Inactive"``; all others
        receive a cluster label from :data:`LABELS_BY_K`.

    Raises:
        KeyError: if a required column is missing from ``features_df``.
        ValueError: if ``run_date`` is an invalid date string, or there are too
            few active customers to form the minimum number of clusters.
    """
    required = {"customer_id", ACTIVITY_COLUMN, *RFM_COLUMNS}
    missing = required.difference(features_df.columns)
    if missing:
        raise KeyError(
            f"features_df is missing required columns: {sorted(missing)}."
        )

    resolved_run_date = _coerce_run_date(run_date)

    # segment_label defaults to Inactive; active customers are overwritten below.
    result = pd.DataFrame(
        {
            "customer_id": features_df["customer_id"].to_numpy(),
            "segment_label": INACTIVE_LABEL,
        },
        index=features_df.index,
    )

    activity = pd.to_numeric(
        features_df[ACTIVITY_COLUMN], errors="coerce"
    ).fillna(0)
    active_mask = activity > 0
    n_active = int(active_mask.sum())
    log.info(
        "Segmentation: %d active / %d total customers for run_date=%s",
        n_active,
        len(features_df),
        resolved_run_date.isoformat(),
    )

    if n_active == 0:
        # No active customers — everyone is Inactive; nothing to cluster.
        log.warning(
            "Segmentation: no active customers for run_date=%s; all labelled %s.",
            resolved_run_date.isoformat(),
            INACTIVE_LABEL,
        )
        return result

    from sklearn.preprocessing import StandardScaler

    active = features_df.loc[active_mask]
    raw_features = _rfm_matrix(active)

    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(raw_features)

    best_k, model, silhouette, inertia = _select_best_k(scaled_features)

    label_map = _cluster_label_map(model, scaler, best_k)
    active_labels = [label_map[int(c)] for c in model.labels_]
    result.loc[active_mask, "segment_label"] = active_labels

    _log_to_mlflow(
        model=model,
        k=best_k,
        silhouette=silhouette,
        inertia=inertia,
        run_date=resolved_run_date,
        mlflow_tracking_uri=mlflow_tracking_uri,
    )

    return result.reset_index(drop=True)
