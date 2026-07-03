"""Load the per-customer feature matrix for the ML scoring pipeline (Task 25).

The single feature source for every model is ``marts.mart_customer_360`` (one row
per customer). Following the design's *ML Feature Engineering Strategy*, this
module:

1. Exports ``mart_customer_360`` for a given run date into an **in-process DuckDB**
   database (``duckdb.connect(':memory:')`` — no server required).
2. Materializes the export as a **Parquet-backed relation** on disk under
   ``ml/feature_snapshots/`` (the feature snapshot, reusable as an MLflow
   artifact and cheap to re-read).
3. Returns a pandas ``DataFrame`` containing all feature columns to the calling
   model code.

Rationale (Risk register / NFR-3.3): DuckDB performs the extract-and-shape work
in-process so large feature frames never pass through the PostgreSQL server-side
cursor path used by the rest of the platform, reducing memory pressure on the
single shared PostgreSQL instance during ML runs.

Security: ``pg_conn_string`` is treated as trusted configuration (the same value
family as ``DATABASE_URL``). The only caller-influenced value that reaches SQL is
``run_date``, which is coerced to a validated ``datetime.date`` before it is
rendered as a typed ``DATE`` literal — never as free-form text.
"""

from __future__ import annotations

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:  # Import only for type checking to keep runtime import light.
    import duckdb
    import pandas as pd

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Feature contract
# ---------------------------------------------------------------------------
# The columns produced by marts.mart_customer_360 (design "Data Models" section).
# Downstream models select the subset they need; this tuple documents the full
# contract and is used to validate that an export returned every expected field.
FEATURE_COLUMNS: tuple[str, ...] = (
    "customer_id",
    "recency_score",
    "frequency_score",
    "monetary_score",
    "rfm_score",
    "recency_days",
    "order_frequency_365d",
    "total_spend_365d_usd",
    "total_order_count",
    "total_spend_usd",
    "most_recent_event_date",
    "days_since_last_event",
    "days_since_last_order",
    "order_count_last_30d",
    "order_count_prior_30d",
    "order_frequency_trend",
    "active_campaign_count",
    "open_ticket_count",
    "acquisition_channel",
    "customer_tenure_days",
    "is_active",
)

# Fully-qualified source relation and the DuckDB alias the PostgreSQL database is
# attached under.
_SOURCE_TABLE = "marts.mart_customer_360"
_PG_ALIAS = "cip_pg"

# Default location for persisted feature snapshots (Parquet). Resolved relative
# to the repository root (two levels up from this file: ml/features/ -> repo).
DEFAULT_SNAPSHOT_DIR = Path(__file__).resolve().parents[2] / "ml" / "feature_snapshots"


def _coerce_run_date(run_date: Union[str, "date", "datetime"]) -> date:
    """Coerce ``run_date`` to a :class:`datetime.date`, validating string input.

    Accepting only real dates (or strict ``YYYY-MM-DD`` strings) is what makes it
    safe to render ``run_date`` directly into SQL as a typed ``DATE`` literal.

    Args:
        run_date: a ``date``, a ``datetime``, or an ISO ``YYYY-MM-DD`` string.

    Returns:
        The corresponding :class:`datetime.date`.

    Raises:
        TypeError: if ``run_date`` is not a supported type.
        ValueError: if a string is not a valid ISO ``YYYY-MM-DD`` date.
    """
    if isinstance(run_date, datetime):
        return run_date.date()
    if isinstance(run_date, date):
        return run_date
    if isinstance(run_date, str):
        # Raises ValueError on anything that is not a strict ISO calendar date.
        return date.fromisoformat(run_date)
    raise TypeError(
        "run_date must be a datetime.date, datetime.datetime, or an ISO "
        f"'YYYY-MM-DD' string; got {type(run_date).__name__}."
    )


def _escape_sql_literal(value: str) -> str:
    """Escape a string for use inside a single-quoted SQL literal."""
    return value.replace("'", "''")


def _attach_postgres(con: "duckdb.DuckDBPyConnection", pg_conn_string: str) -> None:
    """Attach a PostgreSQL database to an open DuckDB connection (read-only).

    Loads DuckDB's ``postgres`` extension (installing it on first use) and
    attaches ``pg_conn_string`` under the :data:`_PG_ALIAS` alias. The function is
    idempotent and safe to call repeatedly, including on a reused connection:

    * ``INSTALL`` / ``LOAD`` are no-ops when the extension is already present, so
      repeated calls do not re-download or error.
    * Any pre-existing attachment under the alias is detached before re-attaching.

    Args:
        con: an open DuckDB connection.
        pg_conn_string: a libpq connection string / URI for the CIP database.
    """
    # INSTALL/LOAD are idempotent; wrap defensively so a "already installed/loaded"
    # condition on a reused connection can never propagate as a failure.
    try:
        con.execute("INSTALL postgres;")
    except Exception:  # noqa: BLE001 - extension already installed.
        pass
    con.execute("LOAD postgres;")

    # Detach a stale alias if the caller reuses a long-lived connection. Older
    # DuckDB builds lack DETACH ... IF EXISTS, so tolerate the "not attached" error.
    try:
        con.execute(f"DETACH {_PG_ALIAS};")
    except Exception:  # noqa: BLE001 - alias simply was not attached yet.
        pass

    dsn = _escape_sql_literal(pg_conn_string)
    con.execute(f"ATTACH '{dsn}' AS {_PG_ALIAS} (TYPE postgres, READ_ONLY);")


def load_customer_features(
    run_date: Union[str, "date", "datetime"],
    pg_conn_string: Optional[str] = None,
    *,
    snapshot_dir: Union[str, "Path", None] = None,
    duckdb_con: Optional["duckdb.DuckDBPyConnection"] = None,
    enforce_contract: bool = True,
) -> "pd.DataFrame":
    """Load the ``mart_customer_360`` feature matrix for ``run_date``.

    Exports the mart for the given run date to a Parquet snapshot via an
    in-process DuckDB database and returns the result as a pandas ``DataFrame``
    containing all feature columns.

    Args:
        run_date: the pipeline run date whose ``_run_date`` partition is loaded.
            A ``date``/``datetime`` or an ISO ``YYYY-MM-DD`` string.
        pg_conn_string: libpq connection string for the CIP PostgreSQL database.
            Defaults to the ``DATABASE_URL`` environment variable when omitted.
        snapshot_dir: directory to write the Parquet feature snapshot into.
            Defaults to :data:`DEFAULT_SNAPSHOT_DIR` (``ml/feature_snapshots/``).
        duckdb_con: an existing DuckDB connection to reuse. When omitted a new
            in-memory connection is opened and closed automatically.
        enforce_contract: when ``True`` (default), require every column in
            :data:`FEATURE_COLUMNS` to be present (raising :class:`ValueError`
            otherwise), drop any non-contract columns, and return the frame with
            columns ordered exactly as :data:`FEATURE_COLUMNS`. When ``False``,
            the exported frame is returned verbatim (``SELECT *`` order).

    Returns:
        A pandas ``DataFrame`` with one row per customer for ``run_date``. With
        ``enforce_contract=True`` its columns are exactly :data:`FEATURE_COLUMNS`
        in that order.

    Raises:
        RuntimeError: if no connection string is supplied and ``DATABASE_URL`` is
            unset.
        TypeError: if ``run_date`` is not a supported type
            (see :func:`_coerce_run_date`).
        ValueError: if ``run_date`` is an invalid date string, or if
            ``enforce_contract`` is ``True`` and the loaded frame is missing any
            required feature column.
    """
    import duckdb  # Local import keeps module import cheap and dependency-optional.

    resolved_run_date = _coerce_run_date(run_date)

    conn_string = pg_conn_string or os.environ.get("DATABASE_URL")
    if not conn_string:
        raise RuntimeError(
            "No PostgreSQL connection string provided and DATABASE_URL is not "
            "set; cannot load customer features."
        )

    out_dir = Path(snapshot_dir) if snapshot_dir is not None else DEFAULT_SNAPSHOT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)
    snapshot_path = out_dir / f"customer_features_{resolved_run_date.isoformat()}.parquet"

    con = duckdb_con or duckdb.connect(":memory:")
    owns_connection = duckdb_con is None
    try:
        _attach_postgres(con, conn_string)

        # run_date is a validated date, so rendering it as a typed DATE literal is
        # injection-safe. COPY writes the Parquet-backed feature snapshot.
        date_literal = f"DATE '{resolved_run_date.isoformat()}'"
        con.execute(
            f"""
            COPY (
                SELECT *
                FROM {_PG_ALIAS}.{_SOURCE_TABLE}
                WHERE _run_date = {date_literal}
            ) TO '{_escape_sql_literal(snapshot_path.as_posix())}' (FORMAT PARQUET);
            """
        )

        # Read the snapshot back as an in-process relation and materialize a frame.
        frame = con.execute(
            "SELECT * FROM read_parquet(?);", [str(snapshot_path)]
        ).df()
    finally:
        if owns_connection:
            con.close()

    log.info(
        "Loaded %d customer feature rows for run_date=%s (snapshot: %s)",
        len(frame),
        resolved_run_date.isoformat(),
        snapshot_path,
    )

    if not enforce_contract:
        return frame

    # Fail loudly if the mart did not supply every column the ML layer contracts
    # on — a silently missing feature would otherwise surface as a confusing model
    # error much later in the pipeline.
    missing = [col for col in FEATURE_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(
            "Loaded mart_customer_360 feature frame is missing required "
            f"columns: {missing}. Present columns: {list(frame.columns)}."
        )

    # Non-contract columns (e.g. the `_run_date` audit/partition key added by the
    # dbt audit_columns() macro) are intentionally dropped rather than passed
    # through to models. Projecting to FEATURE_COLUMNS also fixes column order.
    extras = [col for col in frame.columns if col not in FEATURE_COLUMNS]
    if extras:
        log.debug("Dropping non-contract feature columns: %s", extras)

    return frame.loc[:, list(FEATURE_COLUMNS)]
