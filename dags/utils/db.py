"""Parameterized PostgreSQL access helpers for Airflow tasks.

This module provides a thin wrapper around ``psycopg2`` that enforces the
platform's "parameterized SQL only" security rule (NFR-6.4). Values are always
passed to the database driver separately from the query text; queries that
appear to embed values via Python string formatting are rejected before they
ever reach the database.

Design notes
------------
* ``psycopg2`` is imported lazily inside the functions that need it so that this
  module is import-safe for Airflow's DAG parser even if the driver is not yet
  installed in the parsing environment.
* No connection is opened at import time. ``get_connection()`` reads the
  ``DATABASE_URL`` environment variable each call — nothing is cached.
"""

from __future__ import annotations

import os
import re
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence, Union

# Accepted value markers for psycopg2: positional ``%s`` and named ``%(name)s``.
# A literal percent sign in SQL must be escaped as ``%%`` when parameters are
# passed. Anything else that follows a ``%`` indicates a hand-formatted string.
_VALID_PLACEHOLDER = re.compile(r"%(?:s|\([A-Za-z_][A-Za-z0-9_]*\)s|%)")
_ANY_PERCENT = re.compile(r"%")

# Python ``str.format`` / f-string style placeholders that should never appear
# in a query string — their presence means a value was (or was meant to be)
# interpolated directly into SQL.
_FORMAT_PLACEHOLDER = re.compile(r"\{[A-Za-z0-9_]*\}")

ParamsType = Optional[Union[Sequence[Any], dict]]


class UnparameterizedQueryError(ValueError):
    """Raised when a query looks string-interpolated rather than parameterized."""


def assert_parameterized(query: str, params: ParamsType = None) -> None:
    """Validate that ``query`` relies on driver-side parameter substitution.

    The check is deliberately conservative and catches the two most common ways
    a value gets interpolated into SQL by hand:

    * ``str.format`` / f-string placeholders such as ``{}`` or ``{customer_id}``.
    * Bare ``%`` signs that are not valid ``psycopg2`` placeholders. A literal
      percent (e.g. in a ``LIKE`` pattern) must be written as ``%%``.

    Raises:
        UnparameterizedQueryError: if the query appears to be string-interpolated.
    """
    if not isinstance(query, str):
        raise TypeError("query must be a str")

    if _FORMAT_PLACEHOLDER.search(query):
        raise UnparameterizedQueryError(
            "Query contains str.format/f-string placeholders. Use %s placeholders "
            "and pass values via the `params` argument instead of formatting SQL."
        )

    # Validate every percent sign is a legitimate psycopg2 placeholder.
    stripped = _VALID_PLACEHOLDER.sub("", query)
    if _ANY_PERCENT.search(stripped):
        raise UnparameterizedQueryError(
            "Query contains a '%' that is not a valid psycopg2 placeholder. "
            "Use %s / %(name)s for values and escape literal percents as '%%'."
        )


def get_connection():
    """Open a new ``psycopg2`` connection from the ``DATABASE_URL`` env var.

    The value is a libpq connection URI, e.g.
    ``postgresql://user:pass@host:5432/dbname``.

    Returns:
        psycopg2.extensions.connection: a new, open connection. The caller owns
        the connection and is responsible for committing and closing it (or use
        :func:`connection` for automatic cleanup).

    Raises:
        RuntimeError: if ``DATABASE_URL`` is not set.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL environment variable is not set; cannot open a "
            "PostgreSQL connection."
        )

    import psycopg2  # Lazy import keeps this module import-safe.

    return psycopg2.connect(dsn)


@contextmanager
def connection() -> Iterator[Any]:
    """Context manager yielding a connection, committing on success.

    On a clean exit the transaction is committed; on any exception it is rolled
    back. The connection is always closed.
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute(
    query: str,
    params: ParamsType = None,
    *,
    fetch: Optional[str] = None,
    conn: Any = None,
):
    """Execute a parameterized query, optionally fetching results.

    Args:
        query: SQL text using ``%s`` / ``%(name)s`` placeholders for all values.
        params: values bound to the placeholders (sequence or mapping).
        fetch: ``None`` (no fetch), ``"one"`` (return a single row) or ``"all"``
            (return every row). Defaults to ``None``.
        conn: an existing connection to reuse. When omitted, a short-lived
            connection is opened and committed/closed automatically.

    Returns:
        The fetched row(s) when ``fetch`` is provided, otherwise the affected
        ``rowcount``.

    Raises:
        UnparameterizedQueryError: if the query is not properly parameterized.
    """
    assert_parameterized(query, params)

    if fetch not in (None, "one", "all"):
        raise ValueError("fetch must be one of None, 'one', 'all'")

    def _run(active_conn):
        with active_conn.cursor() as cur:
            cur.execute(query, params)
            if fetch == "one":
                return cur.fetchone()
            if fetch == "all":
                return cur.fetchall()
            return cur.rowcount

    if conn is not None:
        # Caller manages the transaction lifecycle for a supplied connection.
        return _run(conn)

    with connection() as owned_conn:
        return _run(owned_conn)
