"""Builders for dbt CLI command strings used by Airflow ``BashOperator`` tasks.

These helpers only assemble command *strings*; they never shell out themselves.
Returning a plain string keeps them trivially usable as a ``BashOperator``
``bash_command`` while remaining import-safe (no dbt dependency required to
import this module).

Arguments are shell-quoted so that a ``--select`` expression or path containing
spaces cannot break out of the intended command.
"""

from __future__ import annotations

import shlex
from typing import Optional

# Default profiles directory inside the Airflow containers, matching the compose
# mount described in the design (``./dbt`` → ``/opt/airflow/dbt``).
DEFAULT_PROFILES_DIR = "/opt/airflow/dbt"


def _build(
    subcommand: str, select: Optional[str], profiles_dir: str
) -> str:
    """Assemble a ``dbt <subcommand>`` command string with quoted arguments."""
    parts = ["dbt", subcommand]
    if select:
        parts += ["--select", shlex.quote(select)]
    parts += ["--profiles-dir", shlex.quote(profiles_dir)]
    return " ".join(parts)


def run_dbt(
    select: Optional[str] = None, profiles_dir: str = DEFAULT_PROFILES_DIR
) -> str:
    """Return a ``dbt run`` command string.

    Args:
        select: optional dbt node selector (e.g. ``"staging"``). When omitted,
            dbt runs its default selection.
        profiles_dir: directory containing ``profiles.yml``.

    Returns:
        A ``BashOperator``-compatible command string.
    """
    return _build("run", select, profiles_dir)


def test_dbt(
    select: Optional[str] = None, profiles_dir: str = DEFAULT_PROFILES_DIR
) -> str:
    """Return a ``dbt test`` command string.

    Args:
        select: optional dbt node selector (e.g. ``"staging"``).
        profiles_dir: directory containing ``profiles.yml``.

    Returns:
        A ``BashOperator``-compatible command string.
    """
    return _build("test", select, profiles_dir)
