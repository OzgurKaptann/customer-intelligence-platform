"""Campaigns ingestion DAG — daily upsert of campaign daily-metric records into
``raw.campaigns`` (Task 11).

Schedule 02:00 UTC, 3 retries (5-min delay), 45-min timeout, ``catchup=False``.
All task wiring and observability live in :mod:`ingestion_common`.
"""

from __future__ import annotations

from ingestion_common import build_ingestion_dag

dag = build_ingestion_dag("campaigns")
