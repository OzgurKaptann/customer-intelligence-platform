"""Synthetic marketing campaign generation for the Raw Zone (Task 9).

Generates ``raw.campaigns`` daily-metric records and upserts them on the
composite natural key ``(campaign_id, campaign_date)`` — one row per campaign
per day — for idempotent re-runs (Requirement 1.8). Each campaign runs for a
span of consecutive days, emitting one daily row each.

Invariants enforced at generation (Requirement 1.4 / Property 2):

* ``platform`` ∈ ``{google_ads, meta_ads}``
* ``daily_spend_usd`` ≥ 0
* ``impressions`` ≥ 0 and ``clicks`` ≥ 0
* ``clicks`` ≤ ``impressions`` — never exceeded

Determinism: IDs, dates, and metrics come from a seeded ``random.Random`` so
repeated runs reproduce the same rows and therefore the same row count.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal

from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# The two ad platforms defined by Requirement 1.4.
PLATFORMS = ("google_ads", "meta_ads")

# Fixed seed distinct from the other domain generators → deterministic IDs.
_SEED = 550_001

# A campaign runs for a random span within this range, one daily row per day.
_MIN_RUN_DAYS = 1
_MAX_RUN_DAYS = 14

# Campaign start dates fall within this many days before the run date.
_WINDOW_DAYS = 90

_TWO_PLACES = Decimal("0.01")

# Composite primary key (campaign_id, campaign_date): one record per campaign per
# day. A defensive CHECK mirrors the generation-time clicks ≤ impressions rule.
_CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.campaigns (
    campaign_id     VARCHAR(36)   NOT NULL,
    platform        VARCHAR(20)   NOT NULL,
    campaign_name   VARCHAR(255)  NOT NULL,
    daily_spend_usd NUMERIC(12,2) NOT NULL CHECK (daily_spend_usd >= 0),
    impressions     INTEGER       NOT NULL CHECK (impressions >= 0),
    clicks          INTEGER       NOT NULL CHECK (clicks >= 0 AND clicks <= impressions),
    campaign_date   DATE          NOT NULL,
    _ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    _run_date       DATE          NOT NULL,
    PRIMARY KEY (campaign_id, campaign_date)
);

CREATE INDEX IF NOT EXISTS ix_raw_campaigns_campaign_date ON raw.campaigns (campaign_date);
CREATE INDEX IF NOT EXISTS ix_raw_campaigns_run_date      ON raw.campaigns (_run_date);
"""

_UPSERT_SQL = """
INSERT INTO raw.campaigns
    (campaign_id, platform, campaign_name, daily_spend_usd, impressions, clicks, campaign_date, _run_date)
VALUES %s
ON CONFLICT (campaign_id, campaign_date) DO UPDATE SET
    platform        = EXCLUDED.platform,
    campaign_name   = EXCLUDED.campaign_name,
    daily_spend_usd = EXCLUDED.daily_spend_usd,
    impressions     = EXCLUDED.impressions,
    clicks          = EXCLUDED.clicks,
    _run_date       = EXCLUDED._run_date
"""


def ensure_tables(conn) -> None:
    """Create the ``raw`` schema and ``raw.campaigns`` table if absent."""
    with conn.cursor() as cur:
        cur.execute(_CREATE_SQL)


def _det_uuid(rng: random.Random) -> str:
    """Return a deterministic UUID4-formatted string from a seeded RNG."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def generate_campaigns(
    conn,
    n_min: int = 500,
    n_max: int = 2_000,
    *,
    n: int | None = None,
    run_date: date | None = None,
    batch_size: int = 1_000,
) -> int:
    """Generate and upsert campaign daily-metric records into ``raw.campaigns``.

    The total number of daily records is ``n`` when provided, otherwise a value
    drawn from ``[n_min, n_max]``; supplied values are clamped into that range.
    Records are produced campaign-by-campaign (each spanning several days) until
    the target count is reached.

    Args:
        conn: an open psycopg2 connection; the caller owns the transaction.
        n_min: lower bound of the record volume (default 500).
        n_max: upper bound of the record volume (default 2,000).
        n: explicit record count override (e.g. from ``SEED_CAMPAIGNS``);
            clamped into ``[n_min, n_max]`` when supplied.
        run_date: pipeline run-date partition value; defaults to today (UTC).
        batch_size: rows per ``execute_values`` round-trip.

    Returns:
        The number of campaign daily records written.
    """
    if run_date is None:
        run_date = datetime.now(timezone.utc).date()

    rng = random.Random(_SEED)

    total = rng.randint(n_min, n_max) if n is None else n
    total = max(n_min, min(total, n_max))

    batch: list[tuple] = []
    written = 0

    with conn.cursor() as cur:
        while written + len(batch) < total:
            campaign_id = _det_uuid(rng)
            platform = rng.choice(PLATFORMS)
            campaign_name = f"{platform}-campaign-{rng.randint(1000, 9999)}"
            start = run_date - timedelta(days=rng.randint(0, _WINDOW_DAYS))
            run_days = rng.randint(_MIN_RUN_DAYS, _MAX_RUN_DAYS)

            for day_offset in range(run_days):
                if written + len(batch) >= total:
                    break
                campaign_date = start + timedelta(days=day_offset)
                impressions = rng.randint(0, 500_000)
                # clicks ≤ impressions is guaranteed by capping at impressions
                # (Requirement 1.4 / Property 2).
                clicks = rng.randint(0, impressions)
                daily_spend = Decimal(str(round(rng.uniform(0.0, 5_000.0), 2))).quantize(
                    _TWO_PLACES, rounding=ROUND_HALF_UP
                )
                batch.append(
                    (
                        campaign_id,
                        platform,
                        campaign_name,
                        daily_spend,
                        impressions,
                        clicks,
                        campaign_date,
                        run_date,
                    )
                )

                if len(batch) >= batch_size:
                    execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
                    written += len(batch)
                    batch.clear()

        if batch:
            execute_values(cur, _UPSERT_SQL, batch, page_size=batch_size)
            written += len(batch)

    log.info("Upserted %d campaign records into raw.campaigns", written)
    return written
