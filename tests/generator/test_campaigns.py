"""Campaign generation invariants (Task 10).

Covers Requirement 1.4 / Property 2: for every generated campaign record,
clicks never exceed impressions (and both, plus daily spend, are non-negative).
"""

from __future__ import annotations

from datetime import date

import campaigns

RUN_DATE = date(2026, 7, 1)


def test_clicks_never_exceed_impressions(conn):
    """No campaign row has clicks > impressions."""
    campaigns.ensure_tables(conn)
    written = campaigns.generate_campaigns(
        conn, n_min=300, n_max=300, n=300, run_date=RUN_DATE
    )
    assert written == 300

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw.campaigns WHERE clicks > impressions")
        assert cur.fetchone()[0] == 0


def test_campaign_metrics_are_non_negative(conn):
    """Impressions, clicks, and daily spend are all non-negative."""
    campaigns.ensure_tables(conn)
    campaigns.generate_campaigns(conn, n_min=300, n_max=300, n=300, run_date=RUN_DATE)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM raw.campaigns
            WHERE impressions < 0 OR clicks < 0 OR daily_spend_usd < 0
            """
        )
        assert cur.fetchone()[0] == 0


def test_platform_in_accepted_set(conn):
    """Platform is one of the two accepted ad platforms."""
    campaigns.ensure_tables(conn)
    campaigns.generate_campaigns(conn, n_min=200, n_max=200, n=200, run_date=RUN_DATE)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM raw.campaigns "
            "WHERE platform NOT IN ('google_ads', 'meta_ads')"
        )
        assert cur.fetchone()[0] == 0
