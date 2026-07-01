"""Synthetic data generator entry point.

Orchestrates generation of all five raw source domains — CRM (customers),
Orders (orders + order items), Events, Campaigns, and Tickets — then runs
referential-integrity assertions across the customer-referencing domains.

Configuration is read entirely from environment variables:

* ``DATABASE_URL``    — libpq connection URI (required).
* ``SEED_CUSTOMERS``  — number of customers to generate (default 100000).
* ``SEED_ORDERS``     — number of orders to generate (default 250000).
* ``SEED_EVENTS``     — number of events to generate (default 1000000, clamped
  to [1000000, 5000000] and raised to at least the customer count).
* ``SEED_CAMPAIGNS``  — number of campaign daily records (default 1000, clamped
  to [500, 2000]).
* ``SEED_TICKETS``    — number of tickets to generate (default 50000).

Transaction semantics: schema/table DDL is applied idempotently first, then all
data generation runs inside a single transaction. On any exception the
transaction is rolled back completely and the process exits non-zero
(Requirement 1.7, 1.8).
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone

import psycopg2

import campaigns
import customers
import events
import integrity
import orders
import tickets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("generator")


def _env_int(name: str, default: int) -> int:
    """Read a positive integer from the environment, falling back to ``default``."""
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be non-negative, got {value}")
    return value


def run() -> None:
    """Generate customers and orders, then assert referential integrity."""
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL environment variable is not set")

    n_customers = _env_int("SEED_CUSTOMERS", 100_000)
    n_orders = _env_int("SEED_ORDERS", 250_000)
    n_events = _env_int("SEED_EVENTS", 1_000_000)
    n_campaigns = _env_int("SEED_CAMPAIGNS", 1_000)
    n_tickets = _env_int("SEED_TICKETS", 50_000)
    run_date = datetime.now(timezone.utc).date()

    log.info(
        "Starting generation: customers=%d orders=%d events=%d campaigns=%d "
        "tickets=%d run_date=%s",
        n_customers,
        n_orders,
        n_events,
        n_campaigns,
        n_tickets,
        run_date,
    )

    conn = psycopg2.connect(dsn)
    try:
        # 1. Idempotent DDL (committed on its own; not part of the data txn).
        conn.autocommit = True
        customers.ensure_tables(conn)
        orders.ensure_tables(conn)
        events.ensure_tables(conn)
        campaigns.ensure_tables(conn)
        tickets.ensure_tables(conn)

        # 2. Single transaction for all data generation + integrity assertions.
        conn.autocommit = False
        customer_ids = customers.generate_customers(
            conn, n_customers, run_date=run_date
        )
        orders.generate_orders(conn, customer_ids, n_orders, run_date=run_date)
        events.generate_events(conn, customer_ids, n=n_events, run_date=run_date)
        campaigns.generate_campaigns(conn, n=n_campaigns, run_date=run_date)
        tickets.generate_tickets(conn, customer_ids, n_tickets, run_date=run_date)
        integrity.assert_referential_integrity(conn)

        conn.commit()
        log.info("Generation complete; transaction committed.")
    except Exception:
        conn.rollback()
        log.exception("Generation failed; transaction rolled back.")
        raise
    finally:
        conn.close()


def main() -> int:
    """CLI wrapper returning a process exit code."""
    try:
        run()
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
