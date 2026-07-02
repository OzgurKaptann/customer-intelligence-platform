{{ config(severity='warn') }}

-- Volume guard for mart_customer_360 (Requirement 4.4): the current run date's
-- row count must stay within +/-20% of the prior run date's row count.
--
-- mart_customer_360 is incremental (delete+insert on _run_date) and therefore
-- retains one partition per prior run date, so the prior-day baseline is read
-- directly from the model's own retained partitions rather than recomputed.
-- When no prior partition exists (first run), the cross join yields no rows and
-- the test passes -- baseline establishment is recorded by the pipeline layer
-- (observability.pipeline_run_log) rather than mutated from a read-only test,
-- since a dbt data test compiles to a SELECT and cannot perform an INSERT.
--
-- Severity is configured as `warn` so a volume deviation surfaces in the run
-- summary and Orchestrator UI without halting the pipeline.
with counts_by_run as (

    select
        _run_date,
        count(*) as row_count
    from {{ ref('mart_customer_360') }}
    group by _run_date

),

current_run as (

    select row_count
    from counts_by_run
    where _run_date = {{ get_run_date() }}

),

prior_run as (

    select row_count
    from counts_by_run
    where _run_date < {{ get_run_date() }}
    order by _run_date desc
    limit 1

)

select
    c.row_count as current_row_count,
    p.row_count as prior_row_count,
    abs(c.row_count - p.row_count)::numeric / p.row_count as deviation_pct
from current_run as c
cross join prior_run as p
where p.row_count > 0
  and abs(c.row_count - p.row_count)::numeric / p.row_count > 0.20
