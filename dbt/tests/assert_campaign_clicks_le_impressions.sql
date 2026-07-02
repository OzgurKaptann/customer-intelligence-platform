-- Campaign invariant (Requirement 1.4, design mart_campaigns): clicks must never
-- exceed impressions. Returns any mart_campaigns rows that violate
-- clicks <= impressions; the test fails if any such rows exist.
select
    campaign_id,
    campaign_date,
    impressions,
    clicks
from {{ ref('mart_campaigns') }}
where clicks > impressions
