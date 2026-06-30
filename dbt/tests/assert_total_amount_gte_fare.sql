-- Singular test: total_amount should never be less than fare_amount, since tips
-- and surcharges only add to the fare. Any returned row is a failure.
select
    trip_id,
    fare_amount,
    total_amount
from {{ ref('fct_trips') }}
where total_amount < fare_amount
