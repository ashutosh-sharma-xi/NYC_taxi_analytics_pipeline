-- Payment mix and tipping behaviour. Cash tips are typically unrecorded, so
-- expect near-zero tip rates on Cash vs Credit.
select
    payment_type,
    case payment_type
        when 1 then 'Credit card' when 2 then 'Cash'
        when 3 then 'No charge'  when 4 then 'Dispute'
        else 'Other' end                                                  as label,
    count(*)                                                              as trips,
    round(100.0 * count(*) / sum(count(*)) over (), 1)                    as pct_of_trips,
    round(avg(tip_amount), 2)                                            as avg_tip,
    round(100.0 * avg(case when tip_amount > 0 then 1 else 0 end), 1)     as pct_trips_tipped,
    round(100.0 * sum(tip_amount) / nullif(sum(fare_amount), 0), 1)       as tip_pct_of_fare
from analytics_marts.fct_trips
group by 1, 2
order by trips desc;
