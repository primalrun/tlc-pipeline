with trips as (
    select * from {{ ref('fct_trips') }}
)

select
    trip_month,
    vendor_name,
    payment_type_desc,

    count(*)                                    as trip_count,
    sum(passenger_count)                        as total_passengers,
    round(avg(trip_distance), 2)                as avg_trip_distance_miles,
    round(avg(trip_duration_minutes), 2)        as avg_trip_duration_minutes,
    round(avg(fare_amount), 2)                  as avg_fare,
    round(avg(tip_amount), 2)                   as avg_tip,
    round(avg(tip_pct), 2)                      as avg_tip_pct,
    round(avg(total_amount), 2)                 as avg_total_amount,
    round(sum(total_amount), 2)                 as total_revenue,
    round(avg(cost_per_mile), 2)                as avg_cost_per_mile

from trips
group by 1, 2, 3
order by 1, 2, 3
