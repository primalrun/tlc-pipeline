with trips as (
    select * from {{ ref('stg_yellow_trips') }}
)

select
    -- Dimensions
    pickup_datetime,
    dropoff_datetime,
    trip_month,
    vendor_id,
    vendor_name,
    pu_location_id,
    do_location_id,
    passenger_count,
    payment_type,
    payment_type_desc,

    -- Measures
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    congestion_surcharge,
    airport_fee,
    total_amount,
    cost_per_mile,

    -- Tip rate
    case
        when fare_amount > 0 then round(tip_amount / fare_amount * 100, 2)
        else null
    end as tip_pct

from trips
