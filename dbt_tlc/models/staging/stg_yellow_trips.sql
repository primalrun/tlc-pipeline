with source as (
    select * from {{ source('raw', 'yellow_trips') }}
),

pu_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
),

do_zones as (
    select * from {{ ref('taxi_zone_lookup') }}
),

cleaned as (
    select
        s.vendor_id,
        s.pickup_datetime,
        s.dropoff_datetime,
        s.passenger_count,
        s.trip_distance,
        s.ratecode_id,
        s.store_and_fwd_flag,
        s.pu_location_id,
        s.do_location_id,
        s.payment_type,
        s.fare_amount,
        s.extra,
        s.mta_tax,
        s.tip_amount,
        s.tolls_amount,
        s.improvement_surcharge,
        s.congestion_surcharge,
        s.airport_fee,
        s.total_amount,
        s.trip_duration_minutes,
        s.cost_per_mile,

        -- Derived
        case s.payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_type_desc,

        case s.vendor_id
            when 1 then 'Creative Mobile Technologies'
            when 2 then 'VeriFone'
            else 'Unknown'
        end as vendor_name,

        case s.ratecode_id
            when 1 then 'Standard Rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated Fare'
            when 6 then 'Group Ride'
            else 'Unknown'
        end as ratecode_desc,

        case s.store_and_fwd_flag
            when 'Y' then 'Store and Forward'
            when 'N' then 'Live Trip'
            else 'Unknown'
        end as store_and_fwd_desc,

        pu.Borough  as pu_borough,
        pu.Zone     as pu_zone,
        do_.Borough as do_borough,
        do_.Zone    as do_zone,

        date_trunc('month', s.pickup_datetime) as trip_month

    from source s
    left join pu_zones pu  on s.pu_location_id = pu.LocationID
    left join do_zones do_ on s.do_location_id = do_.LocationID
    where
        s.trip_duration_minutes > 0
        and s.trip_duration_minutes < 480  -- exclude trips > 8 hours
        and s.cost_per_mile < 100          -- exclude extreme outliers
)

select * from cleaned
