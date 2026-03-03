with source as (
    select * from {{ source('raw', 'yellow_trips') }}
),

cleaned as (
    select
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        pu_location_id,
        do_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        airport_fee,
        total_amount,
        trip_duration_minutes,
        cost_per_mile,

        -- Derived
        case payment_type
            when 1 then 'Credit Card'
            when 2 then 'Cash'
            when 3 then 'No Charge'
            when 4 then 'Dispute'
            else 'Unknown'
        end as payment_type_desc,

        case vendor_id
            when 1 then 'Creative Mobile Technologies'
            when 2 then 'VeriFone'
            else 'Unknown'
        end as vendor_name,

        case ratecode_id
            when 1 then 'Standard Rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated Fare'
            when 6 then 'Group Ride'
            else 'Unknown'
        end as ratecode_desc,

        case store_and_fwd_flag
            when 'Y' then 'Store and Forward'
            when 'N' then 'Live Trip'
            else 'Unknown'
        end as store_and_fwd_desc,

        date_trunc('month', pickup_datetime) as trip_month

    from source
    where
        trip_duration_minutes > 0
        and trip_duration_minutes < 480  -- exclude trips > 8 hours
        and cost_per_mile < 100          -- exclude extreme outliers
)

select * from cleaned
