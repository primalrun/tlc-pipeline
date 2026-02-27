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

        date_trunc('month', pickup_datetime) as trip_month

    from source
    where
        trip_duration_minutes > 0
        and trip_duration_minutes < 480  -- exclude trips > 8 hours
        and cost_per_mile < 100          -- exclude extreme outliers
)

select * from cleaned
