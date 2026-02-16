with source as (
    select * from {{ source('bigquery_data', 'fhv_tripdata_2019') }}
),

renamed as (
    select
        -- identifiers (standardized naming for consistency across yellow/green)
        cast(dispatching_base_num as string) as dispatch_num,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(SR_Flag as string) as sr_flag,
        cast(Affiliated_base_number as string) as base_num,

        -- timestamps (standardized naming)
        cast(pickup_datetime as timestamp) as pickup_datetime,  -- tpep = Taxicab Passenger Enhancement Program (yellow taxis)
        cast(dropOff_datetime as timestamp) as dropoff_datetime

    from source
    -- Filter out records with null vendor_id (data quality requirement)
    where dispatching_base_num is not null
)

select * from renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
where pickup_datetime >= '2019-01-01' and pickup_datetime < '2019-02-01'
{% endif %}