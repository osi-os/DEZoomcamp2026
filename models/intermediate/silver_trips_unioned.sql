with 
green_tripdata as (
    select * from {{ ref('bronze_green_tripdata')}}
),

yellow_tripdata as (
    select * from {{ ref('bronze_yellow_tripdata')}}
),

trips_unioned as (
    select * from green_tripdata
    union all
    select * from yellow_tripdata
)


select distinct pickup_location_id from trips_unioned