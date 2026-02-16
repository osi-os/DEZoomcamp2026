# de-camp-docker-workshop

## First Module & Homework in the DE Zoomcamp Series

Answers below -

1. 
osiosman@MacBookPro ~/DEZoomcamp2026 [125]> docker run -it --entrypoint=bash --rm python:3.13
root@2544286ca0c7:/# pip --version
pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)

2.
The answer is postgres:5432 because the name of the container pgadmin needs to connect to is
"postgres", and the port that belongs to it is identified by the 2nd number in the ports section, 5432:

services:
  db:
    container_name: postgres
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: 'postgres'
      POSTGRES_PASSWORD: 'postgres'
      POSTGRES_DB: 'ny_taxi'
    ports:
      - '5433:5432'
    volumes:
      - vol-pgdata:/var/lib/postgresql/data

  pgadmin:
    container_name: pgadmin
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: "pgadmin@pgadmin.com"
      PGADMIN_DEFAULT_PASSWORD: "pgadmin"
    ports:
      - "8080:80"
    volumes:
      - vol-pgadmin_data:/var/lib/pgadmin

volumes:
  vol-pgdata:
    name: vol-pgdata
  vol-pgadmin_data:
    name: vol-pgadmin_data


3.
8007

SELECT COUNT(*)
FROM green_trip_data
WHERE green_trip_data."lpep_pickup_datetime" >= '2025-11-01'
	AND green_trip_data."lpep_pickup_datetime" < '2025-12-01'
	AND green_trip_data.trip_distance <= 1

4.
2025-11-14

SELECT * 
FROM green_trip_data
WHERE green_trip_data.trip_distance < 100
ORDER BY green_trip_data.trip_distance DESC;

5.
East Harlem North

SELECT taxi_zone_data."Zone", SUM(green_trip_data.total_amount) AS total_amount
FROM green_trip_data
INNER JOIN taxi_zone_data ON green_trip_data."PULocationID" = taxi_zone_data."LocationID"
WHERE green_trip_data.lpep_pickup_datetime >= '2025-11-18'
	AND green_trip_data.lpep_pickup_datetime < '2025-11-19'
GROUP BY taxi_zone_data."Zone"
ORDER BY total_amount DESC;

6.
Yorkville West

SELECT MAX(green_trip_data.tip_amount) as big_tip, green_trip_data."DOLocationID"
FROM green_trip_data
INNER JOIN taxi_zone_data ON green_trip_data."PULocationID" = taxi_zone_data."LocationID"
WHERE green_trip_data."PULocationID" = 74
	AND green_trip_data.lpep_pickup_datetime >= '2025-11-01'
	AND green_trip_data.lpep_pickup_datetime < '2025-12-01'
GROUP BY green_trip_data."DOLocationID"
ORDER BY big_tip DESC;

select * from taxi_zone_data
where taxi_zone_data."LocationID" = 263;

7.
Please see the main.tf in the terrademo1 folder

terraform init, terraform apply -auto-approve, terraform destroy



## Second Module & Homework in the DE Zoomcamp Series


*Backfilling the data for the specified time period: From 1/1/2021 to 7/31/2021 for the yellow and green taxi data:


<img width="1154" height="699" alt="image" src="https://github.com/user-attachments/assets/83775356-86c5-40e1-a1e3-aa482c10f806" />
<img width="1160" height="332" alt="image" src="https://github.com/user-attachments/assets/c48c186b-9036-4e3e-8880-b4d09fc1d136" />


1. **134.5MB**
   <img width="1326" height="116" alt="image" src="https://github.com/user-attachments/assets/5ab33953-a7aa-4011-a483-ce48050803ed" />




2. **green_tripdata_2020-04.csv**
   <img width="1114" height="67" alt="image" src="https://github.com/user-attachments/assets/7b10be19-ba88-4820-b178-ce96fcae5846" />



3. **24,648,499**

--Rows for the Yellow Taxi data in 2020

SELECT COUNT(*) FROM `kestra-learning-485617.zoomcamp.yellow_tripdata`
WHERE filename >= "yellow_tripdata_2020-01.csv"
  AND filename < "yellow_tripdata_2021-01.csv"


4. **1734051**

--Rows for the Green Taxi data in 2020

SELECT COUNT(*) FROM `kestra-learning-485617.zoomcamp.green_tripdata`
WHERE filename >= "green_tripdata_2020-01.csv"
  AND filename < "green_tripdata_2021-01.csv"



5. **1925152**

<img width="621" height="553" alt="image" src="https://github.com/user-attachments/assets/b73ad18a-3e8a-48ff-9fa8-d27fd1d9ba8d" />




6. **Add a timezone property set to America/New_York in the Schedule trigger configuration**

triggers:

- id: green_schedule
  type: io.kestra.plugin.core.trigger.Schedule
  cron: "0 9 1 * *"
  inputs: 
  taxi: green

- id: yellow_schedule
  type: io.kestra.plugin.core.trigger.Schedule
  cron: "0 10 1 * *"
  inputs:
  taxi: yellow

- id: new_york_schedule
  type: io.kestra.plugin.core.trigger.Schedule
  cron: "0 11 1 * *"
  timezone: America/New_York


## Third Module & Homework in the DE Zoomcamp Series


CREATE OR REPLACE EXTERNAL TABLE `kestra-learning-485617.homework3.yellow_taxi_data_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://homework_3_de_bucket/yellow_tripdata_2024-*.parquet']
);

CREATE OR REPLACE TABLE homework3.yellow_taxi_data_2024_reg AS 
SELECT * FROM `homework3.yellow_taxi_data_2024_external`;

1.
SELECT COUNT(*)
FROM `homework3.yellow_taxi_data_2024_reg`
-- 20,3332,093

2.
SELECT COUNT(DISTINCT(PULocationID))
FROM homework3.yellow_taxi_data_2024_external;

SELECT COUNT(DISTINCT(PULocationID))
FROM homework3.yellow_taxi_data_2024_reg;
-- 0 MB and 155.12 MB

3.
SELECT PULocationID
FROM homework3.yellow_taxi_data_2024_reg;

SELECT PULocationID, DOLocationID
FROM homework3.yellow_taxi_data_2024_reg;
-- BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

4.
SELECT COUNT(*)
FROM `homework3.yellow_taxi_data_2024_external`
WHERE fare_amount = 0;
-- 8333

5.
CREATE OR REPLACE TABLE homework3.yellow_taxi_data_2024_parclu
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS 
SELECT * FROM `homework3.yellow_taxi_data_2024_external`;
-- Partition by tpep_dropoff_datetime and Cluster on VendorID

6.
SELECT DISTINCT(VendorID)
FROM `homework3.yellow_taxi_data_2024_reg`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <= '2024-03-15';

SELECT DISTINCT(VendorID)
FROM `homework3.yellow_taxi_data_2024_parclu`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime <= '2024-03-15';
-- 310.24 MB and 26.84 MB

7.
-- GCP Bucket

8.
-- False

9.
SELECT * FROM `homework3.yellow_taxi_data_2024_reg`
-- 2.72 GB because the query has to run through every column in the table to extract every entry


## Fourth Module & Homework in the DE Zoomcamp Series

1.
dbt run --select int_trips_unioned

The above means that only the int_trips_unioned model will be built.

2.
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false

Your model fct_trips has been running successfully for months. A new value 6 now appears in the source data.

When I run dbt test --select fct_trips, dbt will fail the test, returning a non-zero exit code.

3.
After running your dbt project, the count of records in the fct_monthly_zone_revenue model:

select COUNT(*) from `dbt_osios.fct_monthly_zone_revenue`
12184

4.
Using the fct_monthly_zone_revenue table, find the pickup zone with the highest total revenue (revenue_monthly_total_amount) for Green taxi trips in 2020.

SELECT 
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS revenue_2020
FROM `dbt_osios.fct_monthly_zone_revenue`
WHERE revenue_month >= '2020-01-01' AND revenue_month < '2021-01-01' AND service_type = 'Green'
GROUP BY pickup_zone
ORDER BY revenue_2020 DESC

East Harlem North

5.
Using the fct_monthly_zone_revenue table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?

SELECT
  SUM(total_monthly_trips) AS oct2019_trips
FROM `dbt_osios.fct_monthly_zone_revenue`
WHERE revenue_month = '2019-10-01' AND service_type = 'Green'

384624

6.

