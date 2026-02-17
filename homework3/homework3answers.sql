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