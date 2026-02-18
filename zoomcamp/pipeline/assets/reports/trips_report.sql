/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips

# Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (pickup_datetime) for consistency.
materialization:
  type: table
  # suggested strategy: time_interval
  # strategy: time_interval
  # set to your report's date column (must match staging incremental_key)
  # incremental_key: pickup_datetime
  # set to `date` or `timestamp`
  # time_granularity: timestamp

# Define report columns + primary key(s) at your chosen level of aggregation.
# Primary key is the combination of date + taxi_type + payment_type_name (aggregation grain).
columns:
  - name: pickup_date
    type: date
    description: "Calendar date of the trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment method name"
    primary_key: true
  - name: total_trips
    type: integer
    description: "Number of trips for this date / taxi_type / payment_type combination"
    checks:
      - name: non_negative
  - name: total_passengers
    type: integer
    description: "Sum of passengers across all trips"
    checks:
      - name: non_negative
  - name: total_distance_miles
    type: float
    description: "Sum of trip distances in miles"
    checks:
      - name: non_negative
  - name: total_fare_amount
    type: float
    description: "Sum of base fares"
    checks:
      - name: non_negative
  - name: total_tip_amount
    type: float
    description: "Sum of tip amounts"
    checks:
      - name: non_negative
  - name: total_revenue
    type: float
    description: "Sum of total_amount charged to passengers"
    checks:
      - name: non_negative
  - name: avg_trip_distance_miles
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: non_negative
  - name: avg_fare_amount
    type: float
    description: "Average base fare per trip"
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- - Group by pickup_date, taxi_type, and payment_type_name (the report grain)
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
    CAST(pickup_datetime AS DATE)   AS pickup_date,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*)                        AS total_trips,
    SUM(passenger_count)            AS total_passengers,
    ROUND(SUM(trip_distance), 2)    AS total_distance_miles,
    ROUND(SUM(fare_amount), 2)      AS total_fare_amount,
    ROUND(SUM(tip_amount), 2)       AS total_tip_amount,
    ROUND(SUM(total_amount), 2)     AS total_revenue,
    ROUND(AVG(trip_distance), 2)    AS avg_trip_distance_miles,
    ROUND(AVG(fare_amount), 2)      AS avg_fare_amount
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    COALESCE(payment_type_name, 'unknown')
