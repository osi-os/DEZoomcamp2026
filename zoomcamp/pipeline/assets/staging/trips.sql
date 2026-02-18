/* @bruin

# Docs:
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks (built-ins): https://getbruin.com/docs/bruin/quality/available_checks
# - Custom checks: https://getbruin.com/docs/bruin/quality/custom

# Set the asset name (recommended: staging.trips).
name: staging.trips

# Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# Declare dependencies so `bruin run ... --downstream` and lineage work.
depends:
  - ingestion.trips
  - ingestion.payment_lookup

# Choose time-based incremental processing if the dataset is naturally time-windowed.
# - This module expects you to use `time_interval` to reprocess only the requested window.
materialization:
  # What is materialization?
  # Materialization tells Bruin how to turn your SELECT query into a persisted dataset.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  #
  # Materialization "type":
  # - table: persisted table
  # - view: persisted view (if the platform supports it)
  type: table
  # set a materialization strategy.
  # Docs: https://getbruin.com/docs/bruin/assets/materialization
  # suggested strategy: time_interval
  #
  # Incremental strategies (what does "incremental" mean?):
  # Incremental means you update only part of the destination instead of rebuilding everything every run.
  # In Bruin, this is controlled by `strategy` plus keys like `incremental_key` and `time_granularity`.
  #
  # Common strategies you can choose from (see docs for full list):
  # - create+replace (full rebuild)
  # - truncate+insert (full refresh without drop/create)
  # - append (insert new rows only)
  # - delete+insert (refresh partitions based on incremental_key values)
  # - merge (upsert based on primary key)
  # - time_interval (refresh rows within a time window)
  strategy: time_interval
  # set incremental_key to your event time column (DATE or TIMESTAMP).
  incremental_key: pickup_datetime
  # choose `date` vs `timestamp` based on the incremental_key type.
  time_granularity: timestamp

# Define output columns, mark primary keys, and add a few checks.
# Composite primary key: the 5 columns together uniquely identify a trip (no single unique ID in raw data).
columns:
  - name: pickup_datetime
    type: timestamp
    description: "Date and time when the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Date and time when the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC taxi zone where the trip started"
    primary_key: true
  - name: dropoff_location_id
    type: integer
    description: "TLC taxi zone where the trip ended"
    primary_key: true
  - name: fare_amount
    type: float
    description: "Base fare calculated by the meter"
    primary_key: true
    checks:
      - name: non_negative
  - name: total_amount
    type: float
    description: "Total amount charged to passengers (excludes cash tips)"
    checks:
      - name: non_negative
  - name: trip_distance
    type: float
    description: "Elapsed trip distance in miles"
    checks:
      - name: non_negative
  - name: payment_type_name
    type: string
    description: "Human-readable payment method name, joined from ingestion.payment_lookup"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    checks:
      - name: not_null

# Add one custom check that validates a staging invariant (uniqueness, ranges, etc.)
# Docs: https://getbruin.com/docs/bruin/quality/custom
custom_checks:
  - name: no_duplicate_trips
    description: "Composite key (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount) must be unique"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount))
      FROM staging.trips
    value: 0

@bruin */

-- Purpose of staging:
-- - Deduplicate records (important since ingestion uses append strategy)
-- - Enrich with payment_lookup to get human-readable payment type names
-- - Filter invalid rows (null PKs, negative fares/distances)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH deduped AS (
    SELECT
        *,
        -- Deduplicate on a composite key: no single unique ID exists in raw NYC taxi data.
        -- ROW_NUMBER assigns 1 to the most recently extracted record per composite key.
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
      -- Filter out rows with null PKs or invalid values before deduplication
      AND pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0
      AND trip_distance >= 0
)

SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.ratecode_id,
    t.store_and_fwd_flag,
    t.pickup_location_id,
    t.dropoff_location_id,
    t.payment_type,
    t.fare_amount,
    t.extra,
    t.mta_tax,
    t.tip_amount,
    t.tolls_amount,
    t.improvement_surcharge,
    t.total_amount,
    t.congestion_surcharge,
    t.taxi_type,
    t.extracted_at,
    -- Enrich with human-readable payment type name from lookup table
    pl.payment_type_name
FROM deduped t
LEFT JOIN ingestion.payment_lookup pl
    ON t.payment_type = pl.payment_type_id
WHERE t.row_num = 1
