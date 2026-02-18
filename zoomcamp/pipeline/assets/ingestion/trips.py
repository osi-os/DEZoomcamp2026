"""@bruin

# Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# Set the connection.
connection: duckdb-default

# Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # Choose `table` or `view` (ingestion generally should be a table)
  type: table
  # Pick a strategy.
  # suggested strategy: append
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: vendor_id
    type: integer
    description: "Provider that provided the record (1=Creative Mobile, 2=VeriFone)"
  - name: pickup_datetime
    type: timestamp
    description: "Date and time when the trip started"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "Date and time when the trip ended"
  - name: passenger_count
    type: integer
    description: "Number of passengers in the vehicle"
  - name: trip_distance
    type: float
    description: "Elapsed trip distance in miles"
  - name: ratecode_id
    type: integer
    description: "Final rate code in effect at end of trip"
  - name: store_and_fwd_flag
    type: string
    description: "Whether trip record was held in vehicle memory before sending (Y/N)"
  - name: pickup_location_id
    type: integer
    description: "TLC taxi zone where the trip started"
  - name: dropoff_location_id
    type: integer
    description: "TLC taxi zone where the trip ended"
  - name: payment_type
    type: integer
    description: "Payment method numeric code"
  - name: fare_amount
    type: float
    description: "Base fare calculated by the meter"
  - name: extra
    type: float
    description: "Miscellaneous extras and surcharges"
  - name: mta_tax
    type: float
    description: "MTA tax automatically triggered based on metered rate"
  - name: tip_amount
    type: float
    description: "Tip amount (automatically populated for credit card tips)"
  - name: tolls_amount
    type: float
    description: "Total amount of all tolls paid in trip"
  - name: improvement_surcharge
    type: float
    description: "Improvement surcharge assessed at flag drop"
  - name: total_amount
    type: float
    description: "Total amount charged to passengers (excludes cash tips)"
  - name: congestion_surcharge
    type: float
    description: "Congestion surcharge for trips in Manhattan south of 96th St"
  - name: taxi_type
    type: string
    description: "Taxi type (yellow or green)"
    checks:
      - name: not_null
  - name: extracted_at
    type: timestamp
    description: "Timestamp when the record was extracted by the pipeline"

@bruin"""

# Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python
import io
import json
import os
from datetime import datetime, date, timezone

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


# Base URL for TLC parquet files.
# Naming convention: <taxi_type>_tripdata_<year>-<month>.parquet
# Example: yellow_tripdata_2022-03.parquet
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Yellow and green taxis use different column names for pickup/dropoff datetimes.
# We normalize them to `pickup_datetime` / `dropoff_datetime` so staging can use a
# single incremental_key regardless of taxi type.
DATETIME_COLUMNS = {
    "yellow": ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
    "green":  ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
}

# Raw column names from TLC parquet files -> normalized snake_case names
COLUMN_RENAMES = {
    "VendorID":     "vendor_id",
    "RatecodeID":   "ratecode_id",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
}

# Final set of output columns written to ingestion.trips (in order)
OUTPUT_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "ratecode_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "taxi_type",
    "extracted_at",
]


def _iter_months(start: date, end: date):
    """Yield (year, month) tuples for every calendar month in [start, end)."""
    current = start.replace(day=1)
    stop = end.replace(day=1)
    while current < stop:
        yield current.year, current.month
        current += relativedelta(months=1)


def _fetch_parquet(taxi_type: str, year: int, month: int) -> pd.DataFrame | None:
    """Download a single TLC parquet file and return it as a DataFrame.
    Returns None if the file is unavailable (e.g. future month or HTTP error)."""
    url = f"{BASE_URL}/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    print(f"Fetching: {url}")
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        return pd.read_parquet(io.BytesIO(response.content))
    except requests.HTTPError as e:
        print(f"  WARNING: HTTP error for {url}: {e}. Skipping.")
        return None
    except Exception as e:
        print(f"  WARNING: Failed to fetch {url}: {e}. Skipping.")
        return None


def _normalize(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """Rename raw TLC columns to standard names and add pipeline metadata columns."""
    pickup_col, dropoff_col = DATETIME_COLUMNS[taxi_type]

    # Build rename map: include datetime columns specific to this taxi type
    renames = {**COLUMN_RENAMES}
    if pickup_col in df.columns:
        renames[pickup_col] = "pickup_datetime"
    if dropoff_col in df.columns:
        renames[dropoff_col] = "dropoff_datetime"

    df = df.rename(columns=renames)

    # Add metadata columns for lineage and debugging
    df["taxi_type"] = taxi_type
    df["extracted_at"] = datetime.now(timezone.utc)

    # Keep only the known output columns that are present in this DataFrame.
    # Columns that differ between yellow/green (e.g. ehail_fee) are silently dropped here;
    # add them to OUTPUT_COLUMNS above if you want to retain them.
    available = [c for c in OUTPUT_COLUMNS if c in df.columns]
    return df[available]


# Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    Bruin runtime context used here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design:
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - `extracted_at` column records the timestamp of extraction for lineage/debugging.
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    # Read the run's date window from Bruin environment variables
    start_date = date.fromisoformat(os.environ["BRUIN_START_DATE"])
    end_date = date.fromisoformat(os.environ["BRUIN_END_DATE"])

    # Read pipeline variables (taxi_types defaults to ["yellow"] if not set)
    bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    frames = []
    for taxi_type in taxi_types:
        if taxi_type not in DATETIME_COLUMNS:
            print(f"WARNING: Unknown taxi_type '{taxi_type}', skipping.")
            continue
        for year, month in _iter_months(start_date, end_date):
            df = _fetch_parquet(taxi_type, year, month)
            if df is not None and not df.empty:
                frames.append(_normalize(df, taxi_type))

    if not frames:
        print("No data fetched for the given date range and taxi types.")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    result = pd.concat(frames, ignore_index=True)
    print(f"Total rows ingested: {len(result):,}")
    # return final_dataframe
    return result


if __name__ == "__main__":
    df = materialize()
    print(df.head())
