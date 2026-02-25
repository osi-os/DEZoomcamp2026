"""dlt pipeline to ingest paginated NYC taxi data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def nyc_taxi_rest_api_source():
    """Define dlt resources from NYC taxi REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # Base URL for the NYC taxi Cloud Function API
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        },
        "resources": [
            {
                "name": "nyc_taxi_trips",
                "endpoint": {
                    # The Cloud Function is served at the base URL, so no extra path is needed.
                    "path": "",
                    # Configure page-number pagination: 1,000 records per page,
                    # keep requesting pages until an empty page is returned.
                    "paginator": {
                        "type": "page_number",
                        "base_page": 1,
                        "page_param": "page",
                        # Avoid relying on a `total` field in the response:
                        # cap the maximum page index and stop when an empty page is returned.
                        "maximum_page": 100000,
                        "stop_after_empty_page": True,
                    },
                    # Let dlt auto-detect where the list of records lives in the response.
                    # If needed, set "data_selector" explicitly later.
                },
            }
        ],
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    # Keep pipeline state (and DuckDB file) local to this directory.
    destination="duckdb",
    # Clean data and state on each run while developing.
    refresh="drop_sources",
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(nyc_taxi_rest_api_source())
    print(load_info)  # noqa: T201

