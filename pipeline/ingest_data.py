#!/usr/bin/env python
# coding: utf-8

#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm.auto import tqdm

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def ingest_data(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> pd.DataFrame:
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    first_chunk = next(df_iter)

    first_chunk.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    first_chunk.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print(f"Inserted first chunk: {len(first_chunk)}")

    for df_chunk in tqdm(df_iter):
        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append"
        )
        print(f"Inserted chunk: {len(df_chunk)}")

    print(f'done ingesting to {target_table}')


import click


@click.command()
@click.option("--pg-user", default="root", show_default=True, help="Postgres user")
@click.option("--pg-pass", default="root", show_default=True, help="Postgres password")
@click.option("--pg-host", default="localhost", show_default=True, help="Postgres host")
@click.option("--pg-port", default=5432, type=int, show_default=True, help="Postgres port")
@click.option("--pg-db", default="ny_taxi", show_default=True, help="Postgres database name")
@click.option("--year", default=2021, type=int, show_default=True, help="Year of the dataset")
@click.option("--month", default=1, type=int, show_default=True, help="Month of the dataset")
@click.option("--chunksize", default=100000, type=int, show_default=True, help="CSV read chunksize")
@click.option("--target-table", default="yellow_taxi_data", show_default=True, help="Target table name in Postgres")
@click.option("--dry-run", is_flag=True, default=False, help="Check DB connectivity and print dataset URL without ingesting")

def run(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table, dry_run):
    """CLI entrypoint for ingesting data into Postgres."""
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    url_prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'

    url = f'{url_prefix}/yellow_tripdata_{year:04d}-{month:02d}.csv.gz'

    if dry_run:
        # Verify database connectivity and print the dataset URL without ingesting.
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            click.echo(f"Connection OK to {pg_user}@{pg_host}:{pg_port}/{pg_db}")
            click.echo(f"Dataset URL: {url}")
        except Exception as e:
            click.echo(f"Connection failed: {e}", err=True)
            raise click.Abort()
        return

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )


if __name__ == '__main__':
    run()


# import pandas as pd
# from sqlalchemy import create_engine
# from tqdm.auto import tqdm

# year = 2021
# month = 1

# dtype = {
#     "VendorID": "Int64",
#     "passenger_count": "Int64",
#     "trip_distance": "float64",
#     "RatecodeID": "Int64",
#     "store_and_fwd_flag": "string",
#     "PULocationID": "Int64",
#     "DOLocationID": "Int64",
#     "payment_type": "Int64",
#     "fare_amount": "float64",
#     "extra": "float64",
#     "mta_tax": "float64",
#     "tip_amount": "float64",
#     "tolls_amount": "float64",
#     "improvement_surcharge": "float64",
#     "total_amount": "float64",
#     "congestion_surcharge": "float64"
# }

# parse_dates = [
#     "tpep_pickup_datetime",
#     "tpep_dropoff_datetime"
# ]


# get_ipython().system('uv add sqlalchemy psycopg2-binary')


# prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
# url = f"{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz"
# url

# engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')



# print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))


# len(df)

# def run():
#     pg_user = "root"
#     pg_pass = "root"
#     pg_host = "localhost"
#     pg_port = "5432"
#     pg_db = "ny_taxi"
#     chunksize = 100000

#     year = 2021
#     month = 1

#     df_iter = pd.read_csv(
#         url,
#         dtype=dtype,
#         parse_dates=parse_dates,
#         iterator=True,
#         chunksize=chunksize
#     )


# for df in df_iter:
#     print(len(df))


# get_ipython().system('uv add tqdm')



# first = True

# for df_chunk in tqdm(df_iter):

#     if first:
#         # Create table schema (no data)
#         df_chunk.head(0).to_sql(
#             name="yellow_taxi_data",
#             con=engine,
#             if_exists="replace"
#         )
#         first = False
#         print("Table created")

#     # Insert chunk
#     df_chunk.to_sql(
#         name="yellow_taxi_data",
#         con=engine,
#         if_exists="append"
#     )

#     print("Inserted:", len(df_chunk))


# # In[ ]:




