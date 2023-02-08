import os
from datetime import timedelta
import pandas as pd
from pandas.io.parsers import TextFileReader
from sqlalchemy.engine import Engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector
from pathlib import Path
from typing import Tuple


@task(
    log_prints=True,
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url: str) -> Tuple[TextFileReader, Path]:
    csv_buffer_path = Path() / "buffer.csv"
    gz_buffer_path = Path() / "buffer.csv.gz"

    if not url.endswith(".csv.gz"):
        raise Exception("Url needs to end with .csv.gz")

    os.system(f"wget {url} -O {gz_buffer_path}")
    os.system(f"gunzip -f {gz_buffer_path} {csv_buffer_path}")

    df = pd.read_csv(csv_buffer_path, chunksize=100000)

    return df, csv_buffer_path


@task(log_prints=True)
def transform_data(df: pd.DataFrame):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df["passenger_count"] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")

    return df


@task(log_prints=True, retries=3)
def load_data(engine: Engine, df: pd.DataFrame, table_name: str):
    # Ensure type
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # Create table if doesnt exist, delete and replace the old one if exist
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
    df.to_sql(name=table_name, con=engine, if_exists="append")


@task(log_prints=True, retries=3)
def load_data_v2(df: pd.DataFrame, table_name: str):
    connection_block = SqlAlchemyConnector.load("ny-taxi")

    with connection_block.get_connection(begin=False) as engine:
        # Ensure type
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        # Create table if doesnt exist, delete and replace the old one if exist
        df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")
        df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Web to CSV to Postgres")
def main_flow():
    table_name = "green_taxi_trips_prefect"
    csv_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/"
        "releases/download/green/green_tripdata_2020-11.csv.gz"
    )

    df, csv_buffer = extract_data(csv_url)
    df = transform_data(df)
    load_data_v2(df, table_name)

    try:
        csv_buffer.unlink()
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    main_flow()
