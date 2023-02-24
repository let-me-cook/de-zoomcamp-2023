import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_gcp.cloud_storage import GcsBucket


@task(
    retries=3,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception

    print("chagned")

    df = pd.read_csv(dataset_url)
    return df


@task()
def clean(df: pd.DataFrame, color: str) -> pd.DataFrame:
    if color == "yellow":
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    else:
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])

    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    path = Path(f"{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")

    return path


@task()
def write_gcs(path: Path) -> None:
    gcs_block = GcsBucket.load("prefect-gcs-dtc")
    assert isinstance(gcs_block, GcsBucket)

    gcs_block.upload_from_path(path)

    return


@flow(log_prints=True)
def etl_web_to_gcs(color, year, month) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = (
        "https://github.com/DataTalksClub/nyc-tlc-data/"
        f"releases/download/{color}/{dataset_file}.csv.gz"
    )
    print(dataset_url)

    df = fetch(dataset_url)
    df = clean(df, color)
    path = write_local(df, color, dataset_file)
    write_gcs(path)  # type: ignore
    path.unlink()  # type: ignore


@flow(log_prints=True)
def multi_etl_web_to_gcs(colors, years, months, test=1):
    for color in colors:
        for year in years:
            for month in months:
                etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    colors = ["yellow"]
    months = [2, 3]
    years = [2021]

    multi_etl_web_to_gcs(colors, years, months)
