from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def get_file_from_gcs(color: str, year: int, month: int) -> Path:
    file = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = Path() / file
    local_path = Path() / file

    gcs_block = GcsBucket.load("prefect-gcs-dtc")
    assert isinstance(gcs_block, GcsBucket)

    gcs_block.download_object_to_path(from_path=gcs_path, to_path=local_path)

    return local_path


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(f"pre: total len: {len(df)}")

    df["passenger_count"].fillna(0, inplace=True)

    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    print(f"post: total len: {len(df)}")
    return df


@task()
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("prefect-creds-dtc")
    assert isinstance(gcp_credentials_block, GcpCredentials)

    df.to_gbq(
        destination_table="prefect_dataset_dtc.ny_taxi",
        project_id="de-dtc-375915",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    years = [2019]
    months = [2, 3]

    for year in years:
        for month in months:
            path = get_file_from_gcs(color, year, month)
            df = transform(path)
            write_to_bq(df)

            try:
                path.unlink()
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    etl_gcs_to_bq()
