import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage
from pathlib import Path

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

services = ["fhv", "green", "yellow"]
init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "prefect-dtc")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(service: str, year: int, m: int):
    month = "0" + str(m + 1)
    month = month[-2:]

    file_name = f"{service}_tripdata_{year}-{month}"
    l_csv_file = Path("data") / f"{file_name}.csv.gz"
    l_par_file = Path("data") / f"{file_name}.parquet"
    gcs_file = Path(service) / f"{file_name}.parquet"

    request_url = f"{init_url}{service}/{file_name}.csv.gz"
    os.system(f"wget -nc {request_url} -O {l_csv_file}")

    if service == "yellow":
        transform_yellow(pd.read_csv(l_csv_file)).to_parquet(l_par_file, engine="pyarrow")
    if service == "green":
        transform_green(pd.read_csv(l_csv_file)).to_parquet(l_par_file, engine="pyarrow")

    # pd.read_csv(l_csv_file).to_parquet(l_par_file, engine="pyarrow")
    print(f"Local csv file: {l_csv_file}")
    print(f"Local parquet file: {l_par_file}")

    upload_to_gcs(BUCKET, str(gcs_file), str(l_par_file))
    print(f"GCS: {gcs_file}")

    # l_par_file.unlink()
    # l_csv_file.unlink()


def transform_green(df_green: pd.DataFrame):
    return df_green.astype(
        {
            "VendorID": "Int64",
            "lpep_pickup_datetime": "string",
            "lpep_dropoff_datetime": "string",
            "store_and_fwd_flag": "string",
            "RatecodeID": "Int64",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "passenger_count": "Int64",
            "trip_distance": "Float64",
            "fare_amount": "Float64",
            "extra": "Float64",
            "mta_tax": "Float64",
            "tip_amount": "Float64",
            "tolls_amount": "Float64",
            "ehail_fee": "Float64",
            "improvement_surcharge": "Float64",
            "total_amount": "Float64",
            "payment_type": "Float64",
            "trip_type": "Float64",
            "congestion_surcharge": "Float64",
        }
    )


def transform_yellow(df_yellow: pd.DataFrame):
    return df_yellow.astype(
        {
            "VendorID": "Int64",
            "tpep_pickup_datetime": "string",
            "tpep_dropoff_datetime": "string",
            "passenger_count": "Int64",
            "trip_distance": "Float64",
            "RatecodeID": "Int64",
            "store_and_fwd_flag": "string",
            "PULocationID": "Int64",
            "DOLocationID": "Int64",
            "payment_type": "Int64",
            "fare_amount": "Float64",
            "extra": "Float64",
            "mta_tax": "Float64",
            "tip_amount": "Float64",
            "tolls_amount": "Float64",
            "improvement_surcharge": "Float64",
            "total_amount": "Float64",
            "congestion_surcharge": "Float64",
        }
    )


for service in ["yellow", "green"]:
    for year in [2019, 2020]:
        for m in range(12):
            try:
                web_to_gcs(service, year, m)
            except UnicodeDecodeError:
                print("Broken csv.gz")
                continue
            except pd.errors.EmptyDataError:
                print("Broken csv.gz")
                continue
