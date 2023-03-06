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


def web_to_gcs(year: int, service: str):
    for i in range(12):
        month = "0" + str(i + 1)
        month = month[-2:]

        file_name = f"{service}_tripdata_{year}-{month}"
        l_csv_file = Path("data") / f"{file_name}.csv.gz"
        l_par_file = Path("data") / f"{file_name}.parquet"
        gcs_file = Path(service) / f"{file_name}.parquet"

        request_url = f"{init_url}{service}/{file_name}.csv.gz"
        print(request_url)
        r = requests.get(request_url)
        pd.read_csv(request_url).to_csv(l_csv_file)
        print(f"Local csv file: {l_csv_file}")

        pd.read_csv(l_csv_file).to_parquet(l_par_file, engine="pyarrow")

        upload_to_gcs(BUCKET, str(gcs_file), str(l_par_file))
        print(f"GCS: {gcs_file}")

        # l_par_file.unlink()
        # l_csv_file.unlink()


for service in ["fhv"]:
    for year in [2019]:
        web_to_gcs(year, service)
