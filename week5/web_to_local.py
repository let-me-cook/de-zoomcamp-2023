import os
import pandas as pd
from pathlib import Path

services = ["fhv", "green", "yellow"]
init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"


def web_to_local(service: str, year: int, m: int):
    month = "0" + str(m + 1)
    month = month[-2:]

    file_name = f"{service}_tripdata_{year}-{month}"
    l_csv_folder = Path("data/raw") / f"{service}/{year}/{month}/"
    l_csv_folder.mkdir(parents=True, exist_ok=True)
    l_csv_file = l_csv_folder / f"{file_name}.csv.gz"

    request_url = f"{init_url}{service}/{file_name}.csv.gz"
    os.system(f"wget -nc {request_url} -O {l_csv_file}")

    print(f"Local csv file: {l_csv_file}")


for service in ["yellow", "green"]:
    for year in [2020, 2021]:
        for m in range(12):
            try:
                web_to_local(service, year, m)
            except UnicodeDecodeError:
                print("Broken csv.gz")
                continue
            except pd.errors.EmptyDataError:
                print("Broken csv.gz")
                continue
