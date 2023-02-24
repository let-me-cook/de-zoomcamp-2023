import pandas as pd
from prefect import flow, task
from pathlib import Path


# @task(log_prints=True)
def get_generator(path: str):
    df_generator = pd.read_csv(path, iterator=True, chunksize=10000)

    return df_generator


@flow()
def main_flow():
    csv_path = Path() / "yellow_tripdata_2019-02.csv"

    df_gen = get_generator(csv_path)
    df = next(df_gen)
    print(df.head(5))


if __name__ == "__main__":
    main_flow()
