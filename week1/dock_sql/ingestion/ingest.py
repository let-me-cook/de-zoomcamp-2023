import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
from time import time
import os
import typer


def pipeline(
    user: str = typer.Option("root"),
    password: str = typer.Option("root"),
    host: str = typer.Option("localhost"),
    port: str = typer.Option("5432"),
    db_name: str = typer.Option("ny_taxi"),
    table_name: str = typer.Option(""),
    url: str = typer.Option(""),
):
    csv_buffer_path = Path() / "buffer.csv"
    gz_buffer_path = Path() / "buffer.csv.gz"

    os.system(f"wget {url} -O {gz_buffer_path}")
    os.system(f"gunzip -f {gz_buffer_path} {csv_buffer_path}")

    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    df_iterator = pd.read_csv(
        csv_buffer_path, iterator=True, chunksize=100000, low_memory=False
    )
    df = next(df_iterator)

    # Ensure type
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)

    # Create table if doesnt exist, delete and replace the old one if exist
    df.head(0).to_sql(name=table_name, con=engine, if_exists="replace")

    # Insert the rest of the data
    df.to_sql(name=table_name, con=engine, if_exists="append")

    print("Starting insert loop")

    for cur_df in df_iterator:
        t_start = time()

        # Ensure column type
        cur_df.lpep_dropoff_datetime = pd.to_datetime(cur_df.lpep_dropoff_datetime)
        cur_df.lpep_pickup_datetime = pd.to_datetime(cur_df.lpep_pickup_datetime)

        # Insert the current iteration of batch
        cur_df.to_sql(name=table_name, con=engine, if_exists="append")

        t_end = time()

        print(f"Batch inserted in {(t_end - t_start):.2f} seconds")

    print("Ingestion is done, deleting buffer files...")

    csv_buffer_path.unlink()

if __name__ == "__main__":
    typer.run(pipeline)
