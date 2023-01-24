URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

python etl/etl.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db-name=ny_taxi \
    --table-name=green_taxi_trips \
    --url=${URL}