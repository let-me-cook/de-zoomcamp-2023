# docker build -t taxi_ingest:v001 .

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
        --user=root \
        --password=root \
        --host=pg-database \
        --port=5432 \
        --db-name=ny_taxi \
        --table-name=green_taxi_trips \
        --url=${URL}