docker network create pg-network

docker run -d \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v /home/user/de-zoomcamp-2023/common/postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --name=pg-database \
    --network=pg-network \
    postgres:13

# docker run -d \
#     -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
#     -e PGADMIN_DEFAULT_PASSWORD="root" \
#     -p 8080:80 \
#     --network=pg-network \
#     --name=pg-admin \
#     dpage/pgadmin4