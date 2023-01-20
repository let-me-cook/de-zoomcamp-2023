select count(*) from yellow_taxi_data
where lpep_pickup_datetime::date = '2019-01-15' and lpep_dropoff_datetime::date = '2019-01-15'