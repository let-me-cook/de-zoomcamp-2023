select lpep_pickup_datetime::date, max(trip_distance) from yellow_taxi_data
group by lpep_pickup_datetime::date
order by max(trip_distance) desc