select passenger_count, count(*)
from yellow_taxi_data ytd 
where lpep_pickup_datetime::date = '2019-01-01'
group by passenger_count 
order by count(*) desc