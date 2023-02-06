select "PULocationID", zdp."Zone", "DOLocationID", zdd."Zone", tip_amount from yellow_taxi_data ytd 
left join zone_data zdp on "PULocationID" = zdp."LocationID"
left join zone_data zdd on "DOLocationID" = zdd."LocationID"
where zdp."Zone" = 'Astoria'
order by ytd.tip_amount DESC