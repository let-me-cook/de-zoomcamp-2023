-- What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?
select count(*) from de-dtc-375915.production.fact_trips

-- What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?
select 
  service_type, 
  count(service_type),
  count(*) * 100.0 / sum(count(*)) over () as ratio 
from de-dtc-375915.production.fact_trips group by service_type

-- What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?
select count(*) from de-dtc-375915.production.stg_fhv_tripdata

-- What is the count of records in the model fact_fhv_trips after running all models with the test run variable disabled (:false)?
select count(*) from de-dtc-375915.production.fact_fhv_trips

-- What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?
select date_trunc(pickup_datetime, month), count(1) from de-dtc-375915.production.fact_fhv_trips group by 1 order by 2 desc
