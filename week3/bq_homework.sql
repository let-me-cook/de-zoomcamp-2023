CREATE OR REPLACE EXTERNAL TABLE `de-dtc-375915.nytaxi.external_fhv_tripdata`
(
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,	
  PUlocationID INTEGER,
  DOlocationID INTEGER,	
  SR_Flag INTEGER,
  Affiliated_base_number STRING	
)
OPTIONS (
  format = 'parquet',
  uris = ['gs://prefect-dtc/fhv/fhv_tripdata_2019-*.parquet']
);

CREATE OR REPLACE TABLE `de-dtc-375915.nytaxi.fhv_tripdata` 
AS (
  SELECT *
  FROM `de-dtc-375915.nytaxi.external_fhv_tripdata`
)

SELECT COUNT(distinct affiliated_base_number) FROM `de-dtc-375915.nytaxi.external_fhv_tripdata` 

SELECT COUNT(distinct affiliated_base_number) FROM `de-dtc-375915.nytaxi.fhv_tripdata` 


SELECT COUNT(*) FROM `de-dtc-375915.nytaxi.fhv_tripdata` WHERE DOlocationID is null AND PUlocationID is null

CREATE OR REPLACE TABLE `de-dtc-375915.nytaxi.fhv_tripdata_partitioned_clustered` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT *
  FROM `de-dtc-375915.nytaxi.fhv_tripdata`
)

SELECT COUNT(distinct affiliated_base_number) FROM `de-dtc-375915.nytaxi.fhv_tripdata`
WHERE pickup_datetime BETWEEN '2019-03-02' and '2019-03-31'

SELECT COUNT(distinct affiliated_base_number) FROM `de-dtc-375915.nytaxi.fhv_tripdata_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-02' and '2019-03-31'