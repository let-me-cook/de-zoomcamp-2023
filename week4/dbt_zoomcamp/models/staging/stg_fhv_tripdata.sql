{{ config(materialized="view") }}

select
    -- identifiers
    {{ dbt_utils.surrogate_key(["dispatching_base_num", "pickup_datetime", "PUlocationID", "DOlocationID"]) }} as tripid,
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(Affiliated_base_number as string) as Affiliated_base_number,
    cast(PUlocationID as integer) as PUlocationID,
    cast(DOlocationID as integer) as DOlocationID,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    SR_Flag,
from {{ source("staging", "fhv_tripdata") }}
{% if var("is_test_run", default=true) %} limit 100 {% endif %}
