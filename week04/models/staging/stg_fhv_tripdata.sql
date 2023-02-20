{{ config(materialized='table') }}

--with tripdata as 
--(
--  select *,
--    row_number() over(partition by pickup_datetime) as rn
--  from {{ source('staging','fhv_tripdata') }}
--  -- where vendorid is not null 
--)

select 

  -- identifiers
  Affiliated_base_number,
  cast(PUlocationID as integer) as pickup_locationid,
  cast(DOlocationID as integer) as dropoff_locationid,

  -- timestamps
  cast(pickup_datetime as timestamp) as pickup_datetime,
  cast(dropOff_datetime as timestamp) as dropoff_datetime,

  -- trip infos
  SR_Flag,
  dispatching_base_num,  

from {{ source('staging','fhv_tripdata') }}
-- where Affiliated_base_number is not null
--from tripdata
--where rn = 1

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}
    limit 100
{% endif %}
