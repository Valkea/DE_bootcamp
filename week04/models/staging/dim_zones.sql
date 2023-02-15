{{ config(materialized='table') }}

select
  cast(LocationID as numeric) as locationid,
  Borough as borough,
  Zone as zone,
  replace(service_zone, 'Boro', 'Green') as service_zone -- because the CSV file contains "Boro" and "Yellow" zones...
from {{ ref('taxi_zone_lookup')}}