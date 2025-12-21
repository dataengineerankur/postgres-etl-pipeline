{{ config(materialized='view') }}

select *
from {{ source('src', 'transactions') }}
