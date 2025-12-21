{{ config(materialized='view') }}

with src as (
    select *
    from {{ source('retail', 'transactions') }}
)

select
    transaction_id,
    -- Fixed column name: use the correct "amount_cents" column from the source table.
    cast(amount_cents as integer) as amount_cents,
    -- Add other columns as needed; they are passed through unchanged.
    src.*
from src;
