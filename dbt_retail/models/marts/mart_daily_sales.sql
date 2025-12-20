with base as (
  select *
  from {{ ref('stg_transactions') }}
),

daily as (
  select
    date_trunc('day', transaction_ts)::date as day,
    store_id,
    count(*) as txns,
    sum(amount_cents) as gross_amount_cents,
    sum(quantity) as units
  from base
  group by 1,2
)

select *
from daily


