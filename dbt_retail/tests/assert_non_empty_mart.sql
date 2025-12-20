-- Fails if mart_daily_sales is empty (simulates real contract expectation: downstream depends on rows).
select 1
where not exists (
  select 1 from {{ ref('mart_daily_sales') }} limit 1
)


