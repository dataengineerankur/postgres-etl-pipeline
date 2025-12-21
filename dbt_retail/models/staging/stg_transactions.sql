{#-
  Scenario-driven model:
  - scenario=model_bug: intentionally references a wrong column to simulate a real model regression.
  - scenario=source_bug: wrong source/table reference (compile-time error).
  - scenario=syntax_bug: introduce a SQL syntax mistake (compile-time or run-time parse error).
  - scenario=logic_bug: runtime math error / unsafe logic that should be fixed in SQL.
  - otherwise: normal logic.
-#}

{%- set sc = var('scenario','ok') -%}

with src as (
  select *
  from
  {%- if sc == 'source_bug' -%}
    {{ source('retail_src', 'transactions_src_typo') }}
  {%- else -%}
    {{ source('retail_src', 'transactions_src') }}
  {%- endif -%}
),

typed as (
  select
    cast(transaction_id as text) as transaction_id,
    cast(store_id as integer) as store_id,
    cast(sku as text) as sku,
    -- model_bug scenario: intentional cast error to simulate a regression
    {% if sc == 'model_bug' %}
    cast('invalid' as integer) as amount_cents,
    {% elif sc == 'logic_bug' %}
    -- logic_bug: unsafe division by zero (should be guarded with nullif or similar)
    cast(amount_cents as integer) / 0 as amount_cents,
    {% elif sc == 'syntax_bug' %}
    -- syntax_bug: missing comma below is intentional (SQL syntax error)
    cast(amount_cents as integer) as amount_cents
    {% else %}
    -- bad_data scenario inserts strings; casting can fail (real-world data issue)
    cast(amount_cents as integer) as amount_cents,
    {% endif %}
    cast(quantity as integer) as quantity,
    cast(transaction_ts as timestamp) as transaction_ts
  from src
)

select *
from typed
