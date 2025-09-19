-- models/marts/ae_counts_q.sql
{{ config(materialized='table') }}

with src as (
  select
    coalesce(canonical_mfr, '(unknown)') as canonical_mfr,
    coalesce(failure_mode,  '(unknown)') as failure_mode,
    date_received
  from {{ ref('fact_adverse_events') }}
  where date_received is not null
)

select
  canonical_mfr,
  failure_mode,
  year(date_received)                                  as yyyy,
  quarter(date_received)                               as q,
  (year(date_received) * 4 + quarter(date_received))   as quarter_index,        -- klucz numeryczny do sortowania
  to_char(date_received, 'YYYY-"Q"Q')                  as year_quarter,          -- etykieta do wyświetlania
  count(*)                                             as n_events
from src
group by 1,2,3,4,5,6
