-- models/marts/v_ae_early_signals.sql
{{ config(materialized='view') }}

with seq as (
  select
    canonical_mfr,
    failure_mode,
    year_quarter,
    quarter_index,
    n_events,
    row_number() over (
      partition by canonical_mfr, failure_mode
      order by quarter_index
    ) as rn
  from {{ ref('ae_counts_q') }}
),

roll as (
  select
    s.*,
    /* średnia i odchylenie z 4 poprzednich kwartałów (bez bieżącego) */
    avg(n_events) over (
      partition by canonical_mfr, failure_mode
      order by rn
      rows between 4 preceding and 1 preceding
    ) as mean_4,
    stddev_samp(n_events) over (
      partition by canonical_mfr, failure_mode
      order by rn
      rows between 4 preceding and 1 preceding
    ) as std_4
  from seq s
)

select
  canonical_mfr,
  failure_mode,
  year_quarter,
  quarter_index,
  n_events,
  mean_4,
  std_4,
  case
    when std_4 is null or std_4 = 0 then null
    else (n_events - mean_4) / std_4
  end as z_score
from roll
