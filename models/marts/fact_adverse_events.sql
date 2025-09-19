{{ config(materialized='table') }}

with s as (
  select * from {{ ref('stg_maude') }}
),
d as (
  -- na start bezpośrednio DIM z PUBLIC
  select raw_name, canonical_name, min(manufacturer_id) as manufacturer_id
  from MEDTECH.PUBLIC.DIM_MANUFACTURER
  group by 1,2
)

select
  to_varchar(
    sha2(
      coalesce(s.report_number,'') || '|' ||
      coalesce(s.mdr_report_key,'') || '|' ||
      coalesce(to_varchar(s.date_received),''),
      256
    )
  ) as event_id,

  s.report_number, s.mdr_report_key,
  d.manufacturer_id,
  d.canonical_name as canonical_mfr,

  s.product_code, s.device_name, s.brand_name,
  s.event_type, s.product_problem,
  s.event_date, s.date_received,
  to_char(s.date_received, 'YYYY-"Q"Q') as year_quarter,

  s.narrative_text,
  length(s.narrative_text) as narrative_len,

  case
    when s.narrative_text ilike '%LEAK%'       then 'LEAK'
    when s.narrative_text ilike '%FRACTURE%'   then 'FRACTURE'
    when s.narrative_text ilike '%BREAK%'      then 'FRACTURE/BREAK'
    when s.narrative_text ilike '%THROMB%'     then 'THROMBUS/CLOT'
    when s.narrative_text ilike '%INFECTION%'  then 'INFECTION'
    when s.narrative_text ilike '%DISLODG%'    then 'DISLODGEMENT'
    when s.narrative_text ilike '%MIGRAT%'     then 'MIGRATION'
    else null
  end as failure_mode
from s
left join d on d.raw_name = s.manufacturer_name
