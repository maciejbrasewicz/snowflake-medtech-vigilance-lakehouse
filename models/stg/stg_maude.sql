{{ config(materialized='view') }}

with base as (
  select
    raw,
    raw:"mdr_report_key"::string             as mdr_report_key,
    raw:"report_number"::string              as report_number,
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"date_received"::string) as date_received,
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"event_date"::string)     as event_date,
    raw:"event_type"::string                 as event_type,
    raw:"product_problem"::string            as product_problem,
    raw:"device_report_product_code"::string as product_code,
    raw:"device_name"::string                as device_name,
    raw:"brand_name"::string                 as brand_name,
    raw:"manufacturer_d_name"::string        as manufacturer_name,
    raw:"manufacturer_g1_name"::string       as manufacturer_g1_name,
    src_filename,
    load_ts
  from {{ source('medtech','MAUDE_RAW') }}
),

flat as (
  select
    b.*,
    f.index,
    nullif(trim(f.value:"text"::string), '') as text_piece
  from base b,
  lateral flatten(input => b.raw:"mdr_text") f
)

select
  mdr_report_key,
  report_number,
  date_received,
  event_date,
  event_type,
  product_problem,
  product_code,
  device_name,
  brand_name,
  manufacturer_name,
  manufacturer_g1_name,
  listagg(text_piece, '\n') within group (order by index) as narrative_text,
  src_filename,
  load_ts
from flat
group by
  mdr_report_key, report_number, date_received, event_date, event_type,
  product_problem, product_code, device_name, brand_name,
  manufacturer_name, manufacturer_g1_name, src_filename, load_ts
