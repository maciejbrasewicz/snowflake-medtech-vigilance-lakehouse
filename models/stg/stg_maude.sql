-- models/stg/stg_maude.sql
{{ config(materialized='view') }}

with base as (
  select
    raw,
    /* klucze i podstawowe pola */
    raw:"mdr_report_key"::string             as mdr_report_key,
    raw:"report_number"::string              as report_number,

    /* daty (UDF istnieje w MEDTECH.PUBLIC – fallback niepotrzebny) */
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"date_received"::string) as date_received,
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"event_date"::string)     as event_date,

    /* event_type w oryginale + normalizacja do kanonicznych wartości */
    raw:"event_type"::string                 as event_type_raw,
    case
      when upper(trim(raw:"event_type"::string)) in ('INJURY','MALFUNCTION','DEATH')
        then initcap(trim(raw:"event_type"::string))                                   -- Injury/Malfunction/Death
      when raw:"event_type"::string is null or trim(raw:"event_type"::string) = '' 
           or upper(trim(raw:"event_type"::string)) in ('N/A','NA','UNKNOWN','NOT AVAILABLE')
        then 'No Answer Provided'
      else 'Other'
    end                                         as event_type,

    /* pozostałe atrybuty */
    raw:"product_problem"::string            as product_problem,
    raw:"device_report_product_code"::string as product_code,
    raw:"device_name"::string                as device_name,
    raw:"brand_name"::string                 as brand_name,
    raw:"manufacturer_d_name"::string        as manufacturer_name,
    raw:"manufacturer_g1_name"::string       as manufacturer_g1_name,

    /* metadane ładowania */
    src_filename,
    load_ts
  from {{ source('medtech','MAUDE_RAW') }}
),

flat as (
  /* OUTER => TRUE żeby NIE gubić rekordów bez mdr_text (zostanie jedna linia z NULL-em) */
  select
    b.*,
    f.index,
    nullif(trim(f.value:"text"::string), '') as text_piece
  from base b,
  lateral flatten(input => b.raw:"mdr_text", OUTER => TRUE) f
)

select
  /* klucze */
  mdr_report_key,
  report_number,

  /* daty */
  date_received,
  event_date,

  /* typ zdarzenia (znormalizowany) i oryginał (dla ewentualnych analiz) */
  event_type,
  event_type_raw,

  /* reszta pól */
  product_problem,
  product_code,
  device_name,
  brand_name,
  manufacturer_name,
  manufacturer_g1_name,

  /* scalona narracja; listagg ignoruje NULL-e */
  listagg(text_piece, '\n') within group (order by index) as narrative_text,

  /* metadane */
  src_filename,
  load_ts,

  /* pomocnicza metryka do testów/warunków */
  length(listagg(text_piece, '\n') within group (order by index)) as narrative_len
from flat
group by
  mdr_report_key, report_number,
  date_received, event_date,
  event_type, event_type_raw,
  product_problem, product_code, device_name, brand_name,
  manufacturer_name, manufacturer_g1_name,
  src_filename, load_ts
