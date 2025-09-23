{{ config(materialized='table') }}

WITH s AS (
  -- surowe zdarzenia po stagingu
  SELECT
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
    narrative_text,
    LENGTH(narrative_text) AS narrative_len
  FROM {{ ref('stg_maude') }}
),

-- mapa producentów z seeda (kanonizacja nazw)
d AS (
  SELECT
    UPPER(TRIM(raw_name)) AS raw_name_u,
    canonical_name,
    manufacturer_id
  FROM {{ ref('manufacturer_map') }}     -- <-- jeśli seed masz jako "manufacturer", zamień na ref('manufacturer')
),

-- wybór "surowej" nazwy producenta do złączenia (główna, potem g1, potem brand)
s_with_raw AS (
  SELECT
    s.*,
    UPPER(TRIM(
      COALESCE(s.manufacturer_name, s.manufacturer_g1_name, s.brand_name)
    )) AS manufacturer_name_u
  FROM s
)

SELECT
  -- stabilny klucz zdarzenia (hash kluczowych pól)
  TO_VARCHAR(
    SHA2(
      COALESCE(report_number, '') || '|' ||
      COALESCE(mdr_report_key, '') || '|' ||
      COALESCE(TO_VARCHAR(date_received), ''),
      256
    )
  ) AS event_id,

  -- identyfikatory źródła
  report_number,
  mdr_report_key,

  -- producent po kanonizacji (z seeda) + id (może być NULL, jeśli brak w mapie)
  d.manufacturer_id,
  COALESCE(d.canonical_name, manufacturer_name) AS canonical_mfr,

  -- atrybuty produktu / zdarzenia
  product_code,
  device_name,
  brand_name,
  event_type,
  product_problem,

  -- daty i etykiety czasu
  event_date,
  date_received,
  TO_CHAR(date_received, 'YYYY-"Q"Q') AS year_quarter,

  -- narracja
  narrative_text,
  narrative_len,

  -- prosta heurystyka failure_mode (do czasu AISQL)
  CASE
    WHEN narrative_text ILIKE '%LEAK%'       THEN 'LEAK'
    WHEN narrative_text ILIKE '%FRACTURE%'   THEN 'FRACTURE'
    WHEN narrative_text ILIKE '%BREAK%'      THEN 'FRACTURE/BREAK'
    WHEN narrative_text ILIKE '%THROMB%'     THEN 'THROMBUS/CLOT'
    WHEN narrative_text ILIKE '%INFECTION%'  THEN 'INFECTION'
    WHEN narrative_text ILIKE '%DISLODG%'    THEN 'DISLODGEMENT'
    WHEN narrative_text ILIKE '%MIGRAT%'     THEN 'MIGRATION'
    ELSE NULL
  END AS failure_mode

FROM s_with_raw s
LEFT JOIN d
  ON d.raw_name_u = s.manufacturer_name_u;
