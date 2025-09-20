{{ config(materialized='view') }}

WITH base AS (
  SELECT
    raw,
    raw:"mdr_report_key"::string  AS mdr_report_key,
    raw:"report_number"::string   AS report_number,
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"date_received"::string) AS date_received,
    MEDTECH.PUBLIC.PARSE_YYYYMMDD(raw:"event_date"::string)     AS event_date,
    raw:"event_type"::string AS event_type_raw,
    CASE
      WHEN UPPER(TRIM(raw:"event_type"::string)) IN ('INJURY','MALFUNCTION','DEATH')
        THEN INITCAP(TRIM(raw:"event_type"::string))
      WHEN raw:"event_type"::string IS NULL OR TRIM(raw:"event_type"::string) = ''
        OR UPPER(TRIM(raw:"event_type"::string)) IN ('N/A','NA','UNKNOWN','NOT AVAILABLE')
        THEN 'No Answer Provided'
      ELSE 'Other'
    END AS event_type,
    raw:"product_problem"::string            AS product_problem,
    raw:"device_report_product_code"::string AS product_code,
    /* top-level (często puste) */
    raw:"device_name"::string          AS device_name_top,
    raw:"brand_name"::string           AS brand_name_top,
    raw:"manufacturer_d_name"::string  AS manufacturer_name_top,
    raw:"manufacturer_g1_name"::string AS manufacturer_g1_name_top,
    src_filename,
    load_ts
  FROM {{ source('medtech','MAUDE_RAW') }}
),

/* >>> producenci/brand z device[] */
devices_agg AS (
  SELECT
    b.mdr_report_key,
    MAX(IFF(NULLIF(TRIM(d.value:"manufacturer_d_name"::string), '') IS NOT NULL,
            d.value:"manufacturer_d_name"::string, NULL))  AS manufacturer_name_dev,
    MAX(IFF(NULLIF(TRIM(d.value:"manufacturer_g1_name"::string), '') IS NOT NULL,
            d.value:"manufacturer_g1_name"::string, NULL)) AS manufacturer_g1_name_dev,
    MAX(IFF(NULLIF(TRIM(d.value:"brand_name"::string), '') IS NOT NULL,
            d.value:"brand_name"::string, NULL))           AS brand_name_dev,
    MAX(IFF(NULLIF(TRIM(d.value:"device_name"::string), '') IS NOT NULL,
            d.value:"device_name"::string, NULL))          AS device_name_dev
  FROM base b,
  LATERAL FLATTEN(input => b.raw:"device", OUTER => TRUE) d
  GROUP BY 1
),

/* narracja z mdr_text (OUTER => zachowaj też puste) */
text_flat AS (
  SELECT
    b.*,
    f.index,
    NULLIF(TRIM(f.value:"text"::string), '') AS text_piece
  FROM base b,
  LATERAL FLATTEN(input => b.raw:"mdr_text", OUTER => TRUE) f
)

/* final: COALESCE(top-level, device[]), scalona narracja */
SELECT
  t.mdr_report_key,
  t.report_number,
  t.date_received,
  t.event_date,
  t.event_type,
  t.event_type_raw,
  t.product_problem,
  t.product_code,
  COALESCE(t.device_name_top,  d.device_name_dev)   AS device_name,
  COALESCE(t.brand_name_top,   d.brand_name_dev)    AS brand_name,
  COALESCE(t.manufacturer_name_top,    d.manufacturer_name_dev)    AS manufacturer_name,
  COALESCE(t.manufacturer_g1_name_top, d.manufacturer_g1_name_dev) AS manufacturer_g1_name,
  LISTAGG(t.text_piece, '\n') WITHIN GROUP (ORDER BY t.index) AS narrative_text,
  t.src_filename,
  t.load_ts,
  LENGTH(LISTAGG(t.text_piece, '\n') WITHIN GROUP (ORDER BY t.index)) AS narrative_len
FROM text_flat t
LEFT JOIN devices_agg d USING (mdr_report_key)
GROUP BY
  t.mdr_report_key, t.report_number,
  t.date_received, t.event_date,
  t.event_type, t.event_type_raw,
  t.product_problem, t.product_code,
  t.device_name_top,  d.device_name_dev,
  t.brand_name_top,   d.brand_name_dev,
  t.manufacturer_name_top,    d.manufacturer_name_dev,
  t.manufacturer_g1_name_top, d.manufacturer_g1_name_dev,
  t.src_filename, t.load_ts
