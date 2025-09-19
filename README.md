# MedTech Vigilance & Compliance Lakehouse

Snowflake-first pipeline do triage’u zdarzeń niepożądanych (MAUDE/FAERS) z wyszukiwaniem podobnych przypadków (Cortex Search: BM25 + wektory), AISQL (klasyfikacja/severity/summary) oraz publikacją Apache Iceberg przez Open Catalog (Trino/DuckDB). **Bez SQL i bez sekcji konfiguracji** (na ten moment).

Projekt buduje w Snowflake end-to-end lakehouse do triage’u zdarzeń niepożądanych z MAUDE/FAERS: automatycznie pobiera i normalizuje dane, wzbogaca je o klasyfikację tematu/severity i streszczenia (AISQL), wyszukuje podobne przypadki hybrydowo (BM25 + wektory/Cortex Search), generuje „wczesne sygnały” wzrostów ryzyka, publikuje wzbogacone tabele w formacie Iceberg przez Open Catalog (do użycia w Trino/DuckDB), a wszystko w reżimie governance i kontroli kosztów.

---

## Spis treści
1. [Stack](#stack)
2. [Wymagania](#wymagania)
3. [Założenia danych](#założenia-danych)
4. [Architektura](#architektura)
5. [Ingest (S3 → Snowflake)](#ingest-s3--snowflake)
7. [Modelowanie (dbt)](#modelowanie-dbt)
8. [AI Enrichment (AISQL, Embeddings, Cortex Search)](#ai-enrichment-aisql-embeddings-cortex-search)
9. [Open Catalog (Iceberg)](#open-catalog-iceberg)
10. [Governance & FinOps](#governance--finops)
11. [SLO / Metryki](#slo--metryki)
12. [Troubleshooting](#troubleshooting)
13. [Licencja](#licencja)

---

## Stack
- **Snowflake (AWS eu-central-1)**: Warehouse `MEDTECH_XS` (auto-suspend 60s), Database `MEDTECH`, Schema `PUBLIC`
- **AWS S3**: pliki MAUDE/FAERS jako **NDJSON** (1 JSON/linia; opcjonalnie gzip)
- **dbt**: dbt Cloud (Developer) lub dbt Core
- **Snowflake Cortex**: AISQL, Embeddings, **Cortex Search**
- **Apache Iceberg + Open Catalog (REST)**: klienci **Trino** / **DuckDB**
- **FinOps**: Resource Monitor, `QUERY_TAG`, XS warehouse

---

## Wymagania
- Konto Snowflake z uprawnieniami DDL/DML
- Dostęp „read” do S3 z danymi (prefiks NDJSON)
- Storage Integration do S3 (bez sekretów w repo)
- Projekt dbt z połączeniem do Snowflake (env vars / profiles.yml)
- Brak sekretów (ARN/keys/hasła) w repozytorium

---

## Założenia danych
- Horyzont: **5–7 lat**
- Zakres: **2–5 rodzin urządzeń** (MAUDE; FAERS opcjonalnie)
- Wielkość: **~1M rekordów** (ekonomiczny trial)
- Format ingestu: **NDJSON** (eliminuje limit dużych dokumentów JSON)

---

## Architektura

### Warstwy i komponenty
- **Źródła danych**: OpenFDA (MAUDE/FAERS) eksportowane do S3 jako NDJSON (1 rekord = 1 linia).
- **Ingest**: S3 → Snowflake External Stage → COPY do tabeli RAW (VARIANT) w `MEDTECH.PUBLIC`.
- **Modelowanie**: dbt (`raw → stg → marts`) z testami i kontraktami.
- **AI/IR**:
  - AISQL (topic, severity, summary).
  - Embeddings + k-NN (similar cases).
  - Cortex Search (hybryda BM25 + wektory + filtry).
- **Udostępnianie**: Apache Iceberg + Open Catalog (REST) dla Trino/DuckDB.
- **Governance/FinOps**: Masking/Row Access, Resource Monitor, `QUERY_TAG`, raporty z ACCOUNT_USAGE.

### Przepływ danych (end-to-end)
1. **OpenFDA → S3**: Pobranie paczek, konwersja do NDJSON, umieszczenie w prefiksie S3.
2. **S3 → Snowflake RAW**: External Stage wskazuje na prefiks; COPY ładuje rekordy do tabeli `MAUDE_RAW`.
3. **RAW → STG (dbt)**: Rozpakowanie pól VARIANT, parsowanie dat, normalizacja producentów i modeli, agregacja `narrative_text`.
4. **STG → MARTS (dbt)**: Budowa faktów/wymiarów oraz tabeli wzbogaconej pod AI/IR (`adverse_events_enriched`).
5. **AI Enrichment**:
   - AISQL generuje `topic`, `severity`, `summary`.
   - Funkcje embeddingów zapisują wektory; k-NN zwraca podobne przypadki.
6. **Cortex Search**: Indeks na tekście (np. `narrative_text`) + filtry (`product_code`, `manufacturer`, daty) do zapytań UC1.
7. **Publikacja Iceberg**: Materializacja `marts.adverse_events_enriched` jako tabela Iceberg.
8. **Open Catalog (REST)**: Rejestracja tabeli Iceberg; odczyt z Trino i DuckDB bez kopiowania danych.
9. **Observability/FinOps**: Tagowanie zapytań (`QUERY_TAG`), monitor kredytów, raporty kosztów i profilowanie zapytań.

### Decyzje projektowe
- **NDJSON** eliminuje limit wielkości dokumentu JSON i upraszcza COPY.
- **COPY → internal tables** zamiast external table dla stabilności i prostszej ewolucji schematu.
- **Cortex Search** jako główny silnik podobieństwa (BM25+wektory) z filtracją po meta-polach.
- **Iceberg + Open Catalog** zapewnia interoperacyjność (Snowflake ↔ Trino/DuckDB) bez lock-inu.
- **Batchowanie AI** (10–20k rekordów) + większy `target_lag` w Search dla kontroli kosztów.

### SLO (związane z architekturą)
- **Recall@20** dla „similar cases” ≥ 0.7 na próbce walidacyjnej.
- **P95 latencja** zapytania „similar cases” < 1.5 s przy ~200k rekordów.
- **Budżet**: ≤ 150–170 kredytów (XS warehouse, auto-suspend, tagowanie zadań).

### Artefakty wynikowe
- Tabela `marts.adverse_events_enriched` (kolumny merytoryczne + AI + wektory).
- Usługa Cortex Search gotowa do zapytań UC1.
- Tabela Iceberg zarejestrowana w Open Catalog i dostępna w Trino/DuckDB.
- Raport kosztów i profilowanie kluczowych zapytań.


---

## Ingest (S3 → Snowflake)
- Pliki **NDJSON** w S3 (1 rekord = 1 linia).
- Stage w Snowflake wskazuje na prefiks S3; file format: JSON (outer array = false, multi-line = false).
- Ładowanie **COPY** do tabeli `MAUDE_RAW(raw VARIANT, src_filename STRING, load_ts TIMESTAMP_NTZ)`.

---

## Modelowanie (dbt)
- **raw**: projekcje na `MAUDE_RAW`.
- **stg**: rozpakowanie pól, `PARSE_YYYYMMDD`, normalizacja producentów/modeli, budowa `narrative_text` z `mdr_text[]`.
- **marts**: fakty/wymiary + kolumny pod AI (`topic`, `severity`, `summary`, `embedding`).
- Testy/kontrakty: unikalność `event_id`, niepustość kluczowych pól, relacje do słowników (seeds).

---

## AI Enrichment (AISQL, Embeddings, Cortex Search)
- **AISQL**: batch 10–20k; pola: `topic`, `severity`, `summary`; stały `QUERY_TAG` do rozliczeń.
- **Embeddings**: kolumna `embedding`; k-NN (top-K podobieństwa) na enriched tekście.
- **Cortex Search**: hybryda BM25 + wektory; filtry po `product_code`, `manufacturer`, dacie; `target_lag` dobrany pod koszt/świeżość.

---

## Open Catalog (Iceberg)
- Materializacja `marts.adverse_events_enriched` jako **Iceberg** (managed/external).
- Rejestracja w **Open Catalog (REST)**.
- Weryfikacja odczytu: **Trino** (REST Catalog properties) i **DuckDB** (Iceberg REST).

---

## Governance & FinOps
- **Resource Monitor**: twardy limit kredytów (≤ 150–170 na trial).
- **Warehouse**: XS + `AUTO_SUSPEND=60s`.
- **Tagowanie**: `QUERY_TAG` per job (`ingest`, `dbt`, `aisql`, `search`).
- **Polityki**: `MASKING POLICY`, `ROW ACCESS POLICY` (np. per producent/region).
- **Koszty**: raporty z `ACCOUNT_USAGE`/`INFORMATION_SCHEMA` (wg warehouse/tag).

---

## SLO / Metryki
- **Recall@20** (eval set ręczny): ≥ 0.7.
- **Latencja UC1** (Cortex Search/kNN @ ~200k): < 1.5 s.
- **Koszt trial**: ≤ 150–170 kredytów.
- **Interoperacyjność**: ten sam Iceberg widoczny min. w 2 silnikach (Snowflake + Trino/DuckDB).

---

## Troubleshooting
- „JSON document too large” → użyj **NDJSON** (1 rekord/linia).
- External Table = 0 wierszy → w tym projekcie preferuj **COPY → internal table**.
- „Unsupported subquery in view” → zamień scalar subquery na `JOIN LATERAL + GROUP BY`.
- Wysokie koszty Search → zwiększ `target_lag`, batchuj indeksacje, taguj zapytania.

---

## Licencja
MIT
