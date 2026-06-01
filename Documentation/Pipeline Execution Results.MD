# FHIR Pipeline Execution Report
### Azure Databricks | Medallion Architecture | 3-Day Incremental Ingestion

**Report Generated:** 2026-06-01
**Platform:** Azure Databricks (Runtime 15.4 LTS)
**Catalog:** fhir_databricks | **Database:** fhir_db

---

## 1. Executive Summary

This report documents the successful execution of the FHIR API Data Ingestion & Analytics Pipeline across **3 incremental days (May 27–29, 2026)**. The pipeline ingested data from 4 FHIR resources (Patient, Encounter, Observation, Condition), processed it through a full Medallion Architecture (Raw → Bronze → Silver → Gold), and implemented SCD Type 2 versioning to track data changes over time.

| Metric | Result |
|---|---|
| Total Pipeline Runs | 3 |
| Total Resources Processed | 4 per run (12 total) |
| Total Layers per Run | 3 (Raw, Bronze, Silver) |
| Total API Pages Fetched | 9 per resource (3 days × 3 pages) |
| Total Raw Records Ingested | 60 per resource per day |
| Pipeline Success Rate | 100% |
| SCD Type 2 Changes Detected | Yes (patient, observation) |

---

## 2. Pipeline Architecture

```
FHIR API (hapi.fhir.org/baseR4)
           │
           ▼
┌─────────────────────────────────────────┐
│              RAW LAYER                  │
│  /raw/{resource}/{date}/page_00N.json   │
│  Patient | Encounter | Observation |    │
│  Condition × 3 pages × 3 days          │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│            BRONZE LAYER                 │
│  Delta Tables + Metadata Columns        │
│  extraction_timestamp                   │
│  api_url_or_params                      │
│  ingestion_date                         │
│  source_file                            │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│            SILVER LAYER                 │
│  Cleaned + Deduplicated                 │
│  SCD Type 2 Versioning                  │
│  scd_start_date | scd_end_date          │
│  is_current | scd_version               │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│             GOLD LAYER                  │
│  Reporting Views (always current)       │
│  gold_patient | gold_encounter          │
│  gold_observation | gold_condition      │
└─────────────────────────────────────────┘
```

---

## 3. Pipeline Run Results

### Run 1 — 2026-05-27 | Run ID: c32132bb

| Resource | Layer | Status | Records Loaded | Started | Completed |
|---|---|---|---|---|---|
| patient | raw | SUCCESS | 3 pages | 02:55:47 | 02:55:56 |
| patient | bronze | SUCCESS | 180 | 02:55:47 | 02:56:43 |
| patient | silver | SUCCESS | 64 | 02:55:47 | 02:57:00 |
| encounter | raw | SUCCESS | 3 pages | 02:55:47 | 02:57:10 |
| encounter | bronze | SUCCESS | 180 | 02:55:47 | 02:57:17 |
| encounter | silver | SUCCESS | 60 | 02:55:47 | 02:57:24 |
| observation | raw | SUCCESS | 3 pages | 02:55:47 | 02:57:31 |
| observation | bronze | SUCCESS | 180 | 02:55:47 | 02:57:39 |
| observation | silver | SUCCESS | 61 | 02:55:47 | 02:57:49 |
| condition | raw | SUCCESS | 3 pages | 02:55:47 | 02:57:57 |
| condition | bronze | SUCCESS | 180 | 02:55:47 | 02:58:04 |
| condition | silver | SUCCESS | 60 | 02:55:47 | 02:58:09 |

**Run Duration:** ~2 min 22 sec | **Success Rate:** 12/12 ✅

---

### Run 2 — 2026-05-28 | Run ID: 30793f94

| Resource | Layer | Status | Records Loaded | Started | Completed |
|---|---|---|---|---|---|
| patient | raw | SUCCESS | 3 pages | 02:59:18 | 02:59:23 |
| patient | bronze | SUCCESS | 240 | 02:59:18 | 02:59:29 |
| patient | silver | SUCCESS | 64 | 02:59:18 | 02:59:34 |
| encounter | raw | SUCCESS | 3 pages | 02:59:18 | 02:59:43 |
| encounter | bronze | SUCCESS | 240 | 02:59:18 | 02:59:50 |
| encounter | silver | SUCCESS | 60 | 02:59:18 | 02:59:56 |
| observation | raw | SUCCESS | 3 pages | 02:59:18 | 03:00:02 |
| observation | bronze | SUCCESS | 240 | 02:59:18 | 03:00:16 |
| observation | silver | SUCCESS | 61 | 02:59:18 | 03:00:23 |
| condition | raw | SUCCESS | 3 pages | 02:59:18 | 03:00:30 |
| condition | bronze | SUCCESS | 240 | 02:59:18 | 03:00:38 |
| condition | silver | SUCCESS | 60 | 02:59:18 | 03:00:44 |

**Run Duration:** ~1 min 26 sec | **Success Rate:** 12/12 ✅

---

### Run 3 — 2026-05-29 | Run ID: 14537555

| Resource | Layer | Status | Records Loaded | Started | Completed |
|---|---|---|---|---|---|
| patient | raw | SUCCESS | 3 pages | 03:01:35 | 03:01:40 |
| patient | bronze | SUCCESS | 300 | 03:01:35 | 03:01:48 |
| patient | silver | SUCCESS | 64 | 03:01:35 | 03:01:53 |
| encounter | raw | SUCCESS | 3 pages | 03:01:35 | 03:01:59 |
| encounter | bronze | SUCCESS | 300 | 03:01:35 | 03:02:06 |
| encounter | silver | SUCCESS | 60 | 03:01:35 | 03:02:10 |
| observation | raw | SUCCESS | 3 pages | 03:01:35 | 03:02:18 |
| observation | bronze | SUCCESS | 300 | 03:01:35 | 03:02:24 |
| observation | silver | SUCCESS | 61 | 03:01:35 | 03:02:32 |
| condition | raw | SUCCESS | 3 pages | 03:01:35 | 03:02:38 |
| condition | bronze | SUCCESS | 300 | 03:01:35 | 03:02:45 |
| condition | silver | SUCCESS | 60 | 03:01:35 | 03:02:51 |

**Run Duration:** ~1 min 16 sec | **Success Rate:** 12/12 ✅

---

## 4. Data Volume Summary

### Bronze Layer — Cumulative Record Growth

| Resource | After Day 1 | After Day 2 | After Day 3 | Total Growth |
|---|---|---|---|---|
| patient | 180 | 240 | 300 | +60/day |
| encounter | 180 | 240 | 300 | +60/day |
| observation | 180 | 240 | 300 | +60/day |
| condition | 180 | 240 | 300 | +60/day |

> Bronze layer uses **append mode** — each daily run adds 60 new records, preserving full ingestion history.

### Silver Layer — Current Records (SCD Type 2)

| Resource | Current Records | SCD Changes Detected |
|---|---|---|
| patient | 64 | Yes — 4 new versions created |
| encounter | 60 | No changes detected |
| observation | 61 | Yes — 1 new version created |
| condition | 60 | No changes detected |

> Silver layer maintains **only current records** via `is_current = true` filter, with full version history preserved.

### Gold Layer — Reporting Views

| View | Records | Columns | Type |
|---|---|---|---|
| gold_patient | 64 | 10 | Live View |
| gold_encounter | 60 | 10 | Live View |
| gold_observation | 61 | 13 | Live View |
| gold_condition | 60 | 14 | Live View |

---

## 5. SCD Type 2 Versioning Evidence

SCD Type 2 was implemented to track changes in FHIR records across daily loads. The Silver layer schema includes:

| Column | Purpose |
|---|---|
| `scd_start_date` | When this version became active |
| `scd_end_date` | When this version was superseded (NULL if still active) |
| `is_current` | TRUE = active record, FALSE = historical version |
| `scd_version` | Version number (1 = original, 2+ = updated) |

### Example — Patient SCD2 in action

```
id     | gender | scd_version | is_current | scd_start_date      | scd_end_date
-------|--------|-------------|------------|---------------------|--------------------
P001   | male   |      1      |   false    | 2026-05-27T02:57:00 | 2026-05-28T02:59:34
P001   | other  |      2      |   true     | 2026-05-28T02:59:34 | null
```

> Old version expired, new version inserted — full audit trail preserved.

---

## 6. Folder Structure

```
/Volumes/fhir_databricks/fhir_db/fhir_files/
└── raw/
    ├── patient/
    │   ├── 2026-05-27/
    │   │   ├── page_001.json
    │   │   ├── page_002.json
    │   │   └── page_003.json
    │   ├── 2026-05-28/
    │   │   ├── page_001.json  
    │   │   ├── page_002.json
    │   │   └── page_003.json
    │   └── 2026-05-29/
    │       ├── page_001.json
    │       ├── page_002.json
    │       └── page_003.json
    ├── encounter/   (same structure)
    ├── observation/ (same structure)
    └── condition/   (same structure)
```

**Total raw files:** 4 resources × 3 days × 3 pages = **36 JSON files**

---

## 7. Table Relationships

```
gold_patient (patient_id)
       │
       ├──────────────────────────┐
       │                          │
       ▼                          ▼
gold_encounter              gold_condition
 patient_ref ──► patient_id   patient_ref ──► patient_id
       │
       ▼
gold_observation
  patient_ref   ──► patient_id
  encounter_ref ──► encounter_id
```

---

## 8. Submission Checklist

| Requirement | Status |
|---|---|
| Fetch data incrementally for 2–3 days with pagination | 3 days × 3 pages |
| Store raw JSON as-is in Lakehouse | Date-bucketed folders |
| Convert JSON → Delta Tables (Bronze) | 4 Delta tables |
| Add `extraction_timestamp` metadata column | All tables |
| Add `api_url_or_params` metadata column | All tables |
| SCD Type 2 versioning | silver_* tables |
| Raw Layer (date-bucketed folder structure) | 36 JSON files |
| Bronze Layer (Delta + metadata) | 300 records/resource |
| Silver Layer (clean + deduplicate) | 60-64 current records |
| Gold Layer (reporting views) | 4 live views |
| Pipeline orchestration (Patient→Encounter→Observation→Condition) | 05_orchestration.py |
| Metadata logging | pipeline_run_log table |
| Modular & no hardcoding | All notebooks |
| Documentation | README + docs |

---

## 9. Notebooks Submitted

| Notebook | Purpose |
|---|---|
| `00_setup.py` | Environment setup, folder structure, database creation |
| `01_raw_ingestion.py` | FHIR API fetch with pagination, raw JSON storage |
| `02_bronze_layer.py` | JSON → Delta tables with metadata columns |
| `03_silver_layer.py` | Clean, deduplicate, SCD Type 2 versioning |
| `04_gold_layer.py` | Reporting-ready Gold views |
| `05_orchestration.py` | Master pipeline with run logging |

---

*FHIR API Data Ingestion & Analytics Assignment*
*Platform: Azure Databricks 15.4 LTS | PySpark + SQL | Delta Lake | Unity Catalog*
