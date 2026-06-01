# FHIR API Data Ingestion & Analytics Pipeline
### Azure Databricks | Medallion Architecture | Delta Lake | SCD Type 2

---

## Overview

End-to-end data engineering pipeline that ingests healthcare data from a public FHIR API and processes it through a **Medallion Lakehouse Architecture** (Raw → Bronze → Silver → Gold) on Azure Databricks. Supports incremental daily ingestion, SCD Type 2 versioning, and metadata logging.

---

## Architecture

```
FHIR API (hapi.fhir.org/baseR4)
           │
           ▼
┌─────────────────────────────────────────┐
│              RAW LAYER                  │
│  /raw/{resource}/{date}/page_00N.json   │
│  36 JSON files (4 resources × 3 days    │
│  × 3 pages)                             │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│            BRONZE LAYER                 │
│  Delta Tables + Metadata Columns        │
│  extraction_timestamp                   │
│  api_url_or_params                      │
│  ingestion_date | source_file           │
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
│  Reporting-ready Live Views             │
│  gold_patient | gold_encounter          │
│  gold_observation | gold_condition      │
└─────────────────────────────────────────┘
```

---

## Project Structure

```
fhir-databricks-pipeline/
├── notebooks/
│   ├── 00_setup.py               # Environment & folder structure setup
│   ├── 01_raw_ingestion.py       # FHIR API fetch with pagination (JSON)
│   ├── 02_bronze_layer.py        # JSON → Delta tables with metadata
│   ├── 03_silver_layer.py        # Clean, deduplicate + SCD Type 2
│   ├── 04_gold_layer.py          # Reporting views
│   ├── 05_orchestration.py       # Master pipeline with run logging
├── documents/
│   └── Pipeline Execution Report.md # 3-day run results
└── README.md
```

---

## FHIR Resources Ingested

| Resource | Description |
|---|---|
| **Patient** | Person receiving healthcare |
| **Encounter** | Hospital visit or appointment |
| **Observation** | Test result (e.g. blood pressure) |
| **Condition** | Diagnosis (e.g. diabetes) |

---

## Pipeline Execution Results (3 Days)

| Date | Run ID | Resources | Layers | Status |
|---|---|---|---|---|
| 2026-05-27 | c32132bb | 4/4 | 12/12 | All SUCCESS |
| 2026-05-28 | 30793f94 | 4/4 | 12/12 | All SUCCESS |
| 2026-05-29 | 14537555 | 4/4 | 12/12 | All SUCCESS |

### Data Volume

| Resource | Bronze (total) | Silver (current) | Gold (view) |
|---|---|---|---|
| patient | 300 | 64 | 64 |
| encounter | 300 | 60 | 60 |
| observation | 300 | 61 | 61 |
| condition | 300 | 60 | 60 |

---

## Key Features

- **Incremental Ingestion** — daily runs with pagination (20 records/page, 3 pages/resource)
- **Medallion Architecture** — Raw, Bronze, Silver, Gold with clear separation of concerns
- **SCD Type 2** — full history tracking across daily loads
- **Metadata Columns** — `extraction_timestamp`, `api_url_or_params`, `ingestion_date`, `source_file`
- **Pipeline Run Logging** — every run logged to `pipeline_run_log` Delta table
- **Modular Code** — parameterised, no hardcoding, reusable functions
- **Unity Catalog** — governed storage on Azure Databricks

---

## Tech Stack

| Component | Technology |
|---|---|
| Platform | Azure Databricks |
| Runtime | Databricks 15.4 LTS (Apache Spark 3.5) |
| Storage | Unity Catalog Volumes |
| Table Format | Delta Lake |
| Languages | PySpark, SQL, Python |
| Source API | FHIR R4 (hapi.fhir.org) |
| Reporting | Version Control |
| GitHub |

---

## How to Run

### Prerequisites
- Azure Databricks workspace (Runtime 15.4 LTS)
- Unity Catalog enabled
- Internet access to hapi.fhir.org

### First Time Setup
```
1. Open 00_setup.py → Run All
```

### Daily Run
```
2. Open 05_orchestration.py
3. Set RUN_DATE = "YYYY-MM-DD"  (or leave as datetime.today())
4. Run All
```

### Backfill Past Dates
```
5. In 05_orchestration.py Cell 1, set:
   RUN_DATE = "2026-05-27"  # change for each date
6. Run All → repeat for each date
```

---

## Configuration

All settings centralised at top of each notebook:

```python
CATALOG    = "fhir_databricks"   # Unity Catalog name
DATABASE   = "fhir_db"           # Database name
BASE_URL   = "https://hapi.fhir.org/baseR4"
PAGE_COUNT = 20                  # Records per API page
MAX_PAGES  = 3                   # Max pages per resource per run
```

---

## Table Relationships

```
gold_patient (patient_id)
       │
       ├──────────────────────────┐
       │                          │
       ▼                          ▼
gold_encounter              gold_condition
 patient_ref → patient_id    patient_ref → patient_id
       │
       ▼
gold_observation
  patient_ref   → patient_id
  encounter_ref → encounter_id
```

---

## SCD Type 2 Example

| id | gender | scd_version | is_current | scd_start_date | scd_end_date |
|---|---|---|---|---|---|
| P001 | male | 1 | false | 2026-05-27 | 2026-05-28 |
| P001 | other | 2 | true | 2026-05-28 | null |

---

## Submission Checklist

| Requirement | Status |
|---|---|
| Incremental ingestion 2-3 days with pagination | 3 days × 3 pages |
| Raw JSON stored as-is date-bucketed | 36 files |
| JSON → Delta Tables (Bronze) | 4 tables |
| extraction_timestamp metadata column | All tables |
| api_url_or_params metadata column | All tables |
| SCD Type 2 versioning | silver_* tables |
| Raw Layer (folder structure) | Done |
| Bronze Layer (Delta + metadata) | Done |
| Silver Layer (clean + deduplicate) | Done |
| Gold Layer (reporting views) | Done |
| Orchestration pipeline (correct order) | Done |
| Metadata logging | pipeline_run_log |
| Modular & no hardcoding | All notebooks |
| Documentation | /docs folder |


---

*Platform: Azure Databricks 15.4 LTS | PySpark + SQL | Delta Lake | Unity Catalog*
