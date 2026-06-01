# 04_gold_layer.py
# Creates Gold layer views optimised for reporting and analytics

from pyspark.sql import functions as F
from datetime import datetime

# Configuration
CATALOG   = "fhir_databricks"
DATABASE  = "fhir_db"

print("Config loaded")


# DBTITLE 1,Gold Patient
# Gold: Patient
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{DATABASE}.gold_patient AS
    SELECT
        id                                          AS patient_id,
        gender,
        birthDate                                   AS birth_date,
        -- Extract name from nested JSON string
        CAST(name AS STRING)                        AS name_raw,
        ingestion_date,
        extraction_timestamp,
        scd_start_date,
        scd_end_date,
        is_current,
        scd_version
    FROM {CATALOG}.{DATABASE}.silver_patient
    WHERE is_current = true
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{DATABASE}.gold_patient").collect()[0]["cnt"]
print(f"gold_patient view created → {count} records")


# DBTITLE 1,Gold Encounter
# Gold: Encounter 
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{DATABASE}.gold_encounter AS
    SELECT
        id                                          AS encounter_id,
        CAST(subject AS STRING)                     AS patient_ref,
        CAST(period  AS STRING)                     AS encounter_period,
        CAST(identifier AS STRING)                  AS identifier_raw,
        ingestion_date,
        extraction_timestamp,
        scd_start_date,
        scd_end_date,
        is_current,
        scd_version
    FROM {CATALOG}.{DATABASE}.silver_encounter
    WHERE is_current = true
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{DATABASE}.gold_encounter").collect()[0]["cnt"]
print(f"✅ gold_encounter view created → {count} records")

# COMMAND ----------

# DBTITLE 1,Gold Observation
# Gold: Observation
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{DATABASE}.gold_observation AS
    SELECT
        id                                          AS observation_id,
        CAST(subject  AS STRING)                    AS patient_ref,
        CAST(encounter AS STRING)                   AS encounter_ref,
        status,
        CAST(code     AS STRING)                    AS observation_code,
        CAST(valueQuantity AS STRING)               AS value_quantity,
        effectiveDateTime                           AS effective_date,
        ingestion_date,
        extraction_timestamp,
        scd_start_date,
        scd_end_date,
        is_current,
        scd_version
    FROM {CATALOG}.{DATABASE}.silver_observation
    WHERE is_current = true
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{DATABASE}.gold_observation").collect()[0]["cnt"]
print(f"gold_observation view created → {count} records")


# DBTITLE 1,Gold Condition
# Gold: Condition 
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{DATABASE}.gold_condition AS
    SELECT
        id                                          AS condition_id,
        CAST(subject          AS STRING)            AS patient_ref,
        CAST(encounter        AS STRING)            AS encounter_ref,
        CAST(code             AS STRING)            AS condition_code,
        CAST(clinicalStatus   AS STRING)            AS clinical_status,
        CAST(verificationStatus AS STRING)          AS verification_status,
        onsetDateTime                               AS onset_date,
        recordedDate                                AS recorded_date,
        ingestion_date,
        extraction_timestamp,
        scd_start_date,
        scd_end_date,
        is_current,
        scd_version
    FROM {CATALOG}.{DATABASE}.silver_condition
    WHERE is_current = true
""")

count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{DATABASE}.gold_condition").collect()[0]["cnt"]
print(f"gold_condition view created → {count} records")


# DBTITLE 1,Verify All Gold Views
# Final verification
gold_views = ["gold_patient", "gold_encounter", "gold_observation", "gold_condition"]

print("📊 Gold Layer Summary:\n")
for view in gold_views:
    df    = spark.sql(f"SELECT * FROM {CATALOG}.{DATABASE}.{view} LIMIT 1")
    count = spark.sql(f"SELECT COUNT(*) as cnt FROM {CATALOG}.{DATABASE}.{view}").collect()[0]["cnt"]
    cols  = len(df.columns)
    print(f"{view}: {count} rows | {cols} columns")

print("\n Gold layer complete! Ready for reporting.")