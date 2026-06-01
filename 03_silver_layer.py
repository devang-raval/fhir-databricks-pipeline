# DBTITLE 1,Config
# 03_silver_layer.py
# Cleans, deduplicates and applies SCD Type 2 versioning

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, TimestampType
from datetime import datetime

# Configuration
CATALOG    = "fhir_databricks"
DATABASE   = "fhir_db"
RESOURCES  = ["patient", "encounter", "observation", "condition"]
RUN_DATE   = datetime.today().strftime("%Y-%m-%d")

print(f"Config loaded | Run date: {RUN_DATE}")


# DBTITLE 1,Clean & Deduplicate Function
# Clean and deduplicate incoming bronze data
def clean_bronze(resource: str):
    bronze_table = f"{CATALOG}.{DATABASE}.bronze_{resource}"
    
    df = spark.table(bronze_table)
    
    # Drop rows with no id
    df = df.filter(F.col("id").isNotNull())
    
    # Deduplicate: keep latest record per id
    df = df.withColumn(
        "row_num",
        F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"])
            .Window.partitionBy("id")
            .orderBy(F.col("extraction_timestamp").desc())
        )
    ).filter(F.col("row_num") == 1).drop("row_num")

    print(f"Cleaned {resource}: {df.count()} unique records")
    return df

print("Clean function ready")


# DBTITLE 1,SCD Type 2 Function
# SCD Type 2 upsert into silver table
from delta.tables import DeltaTable

def apply_scd2(resource: str, df_new):
    silver_table  = f"{CATALOG}.{DATABASE}.silver_{resource}"
    now           = datetime.now().isoformat()

    # Add SCD2 tracking columns to incoming data
    df_new = df_new \
        .withColumn("scd_start_date",  F.lit(now)) \
        .withColumn("scd_end_date",    F.lit(None).cast(StringType())) \
        .withColumn("is_current",      F.lit(True).cast(BooleanType())) \
        .withColumn("scd_version",     F.lit(1))

    # If silver table doesn't exist yet, create it
    if not spark.catalog.tableExists(silver_table):
        df_new.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(silver_table)
        print(f"Created silver_{resource} with {df_new.count()} records")
        return

    # Load existing silver table
    dt = DeltaTable.forName(spark, silver_table)
    df_existing = spark.table(silver_table)

    # Find changed records (id exists but content changed)
    df_existing_current = df_existing.filter(F.col("is_current") == True)

    # Hash all non-metadata cols to detect changes
    meta_cols  = ["scd_start_date", "scd_end_date", "is_current", 
                  "scd_version", "extraction_timestamp", "ingestion_date", 
                  "api_url_or_params", "source_file"]
    data_cols  = [c for c in df_new.columns if c not in meta_cols]

    df_new_hashed      = df_new.withColumn("new_hash", F.md5(F.concat_ws("|", *[F.col(c).cast(StringType()) for c in data_cols])))
    df_existing_hashed = df_existing_current.withColumn("old_hash", F.md5(F.concat_ws("|", *[F.col(c).cast(StringType()) for c in data_cols])))

    # Records that changed
    df_changed = df_new_hashed.alias("new") \
        .join(df_existing_hashed.select("id", "old_hash").alias("old"), "id", "inner") \
        .filter(F.col("new.new_hash") != F.col("old.old_hash")) \
        .select("new.*")

    changed_ids = [row["id"] for row in df_changed.select("id").collect()]

    if changed_ids:
        # Expire old records
        dt.update(
            condition = F.col("id").isin(changed_ids) & (F.col("is_current") == True),
            set = {
                "is_current":   "false",
                "scd_end_date": F.lit(now)
            }
        )
        # Insert new versions with incremented version
        df_new_versions = df_changed.drop("new_hash")
        max_versions = df_existing.groupBy("id").agg(F.max("scd_version").alias("max_ver"))
        df_new_versions = df_new_versions.join(max_versions, "id", "left") \
            .withColumn("scd_version", F.coalesce(F.col("max_ver"), F.lit(0)) + 1) \
            .drop("max_ver")

        df_new_versions.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table)

        print(f"{resource}: {len(changed_ids)} records versioned (SCD2)")
    else:
        print(f"{resource}: no changes detected")

    # Insert brand new records (id not in silver at all)
    existing_ids   = df_existing.select("id").distinct()
    df_new_records = df_new.join(existing_ids, "id", "left_anti").drop("new_hash") \
                           if "new_hash" in df_new.columns else \
                     df_new.join(existing_ids, "id", "left_anti")

    if df_new_records.count() > 0:
        df_new_records.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(silver_table)
        print(f"  ➕ {resource}: {df_new_records.count()} new records inserted")

print("SCD2 function ready")


# DBTITLE 1,Run Silver Layer
# Run clean + SCD2 for all resources
print(f"Starting Silver layer for {RUN_DATE}\n")

for resource in RESOURCES:
    print(f"\nProcessing {resource}...")
    try:
        df_clean = clean_bronze(resource)
        apply_scd2(resource, df_clean)
    except Exception as e:
        print(f"{resource} failed: {e}")

print(f"\n Silver layer complete!")


# DBTITLE 1,Verify
# Verify silver tables
for resource in RESOURCES:
    table = f"{CATALOG}.{DATABASE}.silver_{resource}"
    df    = spark.table(table)
    curr  = df.filter(F.col("is_current") == True).count()
    total = df.count()
    print(f"silver_{resource}: {total} total rows | {curr} current | SCD2 cols: scd_start_date, scd_end_date, is_current, scd_version")
