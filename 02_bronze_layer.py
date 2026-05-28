# 02_bronze_layer.py
# Reads raw JSON files and saves to Delta tables with metadata columns

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from datetime import datetime

# Configuration
CATALOG    = "fhir_databricks"
DATABASE   = "fhir_db"
BASE_PATH  = f"/Volumes/{CATALOG}/{DATABASE}/fhir_files"
RESOURCES  = ["patient", "encounter", "observation", "condition"]
RUN_DATE   = datetime.today().strftime("%Y-%m-%d")

print(f"Config loaded | Run date: {RUN_DATE}")

# DBTITLE 1,Bronze Load Function

# Function to load raw JSON → Bronze Delta table
def load_bronze(resource: str, run_date: str):
    raw_path = f"{BASE_PATH}/raw/{resource}/{run_date}/"
    table_name = f"{CATALOG}.{DATABASE}.bronze_{resource}"

    print(f"\n Processing {resource}...")

    # Read all JSON pages for the day
    df = spark.read.option("multiline", "true").json(raw_path)

    # Explode entries from FHIR bundle
    if "entry" in df.columns:
        df = df.select(F.explode("entry").alias("entry")) \
               .select("entry.resource.*")

    # Add metadata columns
    df = df.withColumn("extraction_timestamp", F.lit(datetime.now().isoformat())) \
           .withColumn("api_url_or_params", F.lit(f"https://hapi.fhir.org/baseR4/{resource.capitalize()}")) \
           .withColumn("ingestion_date", F.lit(run_date)) \
           .withColumn("source_file", F.col("_metadata.file_path").cast(StringType()))

    # Cast all columns to string to avoid schema conflicts 
    for col in df.columns:
        df = df.withColumn(col, F.col(col).cast(StringType()))

    # Write to Delta table (append)
    df.write \
      .format("delta") \
      .mode("append") \
      .option("mergeSchema", "true") \
      .saveAsTable(table_name)

    count = spark.table(table_name).count()
    print(f"bronze_{resource} → {count} total records")
    return count

print("Bronze function : ready")


# DBTITLE 1,Run Bronze Load
# Run for all resources
print(f"Starting Bronze layer load for {RUN_DATE}\n")

for resource in RESOURCES:
    try:
        load_bronze(resource, RUN_DATE)
    except Exception as e:
        print(f" {resource} failed: {e}")

print(f"\n Bronze layer complete!")

# DBTITLE 1, Varify
# Quick verification 

for resource in RESOURCES:
    table = f"{CATALOG}.{DATABASE}.bronze_{resource}"
    count = spark.table(table).count()
    cols  = len(spark.table(table).columns)
    print(f"bronze_{resource}: {count} rows, {cols} columns")