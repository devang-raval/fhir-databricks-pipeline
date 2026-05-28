# 05_orchestration.py
# Master pipeline: runs all layers in order with logging

from datetime import datetime

# Config
CATALOG   = "fhir_databricks"
DATABASE  = "fhir_db"
BASE_PATH = f"/Volumes/{CATALOG}/{DATABASE}/fhir_files"
RUN_DATE  = datetime.today().strftime("%Y-%m-%d")
RUN_TS    = datetime.now().isoformat()

print(f"Pipeline config loaded")
print(f"Run date : {RUN_DATE}")
print(f"Run time : {RUN_TS}")

# DBTITLE 1,Metadata Logging Table

# Create pipeline run log table 
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{DATABASE}.pipeline_run_log (
        run_id          STRING,
        run_date        STRING,
        resource        STRING,
        layer           STRING,
        status          STRING,
        records_loaded  LONG,
        error_message   STRING,
        started_at      STRING,
        completed_at    STRING
    )
    USING DELTA
""")

print("Pipeline run log table ready")


# DBTITLE 1,Logging Helper
# Log helper 
import uuid

RUN_ID = str(uuid.uuid4())[:8]

def log_run(resource, layer, status, records=0, error=None):
    completed   = datetime.now().isoformat()
    error_msg   = "" if error is None else str(error)[:200]
    # Clean single quotes to avoid SQL injection issues
    error_msg   = error_msg.replace("'", "")

    spark.sql(f"""
        INSERT INTO {CATALOG}.{DATABASE}.pipeline_run_log VALUES (
            '{RUN_ID}',
            '{RUN_DATE}',
            '{resource}',
            '{layer}',
            '{status}',
            {records},
            '{error_msg}',
            '{RUN_TS}',
            '{completed}'
        )
    """)

print(f"Logger ready | Run ID: {RUN_ID}")


# DBTITLE 1,Pipeline Runner

# Master pipeline: Patient → Encounter → Observation → Condition 

import requests, json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType
from delta.tables import DeltaTable

BASE_URL   = "https://hapi.fhir.org/baseR4"
PAGE_COUNT = 20
MAX_PAGES  = 3
RESOURCES  = ["Patient", "Encounter", "Observation", "Condition"]

# Raw ingestion

def fetch_and_save(resource, run_date):
    url    = f"{BASE_URL}/{resource}"
    params = {"_count": PAGE_COUNT, "_format": "json"}
    page, saved = 1, []
    while url and page <= MAX_PAGES:
        resp = requests.get(url, params=params if page == 1 else None, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        out  = f"{BASE_PATH}/raw/{resource.lower()}/{run_date}/page_{page:03d}.json"
        with open(out, "w") as f:
            json.dump(data, f)
        saved.append(out)
        url = next((l["url"] for l in data.get("link", []) if l.get("relation") == "next"), None)
        params = None
        page  += 1
    return saved

# -- BRONZE LAYER --

# Bronze load

def load_bronze(resource, run_date):
    raw_path   = f"{BASE_PATH}/raw/{resource}/{run_date}/"
    table_name = f"{CATALOG}.{DATABASE}.bronze_{resource}"
    df = spark.read.option("multiline", "true").json(raw_path)
    if "entry" in df.columns:
        df = df.select(F.explode("entry").alias("entry")).select("entry.resource.*")
    df = df.withColumn("extraction_timestamp", F.lit(datetime.now().isoformat())) \
           .withColumn("api_url_or_params",    F.lit(f"{BASE_URL}/{resource.capitalize()}")) \
           .withColumn("ingestion_date",        F.lit(run_date)) \
           .withColumn("source_file",           F.col("_metadata.file_path").cast(StringType()))
    for col in df.columns:
        df = df.withColumn(col, F.col(col).cast(StringType()))
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    return spark.table(table_name).count()

# -- SILVER LAYER --

# Clean bronze 

def clean_bronze(resource):
    from pyspark.sql.window import Window
    df = spark.table(f"{CATALOG}.{DATABASE}.bronze_{resource}").filter(F.col("id").isNotNull())
    df = df.withColumn("row_num",
            F.row_number().over(
                Window.partitionBy("id").orderBy(F.col("extraction_timestamp").desc())
            )).filter(F.col("row_num") == 1).drop("row_num")
    return df

# SCD Type 2 

def apply_scd2(resource, df_new):
    silver_table = f"{CATALOG}.{DATABASE}.silver_{resource}"
    now          = datetime.now().isoformat()
    df_new = df_new \
        .withColumn("scd_start_date", F.lit(now)) \
        .withColumn("scd_end_date",   F.lit(None).cast(StringType())) \
        .withColumn("is_current",     F.lit(True).cast(BooleanType())) \
        .withColumn("scd_version",    F.lit(1))
    if not spark.catalog.tableExists(silver_table):
        df_new.write.format("delta").mode("overwrite").saveAsTable(silver_table)
        return
    dt           = DeltaTable.forName(spark, silver_table)
    df_existing  = spark.table(silver_table)
    meta_cols    = ["scd_start_date","scd_end_date","is_current","scd_version",
                    "extraction_timestamp","ingestion_date","api_url_or_params","source_file"]
    data_cols    = [c for c in df_new.columns if c not in meta_cols]
    df_nh        = df_new.withColumn("new_hash", F.md5(F.concat_ws("|", *[F.col(c) for c in data_cols])))
    df_eh        = df_existing.filter(F.col("is_current")==True) \
                              .withColumn("old_hash", F.md5(F.concat_ws("|", *[F.col(c) for c in data_cols])))
    changed_ids  = [r["id"] for r in df_nh.alias("n").join(df_eh.select("id","old_hash").alias("o"),"id","inner") \
                              .filter(F.col("n.new_hash") != F.col("o.old_hash")).select("n.id").collect()]
    if changed_ids:
        dt.update(F.col("id").isin(changed_ids) & (F.col("is_current")==True),
                  {"is_current": "false", "scd_end_date": F.lit(now)})
        max_v = df_existing.groupBy("id").agg(F.max("scd_version").alias("mv"))
        df_nv = df_nh.filter(F.col("id").isin(changed_ids)).drop("new_hash") \
                     .join(max_v,"id","left") \
                     .withColumn("scd_version", F.coalesce(F.col("mv"),F.lit(0))+1).drop("mv")
        df_nv.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(silver_table)
    existing_ids = df_existing.select("id").distinct()
    df_nr        = df_new.drop("new_hash") if "new_hash" in df_new.columns else df_new
    df_nr        = df_nr.join(existing_ids,"id","left_anti")
    if df_nr.count() > 0:
        df_nr.write.format("delta").mode("append").option("mergeSchema","true").saveAsTable(silver_table)


# Run pipeline 
def run_pipeline():
    print(f"\n{'='*55}")
    print(f"FHIR PIPELINE START | {RUN_DATE} | Run: {RUN_ID}")
    print(f"{'='*55}\n")

    for resource in RESOURCES:
        r = resource.lower()
        print(f"\n{'─'*45}")
        print(f"Processing: {resource}")
        print(f"{'─'*45}")

        try:
            print(f"  [1/3] Raw ingestion...")
            files = fetch_and_save(resource, RUN_DATE)
            log_run(r, "raw", "SUCCESS", len(files))
            print(f"Raw: {len(files)} pages saved")
        except Exception as e:
            log_run(r, "raw", "FAILED", 0, e)
            print(f"Raw failed: {e}")
            continue

        try:
            print(f"  [2/3] Bronze load...")
            cnt = load_bronze(r, RUN_DATE)
            log_run(r, "bronze", "SUCCESS", cnt)
            print(f"Bronze: {cnt} total records")
        except Exception as e:
            log_run(r, "bronze", "FAILED", 0, e)
            print(f"Bronze failed: {e}")
            continue

        try:
            print(f"  [3/3] Silver SCD2...")
            df_clean = clean_bronze(r)
            apply_scd2(r, df_clean)
            cnt = spark.table(f"{CATALOG}.{DATABASE}.silver_{r}").count()
            log_run(r, "silver", "SUCCESS", cnt)
            print(f"Silver: {cnt} total records")
        except Exception as e:
            log_run(r, "silver", "FAILED", 0, e)
            print(f"Silver failed: {e}")
            continue

        print(f"{resource} complete!")

    print(f"\n{'='*55}")
    print(f"PIPELINE COMPLETE | Run: {RUN_ID}")
    print(f"{'='*55}\n")

run_pipeline()

# COMMAND ----------

# DBTITLE 1,View Pipeline Log
# Show pipeline run log 
print(f"Pipeline Run Log (Run ID: {RUN_ID})\n")
display(
    spark.table(f"{CATALOG}.{DATABASE}.pipeline_run_log")
         .filter(f"run_id = '{RUN_ID}'")
         .orderBy("resource", "layer")
)