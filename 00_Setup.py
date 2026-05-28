# 00_setup.py
# FHIR Medallion Architecture Base Folder Structure

from datetime import datetime, timedelta

# Configuration
CATALOG     = "fhir_databricks"
DATABASE    = "fhir_db"
RESOURCES   = ["patient", "encounter", "observation", "condition"]
NUM_DAYS    = 3

# Create Database
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")
print(f"Database '{CATALOG}.{DATABASE}' Status : Ready")

# Create volume
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG}.{DATABASE}.fhir_files
    COMMENT 'FHIR raw data storage'
""")
print(f"Volume '{CATALOG}.{DATABASE}.fhir_files' Status : Ready")

# Base path
BASE_PATH = f"/Volumes/{CATALOG}/{DATABASE}/fhir_files"

# Generate last N dates
dates = [(datetime.today() - timedelta(days=i)).strftime("%Y-%m-%d") 
         for i in range(NUM_DAYS)]

# Create raw folder structure
for resource in RESOURCES:
    for date in dates:
        path = f"{BASE_PATH}/raw/{resource}/{date}"
        dbutils.fs.mkdirs(path)
        print(f"Created: raw/{resource}/{date}")

print("\n Setup Status : Completed")