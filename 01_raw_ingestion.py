# 01_raw_ingestion.py
# Fetches FHIR API data and stores raw JSON in date-bucketed folders

import requests
import json
from datetime import datetime, timedelta

# Configuration
CATALOG     = "fhir_databricks"
DATABASE    = "fhir_db"
BASE_PATH   = f"/Volumes/{CATALOG}/{DATABASE}/fhir_files"
BASE_URL    = "https://hapi.fhir.org/baseR4"
PAGE_COUNT  = 20        # records per page
MAX_PAGES   = 3         # max pages per resource per day (keeps it light)
RESOURCES   = ["Patient", "Encounter", "Observation", "Condition"]
RUN_DATE    = datetime.today().strftime("%Y-%m-%d")

print(f"Config loaded | Run Date: {RUN_DATE}")


# DBTITLE 1,Ingestion Function

# Fetch and save raw JSON Data
def fetch_and_save(resource: str, run_date: str):
    url = f"{BASE_URL}/{resource}"
    params = {"_count": PAGE_COUNT, "_format": "json"}
    page = 1
    saved_files = []

    while url and page <= MAX_PAGES:
        print(f"Fetching {resource} page {page} → {url}")
        
        response = requests.get(url, params=params if page == 1 else None, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Save raw JSON as-is
        out_path = f"{BASE_PATH}/raw/{resource.lower()}/{run_date}/page_{page:03d}.json"
        with open(out_path, "w") as f:
            json.dump(data, f)

        print(f"Saved: raw/{resource.lower()}/{run_date}/page_{page:03d}.json ({len(data.get('entry', []))} records)")
        saved_files.append(out_path)

        # Pagination: find next link
        url = None
        for link in data.get("link", []):
            if link.get("relation") == "next":
                url = link.get("url")
                params = None  # params already in next URL
                break
        
        page += 1

    return saved_files

print("Fetch function ready")

# COMMAND ----------

# DBTITLE 1,Run ingestion
# Run for all resources
all_saved = {}

for resource in RESOURCES:
    print(f"\Ingesting {resource}")
    try:
        files = fetch_and_save(resource, RUN_DATE)
        all_saved[resource] = files
        print(f"{resource} done — {len(files)} pages saved")
    except Exception as e:
        print(f"{resource} failed: {e}")

print(f"\nRaw ingestion complete for {RUN_DATE}!")
print(f"Total resources ingested: {len(all_saved)}")