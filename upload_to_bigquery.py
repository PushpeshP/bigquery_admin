import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Use path from Secret Manager
key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

if not key_path or not os.path.exists(key_path):
    raise Exception(f"GOOGLE_APPLICATION_CREDENTIALS not set or file not found at {key_path}")

# ✅ Load credentials manually
credentials = service_account.Credentials.from_service_account_file(key_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Table info
table_id = "e-outrider-466612-u0.demo_dataset.customers_new"

# Job config
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

# Load file
with open("sample.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()
print(f"✅ Loaded data into {table_id}")
