import os
from google.cloud import bigquery

credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

# ✅ Secret file must be a real file
if not credentials_path or not os.path.exists(credentials_path):
    raise Exception(f"Credential file not found or not mounted: {credentials_path}")

client = bigquery.Client()

table_id = "e-outrider-466612-u0.demo_dataset.customers_new"

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

with open("sample.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()
print(f"✅ Loaded data into {table_id}")
