import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Set the path explicitly (or get from env)
credentials_path = "service_account.json"

# Load credentials
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Initialize client
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# BigQuery target table
table_id = "e-outrider-466612-u0.demo_dataset.customers_new"

# Job config for CSV
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

# Load the data
with open("sample.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Wait for the job to complete

print(f"✅ Data loaded into {table_id}")
