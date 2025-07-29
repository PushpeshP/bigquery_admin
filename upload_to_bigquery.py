import os
from google.cloud import bigquery
from google.oauth2 import service_account

# Secret Manager mount path in Cloud Run
credentials_path = "/secrets/GOOGLE_APPLICATION_CREDENTIALS"

# Load credentials from the mounted secret file
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# Target BigQuery table
table_id = "e-outrider-466612-u0.demo_dataset.customers_new"

# Job config
job_config = bigquery.LoadJobConfig(
    autodetect=True,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

# Load the CSV file
with open("sample.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Wait for job to complete

print(f"✅ Successfully loaded data into {table_id}")
