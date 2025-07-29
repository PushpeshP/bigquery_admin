from google.cloud import bigquery
from google.oauth2 import service_account
import os

credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
credentials = service_account.Credentials.from_service_account_file(credentials_path)
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

table_id = "e-outrider-466612-u0.demo_dataset.customers_new"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("age", "STRING"),
        bigquery.SchemaField("city", "STRING"),
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition="WRITE_TRUNCATE"  # Overwrites previous data
)

with open("sample.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, table_id, job_config=job_config)

job.result()  # Wait for the load to complete

# Confirm row count
table = client.get_table(table_id)
print(f"✅ Loaded {table.num_rows} rows into {table_id}")
