from google.cloud import bigquery
import pandas as pd
import os

# Service account key path (should be passed via env var or mounted secret)
key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

# Create BigQuery client
client = bigquery.Client()

# Load CSV
df = pd.read_csv("customers.csv")

# Define table ID
table_id = "e-outrider-466612-u0.demo_dataset.customers"  # Replace with your project.dataset.table

# Load config
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
)

# Upload
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()
print(f"✅ Uploaded to {table_id}")
