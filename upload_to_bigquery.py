import os
from google.cloud import bigquery
import pandas as pd

#Check environment variable path
print("Auth JSON path:", os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))

# Initialize BigQuery client
client = bigquery.Client()

# Load CSV file
df = pd.read_csv("customers.csv")
table_id = "e-outrider-466612-u0.demo_dataset.customers"  # Replace with your project.dataset.table

# Load configuration
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
)

# Upload to BigQuery
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()
print(f"✅ Uploaded to {table_id}")
