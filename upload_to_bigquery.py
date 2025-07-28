from google.cloud import bigquery
import pandas as pd
import os

# STEP 1: Set the path to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "path/to/your/service_account.json"

# STEP 2: BigQuery client
client = bigquery.Client()

# STEP 3: Load your CSV using pandas
df = pd.read_csv("customers.csv")  # Your CSV file

# STEP 4: Define BigQuery destination
table_id = "e-outrider-466612-u0.demo_dataset.customers"  # Format: project.dataset.table

# STEP 5: Upload job configuration
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite if exists
    autodetect=True,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
)

# STEP 6: Upload to BigQuery
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for completion

# STEP 7: Verify
table = client.get_table(table_id)
print(f"✅ Uploaded {table.num_rows} rows to {table_id}")
