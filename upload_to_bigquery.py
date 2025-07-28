from google.cloud import bigquery
import pandas as pd
import os

def main():
    print("✅ Starting BigQuery Upload")
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/secrets/bq-sa-key")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    client = bigquery.Client()
    df = pd.read_csv("customers.csv")

    table_id = "e-outrider-466612-u0.demo_dataset.customers"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Successfully uploaded to {table_id}")

if __name__ == "__main__":
    main()