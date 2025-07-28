# upload_to_bigquery.py
from flask import Flask
import threading
from google.cloud import bigquery
import pandas as pd

def upload_data():
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
    print("✅ Upload complete")

app = Flask(__name__)

@app.route("/")
def index():
    return "App is alive"

# Run upload in background
threading.Thread(target=upload_data).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
