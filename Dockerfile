FROM python:3.10-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ This copies your Python script, CSV file, and everything else into /app
COPY . .

# ✅ Ensure the working directory is set and command runs the uploader
CMD ["python", "upload_to_bigquery.py"]
