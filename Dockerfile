# Use official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code, data, and credentials
COPY . .

# Set environment variable for authentication
ENV GOOGLE_APPLICATION_CREDENTIALS=service_account.json

# Entry point
CMD ["python", "upload_to_bigquery.py"]
