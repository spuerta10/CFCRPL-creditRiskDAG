# Use the official Airflow image as a base
FROM apache/airflow:2.9.3-python3.11

# Install additional Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt