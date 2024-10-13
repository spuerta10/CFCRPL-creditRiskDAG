# Use the official Airflow image as a base
FROM apache/airflow:2.9.3-python3.11

# Install additional Python packages
COPY requirements.txt /requirements.txt

USER root

# Install git
RUN apt-get update && apt-get install -y git

RUN python3 -m pip install --no-cache-dir -r /requirements.txt