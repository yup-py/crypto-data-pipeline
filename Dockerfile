# Use the official production-ready Airflow image as a base
FROM apache/airflow:2.9.2-python3.11

# Copy your local requirements list into the container
COPY requirements.txt /requirements.txt

# Install the pipeline dependencies inside the container environment
RUN pip install --no-cache-dir -r /requirements.txt