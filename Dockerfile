# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set environment variables
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=${AIRFLOW_HOME}

# Install dependencies
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       default-libmysqlclient-dev \
       libpq-dev \
       curl \
       gnupg \
       locales \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install pip packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your project files
COPY . ${AIRFLOW_HOME}

# Set working directory
WORKDIR ${AIRFLOW_HOME}

# Expose the port for the Airflow webserver
EXPOSE 8080

# Define the default command to start the webserver
CMD ["airflow", "webserver"]
