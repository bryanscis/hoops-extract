# 🏀 Hoops-Extract

## About
This project implements an ETL (Extract, Transform, Load) pipeline designed to automate the ingestion, transformation, and loading of NBA data into a PostgreSQL database. It handles game scheduling, player information, team details, and player statistics, loading them into a PostgreSQL database.

<img src="https://github.com/bryanscis/hoops-extract/blob/3ceff60c77002bece6d47a4c31dc2bd63518ad98/misc/images/diagram.png" alt="diagram" width="750"/>

## Data Pipeline Overview
The ETL pipeline is implemented using Apache Airflow. It handles:

- Extraction: Fetching NBA data (games, players, teams, etc..) from external sources.
- Transformation: Cleaning, normalizing, and organizing the data.
- Loading: Inserting or updating records in a PostgreSQL database.

All transformations and data-loading tasks are managed through Airflow DAGs.

## Getting Started
Before starting, make sure that Docker and Docker Compose has been installed. Clone this repository and populate the .env file with the following:
```
AIRFLOW_DB_USER=airflow_user  
AIRFLOW_DB_PASSWORD=airflow_password
AIRFLOW_DB_NAME=airflow_db

DB_USER=postgres
DB_PASSWORD=your_postgres_password
DB_NAME=nba_db

AIRFLOW__CORE__FERNET_KEY=your_fernet_key
AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key
```
To start the program, run:
```
docker-compose up --build 
```
to setup the containers. This command should set up the necessary containers and once the containers are up and running, Airflow webserver should be able to be accessed at http://0.0.0.0:8000.


## 🚀 What's Next
- Snowflake integration for data warehousing
- Apache Spark for transformations
- Shot chart information
