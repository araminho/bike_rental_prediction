# Bike rental pipeline and regression analysis

## Overview

This is a small project to demonstrate my skills in Data Engineering and Data Science. 

The Data Engineering part consists of 2 Airflow dags (`dags/bike_data_dag.py` and `dags/weather_nyc_dag.py`).
These dags gather data from the Open-Meteo API (https://open-meteo.com/)
and City Bike CSV files (https://s3.amazonaws.com/tripdata/index.html)  and store them into a Postgres database.

Based on that data a csv file is formed, which serves to the second part - Data Science.
It consists of one Jupyter notebook (`Bike_sharing_dataset_Advanced_regression.ipynb`), 
where I do regression analysis of bike rental hourly count.


## Prerequisites

- Docker and Docker Compose installed on your machine.
- Access to a PostgreSQL database (for `bike_db`).

## Setup Instructions

1. **Clone the Repository**:
   ```bash
   git https://github.com/araminho/bike_rental_prediction.git
   cd bike_rental_prediction
   ```

2. **Create a `.env` File**:
   Copy the example environment file and update it with your PostgreSQL credentials:
   ```bash
   cp .env.example .env
   ```

3. **Update the `.env` File**:
   Edit the `.env` file to set your external PostgreSQL credentials:
   ```text
   EXTERNAL_PG_USER=your_pg_user
   EXTERNAL_PG_PASSWORD=your_pg_password
   ```

4. **Start the Services**:
   Run the following command to start the Docker containers:
   ```bash
   docker compose up -d
   ```

5. **Access Airflow**:
   Open your browser and go to `http://localhost:8080`. Log in with:
   - Username: `airflow`
   - Password: `airflow`

6. **Setup Postgres database**:
    Create a new Postgres database `bike_db`. No need to create the tables, 
they will be created automatically by the pipeline during the first run.

7. In Airflow Dashboard you will see a new DAGs - "weather_nyc_2023_2024". Activate it and run it. 
8. If the run is successful, check the database. There should be 2 tables - "forecast_requests" should have 
1 new entry after each run, and "weather_data" should have updated entries for the full years 2023 and 2024.
9. Similarly activate and run the DAG "citibike_etl_pipeline". The result should be a new table "bike_data".
This DAG takes a bit long to execute.

## Instructions for Data Science part
1. (Optional) In your Postgres client run the query from "query.sql" file and dump the results into a csv file, 
or use the existing file "bike-weather-data.csv".
2. Run the Jupyter notebook "Bike_sharing_dataset_Advanced_regression.ipynb", 
placing it in the same folder with the csv file.
