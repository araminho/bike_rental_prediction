This is a small project to demonstrate my skills in Data Engineering and Data Science. 
Here I created a data pipeline from the Open-Meteo API (https://open-meteo.com/)
and City Bike CSV files (https://s3.amazonaws.com/tripdata/index.html). 
Then I trained and evaluated several machine learning models to predict the hourly bike usage.

In order to run the pipeline, you need to have Postgres and Apache Airflow installed. 
The following setup steps are needed.
1. Create a new Postgres database or use an existing one. No need to create the tables, 
they will be created automatically by the pipeline during the first run.
2. In Airflow Dashboard create a new Postgres connection with the name "bike_db_conn". 
Put the credentials of your Postgres database. 
If you are using Docker, put "host.docker.internal" as the host.
3. Put the 2 Python files (bike_data_dag.py and weather_nyc_dag.py) in your Airflow repository. 
The files should be placed in the same folder.
4. Launch Airflow. If you are using Docker, run `docker-compose up`.
5. In Airflow Dashboard you will see a new DAGs - "weather_nyc_2023_2024". Activate it and run it. 
6. If the run is successful, check the database. There should be 2 tables - "forecast_requests" should have 
1 new entry after each run, and "weather_data" should have updated entries for the full years 2023 and 2024.
7. Similarly activate and run the DAG "citibike_etl_pipeline". The result should be a new table "bike_data".
This DAG takes a bit long to execute.
8. In your Postgres client run the query from "query.sql" file and dump the results into a csv file, 
or use the existing file "bike-weather-data.csv".
9. Run the Jupyter notebook "Bike_sharing_dataset_Advanced_regression.ipynb", 
placing it in the same folder with the csv file.