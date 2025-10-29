from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timezone
import requests
import uuid

# Constants
POSTGRES_CONN_ID = "hcvt_db_conn"
API_URL = "https://archive-api.open-meteo.com/v1/archive"
LOCATION = {
    "New York City": {"lat": 40.7128, "lon": -74.0060}
}
PARAMS = {
    "hourly": [
        "temperature_2m",
        "apparent_temperature",
        "windspeed_10m",
        "precipitation",
        "relative_humidity_2m",
    ],
    "timezone": "America/New_York",
    "start_date": "2023-01-01",
    "end_date": "2024-12-31",
}

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
}


def create_tables():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS forecast_requests (
                    request_id UUID PRIMARY KEY,
                    request_time TIMESTAMP,
                    city TEXT,
                    latitude FLOAT,
                    longitude FLOAT
                )
            """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS weather_data (
                    id TEXT PRIMARY KEY,
                    request_id UUID REFERENCES forecast_requests(request_id),
                    timestamp TIMESTAMP,
                    temperature FLOAT,
                    apparent_temperature FLOAT,
                    windspeed FLOAT,
                    precipitation FLOAT,
                    humidity FLOAT
                )
            """
            )
            conn.commit()


def fetch_and_store_weather():
    request_time = datetime.now(timezone.utc)
    all_metadata = []
    all_data = []

    for city, coords in LOCATION.items():
        request_id = str(uuid.uuid4())

        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "hourly": PARAMS["hourly"],
            "timezone": PARAMS["timezone"],
            "start_date": PARAMS["start_date"],
            "end_date": PARAMS["end_date"],
        }

        print(f"Fetching historical data for {city}")
        response = requests.get(API_URL, params=params)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

        data = response.json()
        timestamps = data["hourly"]["time"]
        temperatures = data["hourly"]["temperature_2m"]
        apparent_temperatures = data["hourly"]["apparent_temperature"]
        windspeeds = data["hourly"]["windspeed_10m"]
        precipitations = data["hourly"]["precipitation"]
        humidity = data["hourly"]["relative_humidity_2m"]

        all_metadata.append(
            (request_id, request_time, city, coords["lat"], coords["lon"])
        )

        for i in range(len(timestamps)):
            timestamp = datetime.fromisoformat(timestamps[i])
            row_id = f"{timestamp.year}-{timestamp.month}-{timestamp.day}-{timestamp.hour}"
            all_data.append(
                (
                    row_id,
                    request_id,
                    timestamp,
                    temperatures[i],
                    apparent_temperatures[i],
                    windspeeds[i],
                    precipitations[i],
                    humidity[i],
                )
            )

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO forecast_requests (request_id, request_time, city, latitude, longitude)
                VALUES (%s, %s, %s, %s, %s)
            """,
                all_metadata,
            )
            cur.executemany(
                """
                INSERT INTO weather_data (id, request_id, timestamp, temperature, apparent_temperature, windspeed, precipitation, humidity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    request_id = EXCLUDED.request_id,
                    timestamp = EXCLUDED.timestamp,
                    temperature = EXCLUDED.temperature,
                    apparent_temperature = EXCLUDED.apparent_temperature,
                    windspeed = EXCLUDED.windspeed,
                    precipitation = EXCLUDED.precipitation,
                    humidity = EXCLUDED.humidity
            """,
                all_data,
            )
            conn.commit()


with DAG(
    dag_id="weather_nyc_2023_2024",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["weather", "nyc"],
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    fetch_and_store_task = PythonOperator(
        task_id="fetch_and_store_weather",
        python_callable=fetch_and_store_weather,
    )

    create_tables_task >> fetch_and_store_task
