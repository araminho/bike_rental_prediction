import os
import zipfile
import requests
import logging
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(DAG_DIR)
# set EXTRACT_DIR to container path that is mounted to host ./citibike_data
EXTRACT_DIR = "/opt/airflow/citibike_data"
ZIP_2023_PATH = os.path.join(EXTRACT_DIR, "2023-citibike-tripdata.zip")
ZIP_2023_URL = "https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_bike_data_table():
    hook = PostgresHook(postgres_conn_id="bike_db_conn")
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS bike_data (
                    id TEXT PRIMARY KEY,
                    season INTEGER,
                    yr INTEGER,
                    mnth INTEGER,
                    hr INTEGER,
                    day INTEGER,
                    weekday INTEGER,
                    classic_bike INTEGER,
                    electric_bike INTEGER,
                    casual INTEGER,
                    registered INTEGER,
                    cnt INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            )
            conn.commit()


def process_rows_batch(rows, hook):
    """Helper function to process a batch of rows"""
    connection = hook.get_conn()
    cursor = connection.cursor()

    try:
        for _, row in rows.iterrows():
            cursor.execute(
                """
                INSERT INTO bike_data (id, season, yr, mnth, hr, day, weekday, 
                                     classic_bike, electric_bike, casual, registered, cnt)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    season = EXCLUDED.season,
                    yr = EXCLUDED.yr,
                    mnth = EXCLUDED.mnth,
                    hr = EXCLUDED.hr,
                    day = EXCLUDED.day,
                    weekday = EXCLUDED.weekday,
                    classic_bike = EXCLUDED.classic_bike,
                    electric_bike = EXCLUDED.electric_bike,
                    casual = EXCLUDED.casual,
                    registered = EXCLUDED.registered,
                    cnt = EXCLUDED.cnt,
                    created_at = CURRENT_TIMESTAMP;
                """,
                (
                    row["id"],
                    row["season"],
                    row["yr"],
                    row["mnth"],
                    row["hr"],
                    row["day"],
                    row["weekday"],
                    row["classic_bike"],
                    row["electric_bike"],
                    row["casual"],
                    row["registered"],
                    row["cnt"],
                ),
            )
        connection.commit()
    finally:
        cursor.close()
        connection.close()


def extract_and_process_2023():
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)

    inner_dir = os.path.join(EXTRACT_DIR, "2023-citibike-tripdata")

    # Download and extract main 2023 archive only if inner directory doesn't exist
    if not os.path.exists(inner_dir):
        if not os.path.exists(ZIP_2023_PATH):
            logger.info(f"Downloading 2023 zip archive from {ZIP_2023_URL}")
            response = requests.get(ZIP_2023_URL, stream=True)
            with open(ZIP_2023_PATH, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        logger.info(f"Extracting {ZIP_2023_PATH}")
        os.makedirs(inner_dir, exist_ok=True)
        with zipfile.ZipFile(ZIP_2023_PATH, "r") as zip_ref:
            members = [m for m in zip_ref.namelist() if m.endswith(".zip")]
            for i, member in enumerate(members, start=1):
                logger.info(f"Extracting member {i}/{len(members)}: {member}")
                try:
                    zip_ref.extract(member, inner_dir)
                except Exception as e:
                    logger.warning(f"Failed extracting {member}: {e}")
                    continue

    # Process from local files
    zip_files = [f for f in os.listdir(inner_dir) if f.endswith(".zip")]
    logger.info(f"Found {len(zip_files)} monthly zip files in {inner_dir}")

    for monthly_zip in zip_files:
        full_zip_path = os.path.join(inner_dir, monthly_zip)
        with zipfile.ZipFile(full_zip_path, "r") as month_zip:
            for csv_file in month_zip.namelist():
                if csv_file.endswith(".csv"):
                    logger.info(f"Processing {csv_file} from {monthly_zip}")
                    with month_zip.open(csv_file) as f:
                        df = pd.read_csv(f, parse_dates=["ended_at"])

                        logger.info(f"Read {len(df)} rows")

                        df = df[["ended_at", "rideable_type", "member_casual"]]

                        df["yr"] = df["ended_at"].dt.year
                        df["mnth"] = df["ended_at"].dt.month
                        df["day"] = df["ended_at"].dt.day
                        df["hr"] = df["ended_at"].dt.hour
                        df["weekday"] = df["ended_at"].dt.weekday
                        df["season"] = (df["mnth"] % 12 + 3) // 3

                        df["classic_bike"] = df["rideable_type"].apply(
                            lambda x: 1 if x == "classic_bike" else 0
                        )
                        df["electric_bike"] = df["rideable_type"].apply(
                            lambda x: 1 if x == "electric_bike" else 0
                        )
                        df["casual"] = df["member_casual"].apply(
                            lambda x: 1 if x == "casual" else 0
                        )
                        df["registered"] = df["member_casual"].apply(
                            lambda x: 1 if x == "member" else 0
                        )

                        grouped = (
                            df.groupby(["season", "yr", "mnth", "day", "hr", "weekday"])
                            .agg(
                                {
                                    "classic_bike": "sum",
                                    "electric_bike": "sum",
                                    "casual": "sum",
                                    "registered": "sum",
                                }
                            )
                            .reset_index()
                        )

                        grouped["cnt"] = grouped["casual"] + grouped["registered"]
                        grouped["id"] = grouped.apply(
                            lambda row: f"{row['yr']}-{row['mnth']}-{row['day']}-{row['hr']}",
                            axis=1,
                        )
                        grouped = grouped.sort_values(by=["yr", "mnth", "day", "hr"])

                        logger.info(f"Grouped to {len(grouped)} hourly records")
                        hook = PostgresHook(postgres_conn_id="bike_db_conn")
                        process_rows_batch(grouped, hook)


def extract_and_process_2024():
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)

    urls_2024 = [
        f"https://s3.amazonaws.com/tripdata/2024{str(month).zfill(2)}-citibike-tripdata.zip"
        for month in range(1, 13)
    ]

    # Only download missing zip files
    for url in urls_2024:
        filename = url.split("/")[-1]
        zip_path = os.path.join(EXTRACT_DIR, filename)
        csv_name = filename.replace(".zip", ".csv")
        csv_path = os.path.join(EXTRACT_DIR, csv_name)

        # Skip if CSV already exists
        if os.path.exists(csv_path):
            continue

        # Download and extract only if needed
        if not os.path.exists(zip_path):
            logger.info(f"Downloading {filename}")
            response = requests.get(url, stream=True)
            with open(zip_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

        logger.info(f"Extracting {filename}")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            members = [m for m in zip_ref.namelist() if m.endswith(".csv")]
            for i, member in enumerate(members, start=1):
                logger.info(f"Extracting member {i}/{len(members)}: {member}")
                try:
                    zip_ref.extract(member, EXTRACT_DIR)
                except Exception as e:
                    logger.warning(f"Failed extracting {member}: {e}")
                    continue

    # Process from local files
    extracted_files = [f for f in os.listdir(EXTRACT_DIR) if f.endswith(".csv")]
    logger.info(f"Found {len(extracted_files)} CSV files in {EXTRACT_DIR}")

    for csv_file in extracted_files:
        csv_path = os.path.join(EXTRACT_DIR, csv_file)
        logger.info(f"Processing {csv_file}")
        df = pd.read_csv(csv_path, parse_dates=["ended_at"])

        logger.info(f"Read {len(df)} rows")

        df = df[["ended_at", "rideable_type", "member_casual"]]

        df["yr"] = df["ended_at"].dt.year
        df["mnth"] = df["ended_at"].dt.month
        df["day"] = df["ended_at"].dt.day
        df["hr"] = df["ended_at"].dt.hour
        df["weekday"] = df["ended_at"].dt.weekday
        df["season"] = (df["mnth"] % 12 + 3) // 3

        df["classic_bike"] = df["rideable_type"].apply(
            lambda x: 1 if x == "classic_bike" else 0
        )
        df["electric_bike"] = df["rideable_type"].apply(
            lambda x: 1 if x == "electric_bike" else 0
        )
        df["casual"] = df["member_casual"].apply(lambda x: 1 if x == "casual" else 0)
        df["registered"] = df["member_casual"].apply(
            lambda x: 1 if x == "member" else 0
        )

        grouped = (
            df.groupby(["season", "yr", "mnth", "day", "hr", "weekday"])
            .agg(
                {
                    "classic_bike": "sum",
                    "electric_bike": "sum",
                    "casual": "sum",
                    "registered": "sum",
                }
            )
            .reset_index()
        )

        grouped["cnt"] = grouped["casual"] + grouped["registered"]
        grouped["id"] = grouped.apply(
            lambda row: f"{row['yr']}-{row['mnth']}-{row['day']}-{row['hr']}", axis=1
        )
        grouped = grouped.sort_values(by=["yr", "mnth", "day", "hr"])

        logger.info(f"Grouped to {len(grouped)} hourly records")
        hook = PostgresHook(postgres_conn_id="bike_db_conn")
        process_rows_batch(grouped, hook)


def cleanup_files():
    """Remove all downloaded and extracted files"""
    import shutil

    logger.info(f"Cleaning up files in {EXTRACT_DIR}")
    if os.path.exists(EXTRACT_DIR):
        # Remove the main 2023 directory with all its contents
        inner_dir = os.path.join(EXTRACT_DIR, "2023-citibike-tripdata")
        if os.path.exists(inner_dir):
            shutil.rmtree(inner_dir)

        # Remove the main 2023 zip file
        if os.path.exists(ZIP_2023_PATH):
            os.remove(ZIP_2023_PATH)

        # Remove all 2024 files
        for file in os.listdir(EXTRACT_DIR):
            if file.startswith("2024") and (
                file.endswith(".zip") or file.endswith(".csv")
            ):
                file_path = os.path.join(EXTRACT_DIR, file)
                os.remove(file_path)

        # Remove the extract directory if empty
        if not os.listdir(EXTRACT_DIR):
            os.rmdir(EXTRACT_DIR)

    logger.info("Cleanup completed")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
    "execution_timeout": timedelta(minutes=120),
}

dag = DAG(
    dag_id="citibike_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

with dag:
    create_table = PythonOperator(
        task_id="create_bike_data_table", python_callable=create_bike_data_table
    )

    process_2023 = PythonOperator(
        task_id="process_2023_data", python_callable=extract_and_process_2023
    )

    process_2024 = PythonOperator(
        task_id="process_2024_data", python_callable=extract_and_process_2024
    )

    cleanup = PythonOperator(task_id="cleanup_files", python_callable=cleanup_files)

    # Set dependencies
    create_table >> process_2023 >> process_2024 >> cleanup
