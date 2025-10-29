import os
import zipfile
import requests
import logging
from datetime import datetime
import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

EXTRACT_DIR = os.path.join(os.environ.get("TEMP", "/tmp"), "citibike_data")
ZIP_2023_PATH = os.path.join(EXTRACT_DIR, "2023-citibike-tripdata.zip")
ZIP_2023_URL = "https://s3.amazonaws.com/tripdata/2023-citibike-tripdata.zip"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_postgres_connection():
    conn_info = BaseHook.get_connection("bike_db_conn")
    return psycopg2.connect(
        dbname=conn_info.schema,
        user=conn_info.login,
        password=conn_info.password,
        host=conn_info.host,
        port=conn_info.port,
    )


def create_bike_data_table():
    conn = get_postgres_connection()
    cur = conn.cursor()
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
    cur.close()
    conn.close()


def extract_and_process_2023():
    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)

    if not os.path.exists(ZIP_2023_PATH):
        logger.info(f"Downloading 2023 zip archive from {ZIP_2023_URL}")
        response = requests.get(ZIP_2023_URL)
        with open(ZIP_2023_PATH, "wb") as f:
            f.write(response.content)

    logger.info(f"Extracting {ZIP_2023_PATH}")
    with zipfile.ZipFile(ZIP_2023_PATH, "r") as zip_ref:
        zip_ref.extractall(EXTRACT_DIR)

    inner_dir = os.path.join(EXTRACT_DIR, "2023-citibike-tripdata")
    zip_files = [f for f in os.listdir(inner_dir) if f.endswith(".zip")]
    logger.info(f"Found {len(zip_files)} monthly zip files inside 2023 archive")

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

                        conn = get_postgres_connection()
                        cur = conn.cursor()

                        for _, row in grouped.iterrows():
                            cur.execute(
                                """
                                INSERT INTO bike_data (id, season, yr, mnth, hr, day, weekday, classic_bike, electric_bike, casual, registered, cnt)
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
                        conn.commit()
                        cur.close()
                        conn.close()


def extract_and_process_2024():
    urls_2024 = [
        f"https://s3.amazonaws.com/tripdata/2024{str(month).zfill(2)}-citibike-tripdata.zip"
        for month in range(1, 13)
    ]

    if not os.path.exists(EXTRACT_DIR):
        os.makedirs(EXTRACT_DIR)

    for url in urls_2024:
        filename = url.split("/")[-1]
        zip_path = os.path.join(EXTRACT_DIR, filename)

        if not os.path.exists(zip_path):
            logger.info(f"Downloading {filename}")
            response = requests.get(url)
            with open(zip_path, "wb") as f:
                f.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(EXTRACT_DIR)

    extracted_files = [f for f in os.listdir(EXTRACT_DIR) if f.endswith(".csv")]
    logger.info(f"Extracted {len(extracted_files)} CSV files from 2024 zips")

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

        conn = get_postgres_connection()
        cur = conn.cursor()

        for _, row in grouped.iterrows():
            cur.execute(
                """
                INSERT INTO bike_data (id, season, yr, mnth, hr, day, weekday, classic_bike, electric_bike, casual, registered, cnt)
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
        conn.commit()
        cur.close()
        conn.close()


def download_and_process():
    create_bike_data_table()
    extract_and_process_2023()
    # extract_and_process_2024()  # Uncomment when needed


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

dag = DAG(
    dag_id="citibike_etl_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

with dag:
    process_task = PythonOperator(
        task_id="download_and_process_citibike", python_callable=download_and_process
    )
