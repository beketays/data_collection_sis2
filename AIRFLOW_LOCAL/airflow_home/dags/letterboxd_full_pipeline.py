"""
Letterboxd ETL Pipeline DAG
Scraping -> Cleaning -> Loading into SQLite using separate modules.
"""

from datetime import datetime, timedelta
import logging
import os
import sys
import asyncio

from airflow import DAG
from airflow.operators.python import PythonOperator

# ==== Пути проекта ====
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.join(CURRENT_DIR, "..")  # это папка airflow_home

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

RAW_JSON = os.path.join(DATA_DIR, "letterboxd_movie_data.json")
CSV_FILE = os.path.join(DATA_DIR, "movies.csv")
DB_FILE = os.path.join(DATA_DIR, "movies.db")

# scraper.py: должен содержать async def scrape_letterboxd(output_path: str) или без аргументов
from src.scraper import scrape_letterboxd
# cleaner.py: функции stars_to_rating, load_json, process_data(file_path, output_path)
from src.cleaner import process_data
# loader.py: create_sqlite_table(csv_file, db_file)
from src.loader import create_sqlite_table

logger = logging.getLogger(__name__)


# ==== Обёртки под Airflow tasks ====

def scrape_task_callable(**context):
    """
    Запускает асинхронный парсер Letterboxd и сохраняет JSON.
    Предполагаем, что scrape_letterboxd принимает путь к json-файлу.
    Если у тебя в scraper.py функция без аргументов — просто убери RAW_JSON.
    """
    logger.info("Starting Letterboxd scraping task...")
    try:
        # Вариант 1: если ты изменил функцию так:
        # async def scrape_letterboxd(output_path: str)
        asyncio.run(scrape_letterboxd(RAW_JSON))

        # Вариант 2 (если у функции нет параметров и она сама пишет в RAW_JSON):
        # asyncio.run(scrape_letterboxd())

        logger.info(f"Scraping finished, JSON saved to {RAW_JSON}")
    except Exception as e:
        logger.exception("Error while scraping Letterboxd")
        raise e


def clean_task_callable(**context):
    """
    Чистка и преобразование JSON -> CSV.
    Использует твою process_data(file_path, output_path).
    """
    logger.info(f"Starting cleaning: {RAW_JSON} -> {CSV_FILE}")
    try:
        process_data(RAW_JSON, CSV_FILE)
        logger.info(f"Cleaning finished, CSV saved to {CSV_FILE}")
    except Exception as e:
        logger.exception("Error while processing data")
        raise e


def load_task_callable(**context):
    """
    Загрузка CSV в SQLite.
    Использует create_sqlite_table(csv_file, db_file).
    """
    logger.info(f"Starting load to SQLite: {CSV_FILE} -> {DB_FILE}")
    try:
        create_sqlite_table(CSV_FILE, DB_FILE)
        logger.info(f"SQLite DB created/updated at {DB_FILE}")
    except Exception as e:
        logger.exception("Error while loading data to SQLite")
        raise e


# ==== Аргументы DAG ====

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="letterboxd_etl_pipeline",
    description="Daily ETL: scrape Letterboxd, clean data, load to SQLite",
    default_args=default_args,
    start_date=datetime(2025, 12, 5),
    schedule_interval="@daily",  # не чаще одного раза в день
    catchup=False,
    max_active_runs=1,
    tags=["letterboxd", "etl"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_letterboxd",
        python_callable=scrape_task_callable,
    )

    clean_task = PythonOperator(
        task_id="clean_letterboxd_data",
        python_callable=clean_task_callable,
    )

    load_task = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=load_task_callable,
    )

    # pipeline: scraping → cleaning → loading
    scrape_task >> clean_task >> load_task
