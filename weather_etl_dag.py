from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from weather_etl_functions import extract, transform, load

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for European weather data using OpenWeatherMap API",
    schedule_interval="@daily",  # Run once a day
    catchup=False,
)

cities = ["London", "Paris", "Berlin", "Madrid", "Rome", "Tunis", "Vienna"]

def run_etl():
    for city in cities:
        raw_data = extract(city)
        df = transform(raw_data)
        load(df)
        print(f"{city} processed.")

run_etl_task = PythonOperator(
    task_id="run_etl_task",
    python_callable=run_etl,
    dag=dag,
)

run_etl_task
