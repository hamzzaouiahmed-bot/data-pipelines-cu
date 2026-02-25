from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

EVENTS_PATH = "/tmp/events/events.json"
STATS_PATH = "/tmp/stats/stats.csv"
EVENTS_URL = "http://localhost:5003/events"

with DAG(
    dag_id="02_daily_schedule",
    schedule="@daily",                 
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2019, 1, 5),
    catchup=True,
) as dag:

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            f"mkdir -p {Path(EVENTS_PATH).parent} && "
            f"curl -sf -o {EVENTS_PATH} '{EVENTS_URL}'"
        ),
    )

    def _calculate_stats(input_path, output_path):
        events = pd.read_json(input_path)
        stats = events.groupby(["date", "user"]).size().reset_index(name="count")

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        stats.to_csv(output_path, index=False)

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        op_kwargs={"input_path": EVENTS_PATH, "output_path": STATS_PATH},
    )

    fetch_events >> calculate_stats