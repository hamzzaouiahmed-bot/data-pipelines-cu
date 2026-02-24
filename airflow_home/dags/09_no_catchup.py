import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

EVENTS_DIR = "/tmp/events"
STATS_DIR = "/tmp/stats"
EVENTS_URL = "http://127.0.0.1:5003/events"


def _calculate_stats(ds: str, **context):
    input_path = Path(EVENTS_DIR) / f"{ds}.json"
    output_path = Path(STATS_DIR) / f"{ds}.csv"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="count")
    stats.to_csv(output_path, index=False)


with DAG(
    dag_id="09_no_catchup",
    schedule="@daily",
    start_date=dt.datetime(2026, 2, 20),  
    catchup=False,  
) as dag:

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            "set -euo pipefail; "
            f"mkdir -p {EVENTS_DIR} && "
            f"curl -sf -o {EVENTS_DIR}/{{{{ ds }}}}.json "
            f"'{EVENTS_URL}?start_date={{{{ ds }}}}&"
            "end_date={{ data_interval_end.strftime('%Y-%m-%d') }}'"
        ),
    )

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
    )

    fetch_events >> calculate_stats