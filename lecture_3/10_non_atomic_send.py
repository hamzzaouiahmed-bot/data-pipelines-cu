import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

EVENTS_DIR = "/tmp/events"
STATS_DIR = "/tmp/stats"
EVENTS_URL = "http://127.0.0.1:5003/events"


def _email_stats(stats: pd.DataFrame, email: str):
    """Send an email... (demo)"""
    print(f"Sending stats to {email}...")
    


def _calculate_stats(**context):
    """Calculates event statistics AND sends an email (non-atomic anti-pattern)."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="count")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)

    
    _email_stats(stats, email="user@example.com")


with DAG(
    dag_id="10_non_atomic_send",
    schedule="@daily",
    start_date=dt.datetime(2019, 1, 1),
    end_date=dt.datetime(2019, 1, 5),
    catchup=True, 
) as dag:

    fetch_events = BashOperator(
        task_id="fetch_events",
        bash_command=(
            f"mkdir -p {EVENTS_DIR} && "
            f"curl -sf -o {EVENTS_DIR}/{{{{ ds }}}}.json "
            f"'{EVENTS_URL}?start_date={{{{ ds }}}}&"
            "end_date={{ data_interval_end.strftime('%Y-%m-%d') }}'"
        ),
    )

    calculate_stats = PythonOperator(
        task_id="calculate_stats",
        python_callable=_calculate_stats,
        templates_dict={
            "input_path": f"{EVENTS_DIR}/{{{{ ds }}}}.json",
            "output_path": f"{STATS_DIR}/{{{{ ds }}}}.csv",
        },
    )

    fetch_events >> calculate_stats