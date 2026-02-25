import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator


EVENTS_DIR = "/tmp/events"
STATS_DIR = "/tmp/stats"


EVENTS_URL = "http://127.0.0.1:5003/events"


def _calculate_stats(**context):
    """Calculates event statistics (ONLY) and saves them."""
    input_path = context["templates_dict"]["input_path"]
    output_path = context["templates_dict"]["output_path"]

    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index(name="count")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    stats.to_csv(output_path, index=False)


def email_stats(stats: pd.DataFrame, email: str):
    """Send an email... (demo)"""
    print(f"Sending stats to {email}...")


def _send_stats(email: str, **context):
    """Reads the saved CSV and sends the email (separate atomic task)."""
    stats_path = context["templates_dict"]["stats_path"]
    stats = pd.read_csv(stats_path)
    email_stats(stats, email=email)


with DAG(
    dag_id="11_atomic_send",
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

    send_stats = PythonOperator(
        task_id="send_stats",
        python_callable=_send_stats,
        op_kwargs={"email": "user@example.com"},
        templates_dict={
            "stats_path": f"{STATS_DIR}/{{{{ ds }}}}.csv",
        },
    )

 
    fetch_events >> calculate_stats >> send_stats