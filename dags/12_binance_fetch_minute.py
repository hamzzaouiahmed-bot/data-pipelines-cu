import datetime as dt
from pathlib import Path

import pandas as pd
import requests
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

BASE_DIR = Path("/home/ind10/new-project-airflow-ahmed/airflow_home/data/binance")
API_URL = "https://api.binance.com/api/v3/avgPrice?symbol=BTCUSDT"


def _fetch_binance_price(**context):
    interval_start = context["data_interval_start"]
    date_str = interval_start.strftime("%Y-%m-%d")
    hour_str = interval_start.strftime("%H")
    minute_str = interval_start.strftime("%M")

    out_dir = BASE_DIR / "raw" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)

    resp = requests.get(API_URL, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    now = dt.datetime.now()
    row = {
        "mins": data.get("mins"),
        "price": data.get("price"),
        "closeTime": data.get("closeTime"),
        "timestamp": interval_start.isoformat(),
        "fetch_time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "price_float": float(data["price"]),
    }
    df = pd.DataFrame([row])

    minute_file = out_dir / f"price_{hour_str}_{minute_str}.csv"
    df.to_csv(minute_file, index=False)

    daily_file = out_dir / "daily_raw.csv"
    if daily_file.exists():
        existing = pd.read_csv(daily_file)
        merged = pd.concat([existing, df], ignore_index=True)
        merged = merged.drop_duplicates(subset=["fetch_time"])
        merged.to_csv(daily_file, index=False)
    else:
        df.to_csv(daily_file, index=False)

    print(f"Saved: {minute_file}")
    print(f"Updated: {daily_file}")


with DAG(
    dag_id="binance_fetch_minute",
    schedule=dt.timedelta(minutes=1),
    start_date=dt.datetime(2026, 2, 23),
    catchup=False,
    default_args={"retries": 3, "retry_delay": dt.timedelta(minutes=1)},
    tags=["binance", "btc", "minute"],
) as dag:

    PythonOperator(
        task_id="fetch_binance_price",
        python_callable=_fetch_binance_price,
    )