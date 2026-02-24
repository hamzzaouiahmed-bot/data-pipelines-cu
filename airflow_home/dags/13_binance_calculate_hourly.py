import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

BASE_DIR = Path("/home/ind10/new-project-airflow-ahmed/airflow_home/data/binance")


def _calculate_hourly_average(**context):
    interval_start = context["data_interval_start"]
    date_str = interval_start.strftime("%Y-%m-%d")
    hour_str = interval_start.strftime("%H")

    raw_file = BASE_DIR / "raw" / date_str / "daily_raw.csv"
    if not raw_file.exists():
        print(f"Missing raw file: {raw_file}")
        return

    df = pd.read_csv(raw_file)
    if df.empty:
        print("Raw file empty.")
        return

    df["fetch_time"] = pd.to_datetime(df["fetch_time"], errors="coerce")
    df = df.dropna(subset=["fetch_time"])
    df["hour"] = df["fetch_time"].dt.strftime("%H")

    hdf = df[df["hour"] == hour_str].copy()
    if hdf.empty:
        print(f"No data for hour {hour_str}")
        return

    hdf = hdf.sort_values("fetch_time")

    hourly = {
        "date": date_str,
        "hour": hour_str,
        "avg_price": float(hdf["price_float"].mean()),
        "min_price": float(hdf["price_float"].min()),
        "max_price": float(hdf["price_float"].max()),
        "first_price": float(hdf["price_float"].iloc[0]),
        "last_price": float(hdf["price_float"].iloc[-1]),
        "data_points": int(len(hdf)),
        "calculated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_dir = BASE_DIR / "hourly" / date_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "hourly_avg.csv"

    new_row = pd.DataFrame([hourly])

    if out_file.exists():
        existing = pd.read_csv(out_file)
        existing["hour"] = existing["hour"].astype(str)
        existing = existing[existing["hour"] != hour_str]
        new_row = pd.concat([existing, new_row], ignore_index=True)

    new_row = new_row.sort_values(["date", "hour"])
    new_row.to_csv(out_file, index=False)

    print(f"Saved hourly: {out_file}")


with DAG(
    dag_id="binance_calculate_hourly",
    schedule=dt.timedelta(hours=1),
    start_date=dt.datetime(2026, 2, 23),
    catchup=False,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=5)},
    tags=["binance", "btc", "hourly"],
) as dag:

    PythonOperator(
        task_id="calculate_hourly_average",
        python_callable=_calculate_hourly_average,
    )