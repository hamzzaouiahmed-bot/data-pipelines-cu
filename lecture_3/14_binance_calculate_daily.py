import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

BASE_DIR = Path("/home/ind10/new-project-airflow-ahmed/airflow_home/data/binance")


def _calculate_daily_average(**context):
    interval_start = context["data_interval_start"]
    date_str = interval_start.strftime("%Y-%m-%d")

    hourly_file = BASE_DIR / "hourly" / date_str / "hourly_avg.csv"
    if not hourly_file.exists():
        print(f"Missing hourly file: {hourly_file}")
        return

    df = pd.read_csv(hourly_file)
    if df.empty:
        print("Hourly file empty.")
        return

    df = df.sort_values(["date", "hour"])

    opening_price = float(df["first_price"].iloc[0])
    closing_price = float(df["last_price"].iloc[-1])

    daily = {
        "date": date_str,
        "avg_price": float(df["avg_price"].mean()),
        "min_price": float(df["min_price"].min()),
        "max_price": float(df["max_price"].max()),
        "opening_price": opening_price,
        "closing_price": closing_price,
        "price_change": closing_price - opening_price,
        "price_change_pct": ((closing_price - opening_price) / opening_price * 100.0)
        if opening_price
        else 0.0,
        "total_data_points": int(df["data_points"].sum()),
        "hours_with_data": int(len(df)),
        "calculated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    out_dir = BASE_DIR / "daily"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "daily_avg.csv"

    new_row = pd.DataFrame([daily])

    if out_file.exists():
        existing = pd.read_csv(out_file)
        existing = existing[existing["date"] != date_str]
        new_row = pd.concat([existing, new_row], ignore_index=True)

    new_row = new_row.sort_values("date")
    new_row.to_csv(out_file, index=False)

    print(f"Saved daily: {out_file}")


with DAG(
    dag_id="binance_calculate_daily",
    schedule="@daily",
    start_date=dt.datetime(2026, 2, 23),
    catchup=False,
    default_args={"retries": 2, "retry_delay": dt.timedelta(minutes=10)},
    tags=["binance", "btc", "daily"],
) as dag:

    PythonOperator(
        task_id="calculate_daily_average",
        python_callable=_calculate_daily_average,
    )