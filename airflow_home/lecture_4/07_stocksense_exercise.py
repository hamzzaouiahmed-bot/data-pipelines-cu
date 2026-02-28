from pathlib import Path
import os
import csv
import sqlite3
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

PAGENAMES = {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", str(Path.home() / "airflow"))


BASE_DIR = Path(AIRFLOW_HOME) / "lecture_4" / "stocksense"
OUTPUT_DIR = str(BASE_DIR / "pageview_counts")
DB_PATH = str(BASE_DIR / "stocksense.db")


def _get_data(year, month, day, hour, output_path, **_):
    """Download Wikipedia pageviews for the given hour."""
    from urllib import request

    url = (
        f"https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{int(month):02d}/"
        f"pageviews-{year}{int(month):02d}{int(day):02d}-{int(hour):02d}0000.gz"
    )
    print(f"Downloading {url}")
    request.urlretrieve(url, output_path)


def _fetch_pageviews(pagenames, logical_date=None, **context):
    """Extract counts for tracked companies and save to CSV."""
    result = dict.fromkeys(pagenames, 0)

    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 4:
                domain_code, page_title, view_count = parts[0], parts[1], parts[2]
                if domain_code == "en" and page_title in pagenames:
                    result[page_title] = int(view_count)

    output_path = context["templates_dict"]["output_path"]
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pagename", "pageviewcount", "datetime"])
        for pagename, count in result.items():
            writer.writerow([pagename, count, str(logical_date)])

    print(f"Saved pageview counts to {output_path}")
    print(f"Counts: {result}")
    return result


def _add_to_db(**context):
    """Insert CSV data into SQLite database."""
    output_path = context["templates_dict"]["output_path"]
    csv_path = Path(output_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pageviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pagename TEXT NOT NULL,
            pageviewcount INTEGER NOT NULL,
            datetime TEXT NOT NULL
        )
        """
    )

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = [(r["pagename"], int(r["pageviewcount"]), r["datetime"]) for r in reader]

    cur.executemany(
        "INSERT INTO pageviews (pagename, pageviewcount, datetime) VALUES (?, ?, ?)",
        rows,
    )

    conn.commit()
    conn.close()

    print(f"Inserted {len(rows)} rows into database: {DB_PATH}")


with DAG(
    dag_id="lecture4_stocksense_exercise",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["lecture4", "exercise", "stocksense", "etl"],
) as dag:

    get_data = PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
            
            "year": "{{ (logical_date - macros.timedelta(hours=2)).year }}",
            "month": "{{ (logical_date - macros.timedelta(hours=2)).month }}",
            "day": "{{ (logical_date - macros.timedelta(hours=2)).day }}",
            "hour": "{{ (logical_date - macros.timedelta(hours=2)).hour }}",
            "output_path": "/tmp/wikipageviews.gz",
        },
    )

    extract_gz = BashOperator(
        task_id="extract_gz",
        bash_command="gunzip -f /tmp/wikipageviews.gz",
    )

    fetch_pageviews = PythonOperator(
        task_id="fetch_pageviews",
        python_callable=_fetch_pageviews,
        op_kwargs={"pagenames": PAGENAMES},
        templates_dict={"output_path": f"{OUTPUT_DIR}/{{{{ ts_nodash }}}}.csv"},
    )

    add_to_db = PythonOperator(
        task_id="add_to_db",
        python_callable=_add_to_db,
        templates_dict={"output_path": f"{OUTPUT_DIR}/{{{{ ts_nodash }}}}.csv"},
    )

    get_data >> extract_gz >> fetch_pageviews >> add_to_db