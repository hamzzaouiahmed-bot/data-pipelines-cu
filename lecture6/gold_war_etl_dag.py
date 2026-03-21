from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import yfinance as yf
import feedparser
from textblob import TextBlob
import os
import pickle

BASE_DIR = "/home/ind10/new-project-airflow-ahmed/Solution_lecture6"

DATA_DIR = f"{BASE_DIR}/data"
MODEL_DIR = f"{BASE_DIR}/models"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)


def fetch_gold_prices():
    df = yf.download("GC=F", start="2024-01-01")
    df.reset_index(inplace=True)
    df.rename(columns=str.lower, inplace=True)
    df.to_csv(f"{DATA_DIR}/gold_prices.csv", index=False)


def fetch_war_news():
    urls = [
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
        "https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml",
    ]

    keywords = ["war", "conflict", "attack", "military", "invasion"]

    rows = []

    for url in urls:
        feed = feedparser.parse(url)

        for entry in feed.entries:
            text = (entry.title + entry.summary).lower()

            if any(k in text for k in keywords):
                rows.append({
                    "date": entry.published[:10],
                    "title": entry.title,
                    "summary": entry.summary
                })

    df = pd.DataFrame(rows)
    df.to_csv(f"{DATA_DIR}/war_news.csv", index=False)


def compute_sentiment_and_merge():
    gold = pd.read_csv(f"{DATA_DIR}/gold_prices.csv")
    news = pd.read_csv(f"{DATA_DIR}/war_news.csv")

    news["sentiment"] = news["summary"].apply(
        lambda x: TextBlob(str(x)).sentiment.polarity
    )

    news_agg = news.groupby("date").agg(
        sentiment_mean=("sentiment", "mean"),
        news_count=("sentiment", "count")
    ).reset_index()

    df = pd.merge(gold, news_agg, on="date", how="left")

    df["sentiment_mean"] = df["sentiment_mean"].fillna(0)
    df["news_count"] = df["news_count"].fillna(0)

    df["target"] = (df["close"].shift(-1) > df["close"]).astype(int)
    df.dropna(inplace=True)

    df.to_csv(f"{DATA_DIR}/training_data.csv", index=False)


def train_model():
    from sklearn.ensemble import RandomForestClassifier

    df = pd.read_csv(f"{DATA_DIR}/training_data.csv")

    features = ["close", "sentiment_mean", "news_count"]

    X = df[features]
    y = df["target"]

    model = RandomForestClassifier()
    model.fit(X, y)

    with open(f"{MODEL_DIR}/gold_model.pkl", "wb") as f:
        pickle.dump({
            "model": model,
            "features": features
        }, f)


with DAG(
    "gold_war_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="fetch_gold_prices",
        python_callable=fetch_gold_prices,
    )

    t2 = PythonOperator(
        task_id="fetch_war_news",
        python_callable=fetch_war_news,
    )

    t3 = PythonOperator(
        task_id="compute_sentiment_and_merge",
        python_callable=compute_sentiment_and_merge,
    )

    t4 = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    [t1, t2] >> t3 >> t4
