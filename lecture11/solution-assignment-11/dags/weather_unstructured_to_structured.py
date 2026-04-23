from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import requests
import json
import os

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434/api/chat")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:7b")

with DAG(
    dag_id="weather_unstructured_to_structured",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lecture11", "airflow", "ollama", "weather"],
) as dag:

    @task
    def fetch_weather():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 36.75,
            "longitude": 3.06,
            "current": "temperature_2m,wind_speed_10m,weather_code",
            "timezone": "auto",
        }
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.text

    @task
    def ollama_to_structured(raw_data: str):
        mock_mode = os.getenv("WEATHER_PIPELINES_MOCK_OLLAMA", "0") == "1"

        if mock_mode:
            raw = json.loads(raw_data)
            current = raw.get("current", {})
            result = {
                "source": "open-meteo",
                "latitude": raw.get("latitude"),
                "longitude": raw.get("longitude"),
                "temperature_c": current.get("temperature_2m"),
                "wind_speed_kph": current.get("wind_speed_10m"),
                "weather_code": current.get("weather_code"),
                "conditions_short": "mock-weather",
                "observation_time": current.get("time"),
            }
            return json.dumps(result)

        prompt = f"""
Convert the following weather payload into ONE flat JSON object.
Return ONLY valid JSON.
No markdown.
No explanation.

Required keys:
- source
- latitude
- longitude
- temperature_c
- wind_speed_kph
- weather_code
- conditions_short
- observation_time

Rules:
- source must be "open-meteo"
- conditions_short must be a short human-readable weather summary
- keep numeric values as numbers
- if missing, use null

Raw payload:
{raw_data}
""".strip()

        payload = {
            "model": OLLAMA_MODEL,
            "messages": [
                {"role": "system", "content": "Return only strict JSON."},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
            "format": "json",
        }

        response = requests.post(OLLAMA_URL, json=payload, timeout=180)
        response.raise_for_status()
        result = response.json()["message"]["content"]

        json.loads(result)
        return result

    @task
    def validate_and_emit(structured_json: str):
        data = json.loads(structured_json)

        required_keys = [
            "source",
            "latitude",
            "longitude",
            "temperature_c",
            "wind_speed_kph",
            "weather_code",
            "conditions_short",
            "observation_time",
        ]

        for key in required_keys:
            if key not in data:
                raise ValueError(f"Missing key: {key}")
            if data[key] is None:
                raise ValueError(f"Null value found for key: {key}")

        print(json.dumps(data, indent=2))
        return data

    raw = fetch_weather()
    structured = ollama_to_structured(raw)
    validate_and_emit(structured)