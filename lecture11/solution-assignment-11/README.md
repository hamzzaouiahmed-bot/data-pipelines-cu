# Lecture 11 - Airflow + Ollama Assignment

## Description
This project implements an Airflow DAG that:
- Fetches weather data from Open-Meteo
- Transforms unstructured text into structured JSON using Ollama
- Validates the output schema

## DAG Name
weather_unstructured_to_structured

## Tasks
- fetch_weather
- ollama_to_structured
- validate_and_emit

## How to Run

```bash
export AIRFLOW_HOME=$(pwd)/airflow
export WEATHER_PIPELINES_MOCK_OLLAMA=1

airflow db migrate
airflow webserver --port 8080
airflow scheduler
