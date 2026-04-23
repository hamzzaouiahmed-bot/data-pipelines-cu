# Lecture 11: Airflow + Ollama — Weather → Structured JSON

Build an **Airflow DAG** that:

1. **Fetches** live weather from the **Open-Meteo** HTTP API (no API key).
2. **Sends** the raw JSON text to **Ollama** and asks for a **single JSON object** matching a fixed schema (unstructured / messy input → **structured** output).
3. **Validates** required keys exist (fail the task if the model omits fields).

Starter code: **`assignment/dags/weather_ollama_dag.py`**.

## Objectives

- Run **Ollama** locally and confirm **`/api/chat`** works with **`format: "json"`** (or equivalent in your Ollama version).
- Install **Airflow 2.x**, register the DAG, trigger a run, and inspect **task logs / XCom**.
- **Optional:** Change coordinates or schema fields in the DAG and document behavior.

## Prerequisites

- **Ollama:** `ollama serve` on **`http://127.0.0.1:11434`** (default). Pull a model: `ollama pull tinyllama` (fast) or `ollama pull llama3.2` (better JSON).
- **Airflow 2.7+** with `requests` available to workers (install in the same environment Airflow uses).

### Airflow variables (UI: Admin → Variables)

| Key | Example | Purpose |
|-----|---------|---------|
| `ollama_base_url` | `http://127.0.0.1:11434` | If Airflow runs in Docker, try `http://host.docker.internal:11434` |
| `ollama_model` | `tinyllama` or `llama3.2` | Model name for `/api/chat` |

If variables are missing, the DAG defaults match **`127.0.0.1`** and **`tinyllama`**.

### Mock mode (no Ollama)

For CI or laptops without a model:

```bash
export WEATHER_PIPELINES_MOCK_OLLAMA=1
```

The **`ollama_to_structured`** task returns canned JSON instead of calling Ollama.

## Quick start (local Airflow + local Ollama)

```bash
# Terminal A — Ollama
ollama serve
ollama pull tinyllama   # or: ollama pull llama3.2

# Terminal B — Airflow (venv; match Airflow docs for your Python version)
python3 -m venv .venv-airflow
source .venv-airflow/bin/activate
pip install "apache-airflow>=2.7,<3" requests pendulum

export AIRFLOW_HOME=~/airflow-lecture11
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Copy DAG
mkdir -p "$AIRFLOW_HOME/dags"
cp assignment/dags/weather_ollama_dag.py "$AIRFLOW_HOME/dags/"

airflow standalone
```

Open the UI (URL printed by `airflow standalone`), enable/trigger DAG **`weather_unstructured_to_structured`**, watch tasks **`fetch_open_meteo_raw` → `ollama_to_structured` → `validate_and_emit`**.

## How to submit

1. Screenshot of **Airflow DAG run** (graph or grid) showing **success** (or explain mock mode).
2. Screenshot or paste of **structured JSON** from task logs / XCom (`validate_and_emit`).
3. Your **`weather_ollama_dag.py`** (and any **`requirements.txt`** you added).
4. **Pull request** with the above.

### PR title example

```
Lecture 11: Airflow + Ollama weather pipeline - [Your Name]
```

## Reference

- [Open-Meteo](https://open-meteo.com/)
- Lecture 3: Airflow scheduling examples
- Lecture 10: Ollama on Nomad
