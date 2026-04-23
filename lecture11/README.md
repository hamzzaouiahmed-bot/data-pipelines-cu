# Lecture 11: Airflow + Ollama — Unstructured → Structured Data

**Theme:** Orchestrate a **data pipeline** with **Apache Airflow** where **Ollama** turns **raw weather API** responses into a **stable JSON schema** (unstructured / semi-structured → structured).

## Slides (Marp)

```bash
cd lecture11
npx @marp-team/marp-cli slides.md -o slides.html --no-stdin
```

Additional deck (Hadoop + Spark + ETL + Vagrant install guide):

```bash
cd lecture11
npx @marp-team/marp-cli slides-hadoop-spark.md -o slides-hadoop-spark.html --no-stdin
```

## Assignment

**Weather (Open-Meteo) → Ollama → validated JSON:** see **`LECTURE11_ASSIGNMENT_README.md`** and **`assignment/dags/`**.

## Prerequisites

- **Python 3.10+** and **Apache Airflow 2.x** (see assignment for install options)
- **Ollama** running locally (`ollama serve`) with a small model pulled (e.g. `tinyllama`, or **`llama3.2`** for better JSON)
- **Lecture 3** (Airflow scheduling concepts) and **Lecture 10** (Ollama) helpful

## Reference

- [Open-Meteo API](https://open-meteo.com/) — no API key
- [Ollama HTTP API](https://github.com/ollama/ollama/blob/main/docs/api.md)
- [Apache Airflow](https://airflow.apache.org/docs/)
