"""
Tutorial 2: Data Ingestion with Airflow
========================================

This tutorial covers data ingestion using Apache Airflow - the process of importing
data from various sources into a data pipeline. Data ingestion is the first step
in any data pipeline and involves:

- Reading from multiple data sources (files, APIs, databases, streams)
- Handling different data formats (JSON, CSV, XML, Parquet, etc.)
- Managing data volume and rate
- Error handling and retry logic
- Schema validation and data quality checks

Learning Objectives:
- Understand different data ingestion patterns with Airflow
- Implement ingestion from various sources using PythonOperator
- Handle streaming vs batch ingestion
- Implement error handling and retry mechanisms
- Validate data during ingestion
- Use XCom to pass ingested data between tasks

To use this DAG:
1. Place this file in your Airflow DAGs folder
2. Ensure Airflow is running
3. The DAG will appear in the Airflow UI
4. Trigger it manually or wait for schedule
"""

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Fallback for older Airflow versions
    from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import json
import csv
import logging

# Configure logging
logger = logging.getLogger(__name__)

# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    'tutorial2_data_ingestion',
    default_args=default_args,
    description='Data ingestion from multiple sources (files, APIs, databases)',
    schedule=None,  # Manual trigger for tutorial
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'ingestion', 'data'],
)

# ============================================================================
# TASK FUNCTIONS - FILE INGESTION
# ============================================================================

def ingest_json_file(**context):
    """Ingest data from JSON file."""
    logger.info("Starting JSON file ingestion...")
    
    source_file = 'demo1_source_data.json'
    
    # Create sample data if file doesn't exist
    import os
    if not os.path.exists(source_file):
        logger.info(f"Creating sample JSON file: {source_file}")
        sample_data = [
            {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
            {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35}
        ]
        with open(source_file, 'w') as f:
            json.dump(sample_data, f, indent=2)
    
    # Ingest JSON
    try:
        with open(source_file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                records = []
        
        logger.info(f"Ingested {len(records)} records from {source_file}")
        
        # Validate schema
        validated_records = []
        for record in records:
            if 'id' in record and 'name' in record:
                validated_records.append(record)
            else:
                logger.warning(f"Record missing required fields: {record}")
        logger.info(f"Validated {len(validated_records)} records")
        
        # Push to XCom
        context['ti'].xcom_push(key='json_records', value=validated_records)
        context['ti'].xcom_push(key='json_count', value=len(validated_records))
        
        return validated_records
    except Exception as e:
        logger.error(f"Error ingesting JSON: {e}")
        raise


def ingest_csv_file(**context):
    """Ingest data from CSV file."""
    logger.info("Starting CSV file ingestion...")
    
    source_file = 'demo2_source_products.csv'
    
    # Create sample CSV if file doesn't exist
    import os
    if not os.path.exists(source_file):
        logger.info(f"Creating sample CSV file: {source_file}")
        csv_data = [
            {"id": 1, "product_id": "P001", "product_name": "Laptop", "price": "999.99", "stock": "50"},
            {"id": 2, "product_id": "P002", "product_name": "Mouse", "price": "29.99", "stock": "200"},
            {"id": 3, "product_id": "P003", "product_name": "Keyboard", "price": "79.99", "stock": "150"},
        ]
        with open(source_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['id', 'product_id', 'product_name', 'price', 'stock'])
            writer.writeheader()
            writer.writerows(csv_data)
    
    # Ingest CSV
    try:
        records = []
        with open(source_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Validate and convert types
                if 'id' in row:
                    try:
                        row['id'] = int(row['id'])
                        row['price'] = float(row['price']) if 'price' in row else 0.0
                        row['stock'] = int(row['stock']) if 'stock' in row else 0
                        records.append(row)
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Error converting types for row: {row}, error: {e}")
        
        logger.info(f"Ingested {len(records)} records from {source_file}")
        
        # Push to XCom
        context['ti'].xcom_push(key='csv_records', value=records)
        context['ti'].xcom_push(key='csv_count', value=len(records))
        
        return records
    except Exception as e:
        logger.error(f"Error ingesting CSV: {e}")
        raise


def ingest_from_api(**context):
    """Ingest data from API endpoint (mock)."""
    logger.info("Starting API ingestion...")
    
    # Mock API call - in production use requests library
    endpoint = context.get('params', {}).get('api_endpoint', 'https://api.example.com/data')
    logger.info(f"Calling API: {endpoint}")
    
    # Simulate API response
    api_data = [
        {"id": 1, "user": "api_user_1", "status": "active", "created": "2024-01-01"},
        {"id": 2, "user": "api_user_2", "status": "active", "created": "2024-01-02"},
    ]
    
    logger.info(f"Ingested {len(api_data)} records from API")
    
    # Push to XCom
    context['ti'].xcom_push(key='api_records', value=api_data)
    context['ti'].xcom_push(key='api_count', value=len(api_data))
    
    return api_data


def ingest_from_database(**context):
    """Ingest data from database (mock)."""
    logger.info("Starting database ingestion...")
    
    # Mock database query - in production use database connectors
    query = context.get('params', {}).get('db_query', 'SELECT * FROM users')
    logger.info(f"Executing query: {query}")
    
    # Simulate database results
    db_data = [
        {"id": 1, "username": "db_user_1", "role": "admin", "last_login": "2024-01-15"},
        {"id": 2, "username": "db_user_2", "role": "user", "last_login": "2024-01-14"},
    ]
    
    logger.info(f"Ingested {len(db_data)} records from database")
    
    # Push to XCom
    context['ti'].xcom_push(key='db_records', value=db_data)
    context['ti'].xcom_push(key='db_count', value=len(db_data))
    
    return db_data

# ============================================================================
# TASK FUNCTIONS - DATA MERGING
# ============================================================================

def merge_ingested_data(**context):
    """Merge data from all ingestion sources."""
    logger.info("Merging data from all sources...")
    
    ti = context['ti']
    
    # Pull data from all ingestion tasks
    json_records = ti.xcom_pull(key='json_records', task_ids='ingest_json')
    csv_records = ti.xcom_pull(key='csv_records', task_ids='ingest_csv')
    api_records = ti.xcom_pull(key='api_records', task_ids='ingest_api')
    db_records = ti.xcom_pull(key='db_records', task_ids='ingest_database')
    
    # Merge all records
    merged_data = []
    
    if json_records:
        merged_data.extend(json_records)
        logger.info(f"Added {len(json_records)} JSON records")
    
    if csv_records:
        merged_data.extend(csv_records)
        logger.info(f"Added {len(csv_records)} CSV records")
    
    if api_records:
        merged_data.extend(api_records)
        logger.info(f"Added {len(api_records)} API records")
    
    if db_records:
        merged_data.extend(db_records)
        logger.info(f"Added {len(db_records)} database records")
    
    logger.info(f"Total merged records: {len(merged_data)}")
    
    # Push merged data to XCom
    ti.xcom_push(key='merged_data', value=merged_data)
    ti.xcom_push(key='total_records', value=len(merged_data))
    
    return merged_data


def save_merged_data(**context):
    """Save merged data to output file."""
    logger.info("Saving merged data...")
    
    ti = context['ti']
    merged_data = ti.xcom_pull(key='merged_data', task_ids='merge_data')
    
    if not merged_data:
        raise ValueError("No merged data available")
    
    output_file = 'demo_merged_ingestion.json'
    
    try:
        with open(output_file, 'w') as f:
            json.dump(merged_data, f, indent=2)
        
        logger.info(f"Saved {len(merged_data)} records to {output_file}")
        
        ti.xcom_push(key='output_file', value=output_file)
        
        return True
    except Exception as e:
        logger.error(f"Error saving merged data: {e}")
        raise

# ============================================================================
# TASK DEFINITIONS
# ============================================================================

# Ingestion tasks (run in parallel)
ingest_json_task = PythonOperator(
    task_id='ingest_json',
    python_callable=ingest_json_file,
    dag=dag,
)

ingest_csv_task = PythonOperator(
    task_id='ingest_csv',
    python_callable=ingest_csv_file,
    dag=dag,
)

ingest_api_task = PythonOperator(
    task_id='ingest_api',
    python_callable=ingest_from_api,
    dag=dag,
)

ingest_database_task = PythonOperator(
    task_id='ingest_database',
    python_callable=ingest_from_database,
    dag=dag,
)

# Merge task
merge_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_ingested_data,
    dag=dag,
)

# Save task
save_task = PythonOperator(
    task_id='save_merged_data',
    python_callable=save_merged_data,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# All ingestion tasks run in parallel, then merge, then save
[ingest_json_task, ingest_csv_task, ingest_api_task, ingest_database_task] >> merge_task >> save_task
