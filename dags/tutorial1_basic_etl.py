"""
Tutorial 1: Basic ETL Pipeline with Airflow
============================================

This tutorial demonstrates the fundamental ETL (Extract, Transform, Load) pattern
using Apache Airflow. ETL is the process of:
- EXTRACT: Getting data from source systems
- TRANSFORM: Cleaning, validating, and modifying data
- LOAD: Writing data to destination systems

Learning Objectives:
- Understand the ETL pattern with Airflow
- Implement data extraction from different sources
- Apply data transformations
- Load data to various destinations
- Handle errors and data validation
- Use XCom to pass data between tasks

Note: All demo examples create their own sample data files automatically.
Output files are prefixed with 'demo1_', 'demo2_', etc. to avoid conflicts.

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    'tutorial1_basic_etl',
    default_args=default_args,
    description='Basic ETL pipeline demonstrating Extract, Transform, Load pattern',
    schedule=None,  # Manual trigger only for tutorial
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'etl', 'basic'],
)

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def extract_data(**context):
    """Extract data from source (JSON file)."""
    logger.info("Starting data extraction...")
    
    # Get source file from context or use default
    source_file = context.get('params', {}).get('source_file', 'demo1_source_data.json')
    
    # Create sample data if file doesn't exist
    import os
    if not os.path.exists(source_file):
        logger.info(f"Creating sample source file: {source_file}")
        sample_data = [
            {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
            {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35}
        ]
        with open(source_file, 'w') as f:
            json.dump(sample_data, f, indent=2)
    
    # Extract data
    try:
        with open(source_file, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                extracted = data
            elif isinstance(data, dict):
                extracted = [data]
            else:
                extracted = []
        
        logger.info(f"Extracted {len(extracted)} records from {source_file}")
        
        # Push to XCom for downstream tasks
        context['ti'].xcom_push(key='extracted_data', value=extracted)
        context['ti'].xcom_push(key='source_file', value=source_file)
        
        return extracted
    except FileNotFoundError:
        logger.error(f"File not found: {source_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON: {e}")
        raise


def transform_data(**context):
    """Transform extracted data."""
    logger.info("Starting data transformation...")
    
    # Pull data from XCom
    ti = context['ti']
    extracted_data = ti.xcom_pull(key='extracted_data', task_ids='extract')
    source_file = ti.xcom_pull(key='source_file', task_ids='extract')
    
    if not extracted_data:
        raise ValueError("No data extracted")
    
    logger.info(f"Transforming {len(extracted_data)} records")
    
    # Apply transformations
    transformed = []
    
    for record in extracted_data:
        # Clean: remove nulls and trim strings
        cleaned_record = {}
        for key, value in record.items():
            if value is not None:
                if isinstance(value, str):
                    cleaned_record[key] = value.strip()
                else:
                    cleaned_record[key] = value
        
        # Validate: check required fields
        if 'id' in cleaned_record:
            # Enrich: add metadata
            enriched_record = cleaned_record.copy()
            enriched_record['processed_at'] = datetime.now().isoformat()
            enriched_record['record_count'] = len(extracted_data)
            
            # Normalize: lowercase keys
            normalized_record = {}
            for k, v in enriched_record.items():
                normalized_key = k.lower().replace(' ', '_')
                normalized_record[normalized_key] = v
            
            transformed.append(normalized_record)
        else:
            logger.warning(f"Record missing required 'id' field: {record}")
    
    logger.info(f"Transformed {len(transformed)} records")
    
    # Push to XCom
    ti.xcom_push(key='transformed_data', value=transformed)
    
    return transformed


def load_data(**context):
    """Load transformed data to destination."""
    logger.info("Starting data loading...")
    
    # Pull data from XCom
    ti = context['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform')
    source_file = ti.xcom_pull(key='source_file', task_ids='extract')
    
    if not transformed_data:
        raise ValueError("No transformed data available")
    
    # Determine output file
    if 'demo1_' in source_file:
        output_file = 'demo1_output_data.json'
    elif 'demo2_' in source_file:
        output_file = 'demo2_output_data.json'
    elif 'demo4_' in source_file:
        output_file = 'demo4_output_cleaned.json'
    else:
        output_file = 'output_data.json'
    
    # Load to JSON
    try:
        with open(output_file, 'w') as f:
            json.dump(transformed_data, f, indent=2)
        
        logger.info(f"Loaded {len(transformed_data)} records to {output_file}")
        
        # Push result to XCom
        ti.xcom_push(key='output_file', value=output_file)
        ti.xcom_push(key='records_loaded', value=len(transformed_data))
        
        return True
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load_data,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

extract_task >> transform_task >> load_task

# ============================================================================
# DEMO: CSV to JSON ETL
# ============================================================================

def extract_csv_data(**context):
    """Extract data from CSV file."""
    logger.info("Starting CSV data extraction...")
    
    source_file = 'demo2_source_products.csv'
    
    # Create sample CSV if it doesn't exist
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
    
    # Extract from CSV
    try:
        data = []
        with open(source_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(row)
        
        logger.info(f"Extracted {len(data)} records from {source_file}")
        
        context['ti'].xcom_push(key='extracted_data', value=data)
        context['ti'].xcom_push(key='source_file', value=source_file)
        
        return data
    except Exception as e:
        logger.error(f"Error extracting CSV: {e}")
        raise


def load_to_json(**context):
    """Load transformed data to JSON."""
    logger.info("Loading data to JSON...")
    
    ti = context['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_csv')
    source_file = ti.xcom_pull(key='source_file', task_ids='extract_csv')
    
    if not transformed_data:
        raise ValueError("No transformed data available")
    
    output_file = 'demo2_output_products.json'
    
    try:
        with open(output_file, 'w') as f:
            json.dump(transformed_data, f, indent=2)
        
        logger.info(f"Loaded {len(transformed_data)} records to {output_file}")
        ti.xcom_push(key='output_file', value=output_file)
        
        return True
    except Exception as e:
        logger.error(f"Error loading to JSON: {e}")
        raise


# CSV ETL Tasks
extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv_data,
    dag=dag,
)

transform_csv_task = PythonOperator(
    task_id='transform_csv',
    python_callable=transform_data,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id='load_csv',
    python_callable=load_to_json,
    dag=dag,
)

# CSV ETL Dependencies
extract_csv_task >> transform_csv_task >> load_csv_task
