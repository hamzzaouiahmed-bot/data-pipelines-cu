"""
Tutorial 3: Pipeline Orchestration with Airflow
================================================

This tutorial covers pipeline orchestration using Apache Airflow - the coordination
and management of multiple tasks in a data pipeline. Orchestration involves:

- Defining task dependencies
- Scheduling pipeline execution
- Managing task execution order
- Handling failures and retries
- Monitoring pipeline status
- Parallel and sequential execution

Learning Objectives:
- Understand pipeline orchestration concepts with Airflow
- Implement task dependencies using Airflow operators
- Create schedulable pipelines
- Handle task failures and retries
- Monitor pipeline execution
- Implement parallel and sequential execution patterns
- Use trigger rules for conditional execution

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
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta, timezone
import logging
import time

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
# DAG DEFINITION - SEQUENTIAL PIPELINE
# ============================================================================

dag_sequential = DAG(
    'tutorial3_sequential_pipeline',
    default_args=default_args,
    description='Sequential pipeline execution example',
    schedule=None,
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'orchestration', 'sequential'],
)

# Sequential pipeline tasks
def extract_task(**context):
    """Extract data."""
    logger.info("Extracting data...")
    time.sleep(0.5)  # Simulate work
    data = {"records": 100, "source": "database"}
    context['ti'].xcom_push(key='extracted_data', value=data)
    return data

def transform_task(**context):
    """Transform data."""
    logger.info("Transforming data...")
    ti = context['ti']
    data = ti.xcom_pull(key='extracted_data', task_ids='extract_seq')
    time.sleep(0.5)  # Simulate work
    transformed = {"records": data['records'], "transformed": True}
    ti.xcom_push(key='transformed_data', value=transformed)
    return transformed

def load_task(**context):
    """Load data."""
    logger.info("Loading data...")
    ti = context['ti']
    data = ti.xcom_pull(key='transformed_data', task_ids='transform_seq')
    time.sleep(0.5)  # Simulate work
    logger.info(f"Loaded {data['records']} records")
    return True

extract_seq = PythonOperator(
    task_id='extract_seq',
    python_callable=extract_task,
    dag=dag_sequential,
)

transform_seq = PythonOperator(
    task_id='transform_seq',
    python_callable=transform_task,
    dag=dag_sequential,
)

load_seq = PythonOperator(
    task_id='load_seq',
    python_callable=load_task,
    dag=dag_sequential,
)

# Sequential dependencies
extract_seq >> transform_seq >> load_seq

# ============================================================================
# DAG DEFINITION - PARALLEL PIPELINE
# ============================================================================

dag_parallel = DAG(
    'tutorial3_parallel_pipeline',
    default_args=default_args,
    description='Parallel pipeline execution example',
    schedule=None,
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'orchestration', 'parallel'],
)

# Parallel pipeline tasks
def process_users(**context):
    """Process user data."""
    logger.info("Processing user data...")
    time.sleep(0.5)
    return {"users": 50}

def process_orders(**context):
    """Process order data."""
    logger.info("Processing order data...")
    time.sleep(0.5)
    return {"orders": 200}

def process_products(**context):
    """Process product data."""
    logger.info("Processing product data...")
    time.sleep(0.5)
    return {"products": 100}

def aggregate_results(**context):
    """Aggregate results from parallel tasks."""
    logger.info("Aggregating results...")
    ti = context['ti']
    
    # Pull results from all parallel tasks
    users = ti.xcom_pull(task_ids='process_users')
    orders = ti.xcom_pull(task_ids='process_orders')
    products = ti.xcom_pull(task_ids='process_products')
    
    total = users['users'] + orders['orders'] + products['products']
    logger.info(f"Total processed: {total}")
    
    return {"total": total}

process_users_task = PythonOperator(
    task_id='process_users',
    python_callable=process_users,
    dag=dag_parallel,
)

process_orders_task = PythonOperator(
    task_id='process_orders',
    python_callable=process_orders,
    dag=dag_parallel,
)

process_products_task = PythonOperator(
    task_id='process_products',
    python_callable=process_products,
    dag=dag_parallel,
)

aggregate_task = PythonOperator(
    task_id='aggregate_results',
    python_callable=aggregate_results,
    dag=dag_parallel,
)

# Parallel dependencies
[process_users_task, process_orders_task, process_products_task] >> aggregate_task

# ============================================================================
# DAG DEFINITION - PIPELINE WITH RETRIES
# ============================================================================

dag_retries = DAG(
    'tutorial3_pipeline_with_retries',
    default_args={
        **default_args,
        'retries': 3,
        'retry_delay': timedelta(seconds=5),
    },
    description='Pipeline with retry logic example',
    schedule=None,
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'orchestration', 'retries'],
)

def unreliable_task(**context):
    """Task that may fail (for retry demonstration)."""
    import random
    logger.info("Running unreliable task...")
    
    # 30% chance of failure
    if random.random() < 0.3:
        raise Exception("Random failure occurred!")
    
    logger.info("Task completed successfully!")
    return True

def downstream_task(**context):
    """Downstream task that depends on unreliable task."""
    logger.info("Downstream task running...")
    return True

unreliable_task_op = PythonOperator(
    task_id='unreliable_task',
    python_callable=unreliable_task,
    dag=dag_retries,
)

downstream_task_op = PythonOperator(
    task_id='downstream_task',
    python_callable=downstream_task,
    dag=dag_retries,
)

unreliable_task_op >> downstream_task_op

# ============================================================================
# DAG DEFINITION - COMPLEX DEPENDENCIES
# ============================================================================

dag_complex = DAG(
    'tutorial3_complex_dependencies',
    default_args=default_args,
    description='Complex dependency graph example',
    schedule=None,
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'orchestration', 'complex'],
)

def task_a(**context):
    logger.info("Task A executing...")
    return "A"

def task_b(**context):
    logger.info("Task B executing...")
    return "B"

def task_c(**context):
    logger.info("Task C executing...")
    return "C"

def task_d(**context):
    logger.info("Task D executing...")
    ti = context['ti']
    a = ti.xcom_pull(task_ids='task_a')
    b = ti.xcom_pull(task_ids='task_b')
    logger.info(f"Task D received: {a}, {b}")
    return "D"

def task_e(**context):
    logger.info("Task E executing...")
    ti = context['ti']
    c = ti.xcom_pull(task_ids='task_c')
    logger.info(f"Task E received: {c}")
    return "E"

def task_f(**context):
    logger.info("Task F executing...")
    ti = context['ti']
    d = ti.xcom_pull(task_ids='task_d')
    e = ti.xcom_pull(task_ids='task_e')
    logger.info(f"Task F received: {d}, {e}")
    return "F"

task_a_op = PythonOperator(task_id='task_a', python_callable=task_a, dag=dag_complex)
task_b_op = PythonOperator(task_id='task_b', python_callable=task_b, dag=dag_complex)
task_c_op = PythonOperator(task_id='task_c', python_callable=task_c, dag=dag_complex)
task_d_op = PythonOperator(task_id='task_d', python_callable=task_d, dag=dag_complex)
task_e_op = PythonOperator(task_id='task_e', python_callable=task_e, dag=dag_complex)
task_f_op = PythonOperator(task_id='task_f', python_callable=task_f, dag=dag_complex)

# Complex dependency graph:
# A, B -> D
# C -> E
# D, E -> F
[task_a_op, task_b_op] >> task_d_op
task_c_op >> task_e_op
[task_d_op, task_e_op] >> task_f_op
