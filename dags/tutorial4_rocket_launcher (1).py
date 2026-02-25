"""
Tutorial 4: Rocket Launcher Pipeline with Airflow and API Integration
=====================================================================

This tutorial demonstrates a rocket launch sequence pipeline using Apache Airflow
that integrates with real-world APIs. It shows:

- API integration: Fetching real data from external APIs
- Parallel execution: Multiple pre-flight checks run simultaneously
- Conditional dependencies: Launch only proceeds if all checks pass
- Sequential execution: Launch sequence must follow a specific order
- XCom usage: Passing data between tasks
- Error handling: Retries and failure notifications

Learning Objectives:
- Understand API integration in Airflow DAGs
- Implement parallel and sequential task execution
- Handle conditional dependencies
- Pass data between tasks using XCom
- Implement proper error handling and retries
- Work with real-world APIs

Note: This tutorial uses The Space Devs API (https://ll.thespacedevs.com/) to fetch
real upcoming rocket launch data. An internet connection is required.

To use this DAG:
1. Install requests: pip install requests
2. Place this file in your Airflow DAGs folder
3. Ensure Airflow is running
4. The DAG will appear in the Airflow UI
5. Trigger it manually (schedule=None for safety)
"""

from airflow import DAG
try:
    from airflow.providers.standard.operators.python import PythonOperator
except ImportError:
    # Fallback for older Airflow versions
    from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta, timezone
import requests
import random
import logging

# Configure logging
logger = logging.getLogger(__name__)

# API endpoint for upcoming launches
LAUNCH_API_URL = "https://ll.thespacedevs.com/2.0.0/launch/upcoming/"

# ============================================================================
# DEFAULT ARGUMENTS
# ============================================================================

default_args = {
    'owner': 'mission_control',
    'depends_on_past': False,
    'email': ['mission-control@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'execution_timeout': timedelta(minutes=30),
}

# ============================================================================
# DAG DEFINITION
# ============================================================================

dag = DAG(
    'tutorial4_rocket_launcher',
    default_args=default_args,
    description='Rocket launch sequence with pre-flight checks and launch procedures',
    schedule=None,  # Manual trigger only (safety!)
    start_date=datetime.now(timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'rocket', 'launch', 'api'],
)

# ============================================================================
# TASK FUNCTIONS - FETCH LAUNCH DATA
# ============================================================================

def fetch_upcoming_launch(**context):
    """Fetch the next upcoming launch from The Space Devs API."""
    logger.info("Fetching upcoming launch data from API...")
    
    try:
        # Fetch upcoming launches (limit to 1 for the next launch)
        response = requests.get(LAUNCH_API_URL, params={'limit': 1}, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if data.get('count', 0) == 0 or not data.get('results'):
            raise ValueError("No upcoming launches found")
        
        launch = data['results'][0]
        
        launch_info = {
            'name': launch.get('name', 'Unknown'),
            'status': launch.get('status', {}).get('name', 'Unknown'),
            'net': launch.get('net', 'TBD'),
            'window_start': launch.get('window_start', 'TBD'),
            'window_end': launch.get('window_end', 'TBD'),
            'rocket': launch.get('rocket', {}).get('configuration', {}).get('name', 'Unknown'),
            'mission': launch.get('mission', {}).get('name', 'Unknown'),
            'location': launch.get('pad', {}).get('location', {}).get('name', 'Unknown'),
            'launch_service_provider': launch.get('launch_service_provider', {}).get('name', 'Unknown'),
        }
        
        logger.info(f"Fetched launch: {launch_info['name']}")
        logger.info(f"Status: {launch_info['status']}")
        logger.info(f"Launch Window: {launch_info['window_start']} to {launch_info['window_end']}")
        logger.info(f"Rocket: {launch_info['rocket']}")
        logger.info(f"Location: {launch_info['location']}")
        
        # Push launch info to XCom
        context['ti'].xcom_push(key='launch_info', value=launch_info)
        context['ti'].xcom_push(key='launch_status', value=launch_info['status'])
        
        return launch_info
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch launch data: {e}")
        raise
    except (KeyError, IndexError) as e:
        logger.error(f"Error parsing launch data: {e}")
        raise ValueError(f"Invalid launch data format: {e}")

# ============================================================================
# TASK FUNCTIONS - PRE-FLIGHT CHECKS
# ============================================================================

def check_weather(**context):
    """Check weather conditions for launch."""
    ti = context['ti']
    
    # Get launch info from previous task
    launch_info = ti.xcom_pull(key='launch_info', task_ids='fetch_upcoming_launch')
    
    if launch_info is None:
        raise ValueError("Launch info not found - fetch_upcoming_launch must run first")
    
    logger.info(f"Checking weather conditions for launch: {launch_info.get('name', 'Unknown')}")
    logger.info(f"Launch location: {launch_info.get('location', 'Unknown')}")
    
    # Simulate weather check (in production, you'd call a weather API)
    weather_conditions = {
        'temperature': random.randint(15, 30),
        'wind_speed': random.randint(0, 25),
        'cloud_cover': random.randint(0, 100),
        'visibility': random.randint(5, 20),
        'location': launch_info.get('location', 'Unknown'),
    }
    
    # Determine if weather is suitable
    is_suitable = (
        weather_conditions['wind_speed'] < 20 and
        weather_conditions['cloud_cover'] < 50 and
        weather_conditions['visibility'] > 10
    )
    
    weather_conditions['suitable'] = is_suitable
    
    if is_suitable:
        logger.info(f"Weather check PASSED: {weather_conditions}")
    else:
        logger.warning(f"Weather check FAILED: {weather_conditions}")
        if weather_conditions['wind_speed'] >= 20:
            raise ValueError(f"Wind speed too high: {weather_conditions['wind_speed']} km/h")
        elif weather_conditions['cloud_cover'] >= 50:
            raise ValueError(f"Cloud cover too high: {weather_conditions['cloud_cover']}%")
        else:
            raise ValueError(f"Visibility too low: {weather_conditions['visibility']} km")
    
    # Push result to XCom
    ti.xcom_push(key='weather_status', value=weather_conditions)
    return weather_conditions


def check_fuel(**context):
    """Check fuel levels and systems."""
    ti = context['ti']
    
    # Get launch info from previous task
    launch_info = ti.xcom_pull(key='launch_info', task_ids='fetch_upcoming_launch')
    
    if launch_info is None:
        raise ValueError("Launch info not found - fetch_upcoming_launch must run first")
    
    rocket_name = launch_info.get('rocket', 'Unknown')
    
    logger.info(f"Checking fuel systems for {rocket_name}...")
    
    # Simulate fuel check
    fuel_status = {
        'fuel_level': random.randint(80, 100),  # Percentage
        'oxidizer_level': random.randint(80, 100),
        'pressure': random.randint(90, 110),  # PSI
        'rocket': rocket_name,
    }
    
    # Determine if fuel is adequate
    is_adequate = (
        fuel_status['fuel_level'] >= 85 and
        fuel_status['oxidizer_level'] >= 85 and
        95 <= fuel_status['pressure'] <= 105
    )
    
    fuel_status['adequate'] = is_adequate
    
    if is_adequate:
        logger.info(f"Fuel check PASSED: {fuel_status}")
    else:
        logger.warning(f"Fuel check FAILED: {fuel_status}")
        raise ValueError(f"Fuel systems not ready: {fuel_status}")
    
    # Push result to XCom
    ti.xcom_push(key='fuel_status', value=fuel_status)
    return fuel_status


def check_systems(**context):
    """Check all rocket systems."""
    ti = context['ti']
    
    # Get launch info from previous task
    launch_info = ti.xcom_pull(key='launch_info', task_ids='fetch_upcoming_launch')
    launch_status = ti.xcom_pull(key='launch_status', task_ids='fetch_upcoming_launch')
    
    if launch_info is None:
        raise ValueError("Launch info not found - fetch_upcoming_launch must run first")
    
    logger.info(f"Checking rocket systems for {launch_info.get('name', 'Unknown')}...")
    logger.info(f"Launch status from API: {launch_status}")
    
    # Check if launch status from API is "Go"
    api_status_go = launch_status == "Go"
    
    # Simulate systems check
    systems_status = {
        'navigation': random.choice([True, True, True, False]),  # 75% success rate
        'communication': random.choice([True, True, True, False]),
        'life_support': random.choice([True, True, True, False]),
        'payload': random.choice([True, True, True, False]),
        'api_status': launch_status,
        'api_status_go': api_status_go,
    }
    
    # Determine if all systems are operational
    all_operational = all([
        systems_status['navigation'],
        systems_status['communication'],
        systems_status['life_support'],
        systems_status['payload'],
        api_status_go  # Must be "Go" from API
    ])
    
    systems_status['all_operational'] = all_operational
    
    if all_operational:
        logger.info(f"Systems check PASSED: {systems_status}")
    else:
        failed_systems = [k for k, v in systems_status.items() if not v and k not in ['api_status', 'api_status_go']]
        if not api_status_go:
            failed_systems.append(f"API status: {launch_status} (expected 'Go')")
        logger.warning(f"Systems check FAILED: {failed_systems}")
        raise ValueError(f"Systems not operational: {failed_systems}")
    
    # Push result to XCom
    ti.xcom_push(key='systems_status', value=systems_status)
    return systems_status

# ============================================================================
# TASK FUNCTIONS - LAUNCH SEQUENCE
# ============================================================================

def final_go_no_go(**context):
    """Final go/no-go decision based on all checks."""
    ti = context['ti']
    
    # Pull results from all pre-flight checks
    weather = ti.xcom_pull(key='weather_status', task_ids='check_weather')
    fuel = ti.xcom_pull(key='fuel_status', task_ids='check_fuel')
    systems = ti.xcom_pull(key='systems_status', task_ids='check_systems')
    
    logger.info("Performing final go/no-go decision...")
    logger.info(f"Weather: {weather.get('suitable', False) if weather else False}")
    logger.info(f"Fuel: {fuel.get('adequate', False) if fuel else False}")
    logger.info(f"Systems: {systems.get('all_operational', False) if systems else False}")
    
    # Make final decision
    go_for_launch = (
        weather and weather.get('suitable', False) and
        fuel and fuel.get('adequate', False) and
        systems and systems.get('all_operational', False)
    )
    
    if go_for_launch:
        logger.info("✅ GO FOR LAUNCH! All systems ready.")
        ti.xcom_push(key='go_for_launch', value=True)
        return True
    else:
        logger.error("❌ NO GO! Launch aborted.")
        raise ValueError("Launch aborted: Pre-flight checks failed")


def countdown(**context):
    """Perform launch countdown."""
    ti = context['ti']
    
    # Get launch info
    launch_info = ti.xcom_pull(key='launch_info', task_ids='fetch_upcoming_launch')
    
    logger.info(f"Starting launch countdown for {launch_info.get('name', 'Unknown') if launch_info else 'Unknown'}...")
    if launch_info:
        logger.info(f"Mission: {launch_info.get('mission', 'Unknown')}")
        logger.info(f"Launch window: {launch_info.get('window_start', 'TBD')} to {launch_info.get('window_end', 'TBD')}")
    
    # Simulate countdown
    for i in range(10, 0, -1):
        logger.info(f"T-{i}...")
    
    logger.info("T-0! Ready for ignition!")
    return True


def ignite_engines(**context):
    """Ignite rocket engines."""
    logger.info("Igniting engines...")
    logger.info("🔥 Engine 1: IGNITED")
    logger.info("🔥 Engine 2: IGNITED")
    logger.info("🔥 Engine 3: IGNITED")
    logger.info("All engines ignited successfully!")
    return True


def launch(**context):
    """Execute rocket launch."""
    ti = context['ti']
    
    # Get launch info
    launch_info = ti.xcom_pull(key='launch_info', task_ids='fetch_upcoming_launch')
    
    logger.info(f"🚀 LAUNCH! {launch_info.get('name', 'Unknown') if launch_info else 'Unknown'} is ascending!")
    if launch_info:
        logger.info(f"Rocket: {launch_info.get('rocket', 'Unknown')}")
        logger.info(f"Mission: {launch_info.get('mission', 'Unknown')}")
    logger.info("Altitude: 100m... 500m... 1km... 5km...")
    logger.info("Rocket successfully launched!")
    return True


def mission_success(**context):
    """Celebrate successful launch."""
    logger.info("🎉 MISSION SUCCESS! Rocket launch completed successfully!")
    return True

# ============================================================================
# TASK DEFINITIONS - FETCH LAUNCH DATA
# ============================================================================

# Fetch upcoming launch from API
fetch_launch_task = PythonOperator(
    task_id='fetch_upcoming_launch',
    python_callable=fetch_upcoming_launch,
    dag=dag,
)

# ============================================================================
# TASK DEFINITIONS - PRE-FLIGHT CHECKS
# ============================================================================

# Pre-flight checks (run in parallel, after fetching launch data)
check_weather_task = PythonOperator(
    task_id='check_weather',
    python_callable=check_weather,
    dag=dag,
)

check_fuel_task = PythonOperator(
    task_id='check_fuel',
    python_callable=check_fuel,
    dag=dag,
)

check_systems_task = PythonOperator(
    task_id='check_systems',
    python_callable=check_systems,
    dag=dag,
)

# ============================================================================
# TASK DEFINITIONS - LAUNCH SEQUENCE
# ============================================================================

# Final decision
go_no_go_task = PythonOperator(
    task_id='final_go_no_go',
    python_callable=final_go_no_go,
    trigger_rule=TriggerRule.ALL_SUCCESS,  # All checks must pass
    dag=dag,
)

# Launch sequence
countdown_task = PythonOperator(
    task_id='countdown',
    python_callable=countdown,
    dag=dag,
)

ignite_task = PythonOperator(
    task_id='ignite_engines',
    python_callable=ignite_engines,
    dag=dag,
)

launch_task = PythonOperator(
    task_id='launch',
    python_callable=launch,
    dag=dag,
)

mission_success_task = PythonOperator(
    task_id='mission_success',
    python_callable=mission_success,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================

# First, fetch launch data from API
# Then pre-flight checks run in parallel
fetch_launch_task >> [check_weather_task, check_fuel_task, check_systems_task] >> go_no_go_task

# Launch sequence (sequential)
go_no_go_task >> countdown_task >> ignite_task >> launch_task >> mission_success_task
