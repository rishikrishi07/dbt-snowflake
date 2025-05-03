from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtRunOperator, DbtDebugOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
import logging
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_conn_and_set_env(**context):
    """
    Get Snowflake connection details from Airflow and set them as task instance variables
    """
    try:
        # Get connection from Airflow
        hook = SnowflakeHook(snowflake_conn_id='snowflake-conn')
        conn = hook.get_connection('snowflake-conn')
        
        # Extract connection details
        snowflake_conn = {
            'account': conn.extra_dejson.get('account'),
            'user': conn.login,
            'password': conn.password,
            'role': conn.extra_dejson.get('role'),
            'database': conn.extra_dejson.get('database'),
            'warehouse': conn.extra_dejson.get('warehouse'),
            'schema': conn.schema or 'PUBLIC',
        }
        
        # Log connection (without sensitive data)
        logging.info(f"Using Snowflake account: {snowflake_conn['account']}")
        logging.info(f"Using Snowflake database: {snowflake_conn['database']}")
        logging.info(f"Using Snowflake warehouse: {snowflake_conn['warehouse']}")
        
        # Pass connection details to next task via XCom
        return snowflake_conn
        
    except Exception as e:
        logging.error(f"Failed to get Snowflake connection: {str(e)}")
        raise

# Create DAG for testing Snowflake connection with dbt
with DAG(
    'simple_dbt_test',
    default_args=default_args,
    description='Test DAG for dbt with Snowflake connection',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'test'],
) as dag:
    
    # 1. Get Snowflake connection details from Airflow
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_conn_and_set_env,
    )
    
    # 2. Run a simple dbt command (debug) to test the connection
    dbt_debug = DbtDebugOperator(
        task_id='dbt_debug',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        }
    )
    
    # 3. Run a simple dbt model to fully validate the setup
    dbt_run_test = DbtRunOperator(
        task_id='dbt_run_test',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['first_model'],
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        }
    )
    
    # Define task dependencies
    get_connection >> dbt_debug >> dbt_run_test 