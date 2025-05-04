"""
DAG for incremental loading of daily sales data using dbt.
This DAG starts on April 1, 2025, runs daily, and passes the execution date 
to the dbt model to process only data for that specific date.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.dummy import DummyOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
import logging

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
    Get Snowflake connection details from Airflow and log them
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
        
        # Return connection details for next tasks
        return snowflake_conn
        
    except Exception as e:
        logging.error(f"Failed to get Snowflake connection: {str(e)}")
        raise

def get_execution_date(**context):
    """
    Extract the execution date from the context and format it for use in dbt
    """
    # Get execution_date from the context (this is the logical date of the DAG run)
    execution_date = context['execution_date']
    
    # Format the date as YYYY-MM-DD
    formatted_date = execution_date.strftime('%Y-%m-%d')
    logging.info(f"Processing data for date: {formatted_date}")
    
    return formatted_date

# Create incremental loading DAG
with DAG(
    'dbt_incremental_daily_sales',
    default_args=default_args,
    description='Run incremental loading of daily sales data using dbt',
    start_date=datetime(2025, 4, 1),  # Starting from April 1, 2025
    end_date=datetime(2025, 5, 15),   # End date for demonstration
    schedule_interval='@daily',       # Run daily
    catchup=True,                     # Process all days since start_date
    max_active_runs=1,                # Process one day at a time
    tags=['dbt', 'snowflake', 'incremental'],
) as dag:
    
    # Start of pipeline
    start = DummyOperator(task_id='start_incremental_pipeline')
    
    # 1. Get Snowflake connection details from Airflow
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_conn_and_set_env,
    )
    
    # 2. Get execution date
    get_date = PythonOperator(
        task_id='get_execution_date',
        python_callable=get_execution_date,
        provide_context=True,
    )
    
    # 3. Run the incremental model for the current execution date
    run_incremental_model = DbtRunOperator(
        task_id='run_incremental_model',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['daily_sales_incremental'],
        target='dev',
        vars={"execution_date": "{{ task_instance.xcom_pull(task_ids='get_execution_date') }}"},
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
    
    # 4. End of pipeline
    end = DummyOperator(task_id='end_incremental_pipeline')
    
    # Set task dependencies
    start >> get_connection >> get_date >> run_incremental_model >> end 