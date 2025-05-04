from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator, 
    DbtDebugOperator,
    DbtTestOperator,
    DbtDocsGenerateOperator
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
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

# Create DAG for dbt transformations
with DAG(
    'dbt_transformation_pipeline',
    default_args=default_args,
    description='Run dbt transformations in proper order: staging -> intermediate -> marts',
    schedule_interval='0 2 * * *',  # Daily at 2 AM, adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'transformation'],
) as dag:
    
    # Start of pipeline
    start = DummyOperator(task_id='start_pipeline')
    
    # 1. Get Snowflake connection details from Airflow
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_conn_and_set_env,
    )
    
    # 2. Run dbt debug to verify connection
    dbt_debug = DbtDebugOperator(
        task_id='dbt_debug',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
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
    
    # 3. Run staging models
    run_staging_models = DbtRunOperator(
        task_id='run_staging_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='staging',
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
    
    # 4. Test staging models
    test_staging_models = DbtTestOperator(
        task_id='test_staging_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='staging',
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
    
    # 5. Run intermediate models
    run_intermediate_models = DbtRunOperator(
        task_id='run_intermediate_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='intermediate',
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
    
    # 6. Test intermediate models
    test_intermediate_models = DbtTestOperator(
        task_id='test_intermediate_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='intermediate',
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
    
    # 7. Run marts models
    run_marts_models = DbtRunOperator(
        task_id='run_marts_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='marts',
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
    
    # 8. Test marts models
    test_marts_models = DbtTestOperator(
        task_id='test_marts_models',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
        select='marts',
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
    
    # 9. Generate dbt documentation
    generate_docs = DbtDocsGenerateOperator(
        task_id='generate_dbt_docs',
        project_dir='/opt/airflow/airflow/dbt',
        profiles_dir='/opt/airflow/airflow/dbt',
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
    
    # End of pipeline
    end = DummyOperator(task_id='end_pipeline')
    
    # Define the task dependencies
    start >> get_connection >> dbt_debug
    
    # Staging layer
    dbt_debug >> run_staging_models >> test_staging_models
    
    # Intermediate layer
    test_staging_models >> run_intermediate_models >> test_intermediate_models
    
    # Marts layer
    test_intermediate_models >> run_marts_models >> test_marts_models
    
    # Documentation and completion
    test_marts_models >> generate_docs >> end 