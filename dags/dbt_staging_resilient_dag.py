from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import (
    DbtRunOperator, 
    DbtDebugOperator, 
    DbtTestOperator
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import logging
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # No retries to immediately show failure
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

def failing_task(**kwargs):
    """
    Function that will always fail
    """
    logging.info("This task is designed to fail...")
    raise ValueError("This task fails on purpose to demonstrate resilience")

# Create DAG for resilient staging layer
with DAG(
    '1_dbt_staging_resilient',
    default_args=default_args,
    description='Resilient DAG that continues despite task failures',
    schedule_interval='0 2 * * *',  # Daily at 2 AM, adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'staging', 'resilient'],
) as dag:
    
    # Start of pipeline
    start = DummyOperator(task_id='start_staging_pipeline')
    
    # 1. Get Snowflake connection details from Airflow
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_conn_and_set_env,
    )
    
    # 2. Run dbt debug to verify connection
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
    
    # Task that will always fail
    intentional_failure = PythonOperator(
        task_id='intentional_failure',
        python_callable=failing_task,
    )
    
    # 3. Run individual staging models
    
    # 3.1 HCP Master
    run_stg_hcp_master = DbtRunOperator(
        task_id='run_stg_hcp_master',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_hcp_master',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 3.2 HCP Visits
    run_stg_hcp_visits = DbtRunOperator(
        task_id='run_stg_hcp_visits',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_hcp_visits',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 3.3 Medical Representatives
    run_stg_medical_reps = DbtRunOperator(
        task_id='run_stg_medical_reps',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_medical_representatives',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 3.4 Prescriptions
    run_stg_prescriptions = DbtRunOperator(
        task_id='run_stg_prescriptions',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_prescriptions',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 3.5 Products
    run_stg_products = DbtRunOperator(
        task_id='run_stg_products',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_products',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 3.6 Territories
    run_stg_territories = DbtRunOperator(
        task_id='run_stg_territories',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select='stg_territories',
        target='dev',
        env_vars={
            'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
            'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
            'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
            'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
            'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
            'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
            'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # 4. Test all staging models
    test_staging_models = DbtTestOperator(
        task_id='test_staging_models',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
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
        },
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # End of pipeline
    end = DummyOperator(
        task_id='end_staging_pipeline',
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
    )
    
    # Trigger the intermediate layer DAG
    trigger_intermediate_dag = TriggerDagRunOperator(
        task_id='trigger_intermediate_dag',
        trigger_dag_id='2_dbt_intermediate_layer',
        wait_for_completion=False,
        reset_dag_run=True,
        conf={'parent_dag': '1_dbt_staging_resilient'},
        trigger_rule=TriggerRule.ALL_DONE,  # Trigger even if upstream tasks fail
    )
    
    # Define the task dependencies
    start >> get_connection >> dbt_debug
    
    # The intentional failure task executes after dbt_debug
    dbt_debug >> intentional_failure
    
    # Run all staging models in parallel and continue despite the failure
    dbt_debug >> [
        run_stg_hcp_master,
        run_stg_hcp_visits,
        run_stg_medical_reps,
        run_stg_prescriptions,
        run_stg_products,
        run_stg_territories
    ]
    
    # Wait for all staging models to complete before testing
    [
        run_stg_hcp_master,
        run_stg_hcp_visits,
        run_stg_medical_reps,
        run_stg_prescriptions,
        run_stg_products,
        run_stg_territories
    ] >> test_staging_models >> end
    
    # Trigger next DAG after pipeline ends
    end >> trigger_intermediate_dag 