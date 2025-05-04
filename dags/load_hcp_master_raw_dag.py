from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtSeedOperator, DbtDebugOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging

# Import the simple logger from the local directory
from simple_logger import log_task, log_dbt_result

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@log_task
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

# Create DAG for loading all seed data
with DAG(
    '1_csv_load_all_seeds',
    default_args=default_args,
    description='Load all CSV seed files into Snowflake',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'hcp', 'raw'],
) as dag:
    
    # 1. Get Snowflake connection details from Airflow
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_conn_and_set_env,
        provide_context=True,
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
    
    # Create start and end tasks for better visualization
    start_seeds = DummyOperator(task_id='start_seeds')
    end_seeds = DummyOperator(task_id='end_seeds')
    
    # Common environment variables for all dbt seed tasks
    env_vars = {
        'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
        'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
        'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
        'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
        'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
        'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
        'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
    }
    
    # 3. Run dbt seed to load HCP master data
    load_hcp_master = DbtSeedOperator(
        task_id='load_hcp_master',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['hcp_master'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    # 4. Run dbt seed to load HCP visits data (after master)
    load_hcp_visits = DbtSeedOperator(
        task_id='load_hcp_visits',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['hcp_visits'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    # 5. Run dbt seed to load other data (in parallel)
    load_medical_representatives = DbtSeedOperator(
        task_id='load_medical_representatives',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['medical_representatives'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    load_prescriptions = DbtSeedOperator(
        task_id='load_prescriptions',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['prescriptions'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    load_products = DbtSeedOperator(
        task_id='load_products',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['products'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    load_territories = DbtSeedOperator(
        task_id='load_territories',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['territories'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    # 6. Run dbt seed to load daily sales data
    load_daily_sales = DbtSeedOperator(
        task_id='load_daily_sales',
        project_dir='/opt/airflow/dbt',
        profiles_dir='/opt/airflow/dbt',
        select=['daily_sales'],
        full_refresh=True,
        target='dev',
        env_vars=env_vars
    )
    
    # Set up task dependencies
    get_connection >> dbt_debug >> start_seeds
    
    # Group 1: HCP master and visits (sequential)
    start_seeds >> load_hcp_master >> load_hcp_visits >> end_seeds
    
    # Other seeds in parallel (independent of HCP seeds)
    start_seeds >> [
        load_medical_representatives,
        load_prescriptions,
        load_products,
        load_territories,
        load_daily_sales
    ] >> end_seeds 