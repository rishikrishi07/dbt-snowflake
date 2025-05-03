from datetime import datetime, timedelta
from airflow import DAG
from airflow_dbt_python.operators.dbt import DbtDocsGenerateOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_snowflake_connection(**context):
    """Get Snowflake connection details from Airflow connection"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake-conn')
    conn = hook.get_connection('snowflake-conn')
    
    return {
        'account': conn.extra_dejson.get('account'),
        'user': conn.login,
        'password': conn.password,
        'role': conn.extra_dejson.get('role'),
        'database': conn.extra_dejson.get('database'),
        'warehouse': conn.extra_dejson.get('warehouse'),
        'schema': conn.schema or 'PUBLIC',
    }

# DAG for dbt documentation
with DAG(
    'dbt_documentation',
    default_args=default_args,
    description='Generate dbt documentation',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'docs'],
) as dag:
    
    # Get Snowflake connection details
    get_connection = PythonOperator(
        task_id='get_snowflake_connection',
        python_callable=get_snowflake_connection,
    )
    
    # Generate dbt documentation
    generate_docs = DbtDocsGenerateOperator(
        task_id='generate_dbt_docs',
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
    
    # Set dependencies
    get_connection >> generate_docs 