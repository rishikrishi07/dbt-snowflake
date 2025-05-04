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
from airflow.utils.task_group import TaskGroup
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

# Create DAG for full dbt pipeline
with DAG(
    '4_dbt_transform_pipeline',
    default_args=default_args,
    description='Run complete dbt transformation pipeline - staging, intermediate, and marts layers',
    schedule_interval='0 5 * * *',  # Daily at 5 AM, adjust as needed
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['dbt', 'snowflake', 'full_pipeline'],
) as dag:
    
    # Pipeline structure
    start_pipeline = DummyOperator(task_id='start_pipeline')
    
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
    
    # Create common env vars dictionary to avoid duplication
    dbt_env_vars = {
        'DBT_SNOWFLAKE_ACCOUNT': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['account'] }}",
        'DBT_SNOWFLAKE_USER': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['user'] }}",
        'DBT_SNOWFLAKE_PASSWORD': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['password'] }}",
        'DBT_SNOWFLAKE_ROLE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['role'] }}",
        'DBT_SNOWFLAKE_DATABASE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['database'] }}",
        'DBT_SNOWFLAKE_WAREHOUSE': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['warehouse'] }}",
        'DBT_SNOWFLAKE_SCHEMA': "{{ task_instance.xcom_pull(task_ids='get_snowflake_connection')['schema'] }}"
    }
    
    # 3. STAGING LAYER: Task Group
    with TaskGroup(group_id='staging_layer', tooltip='Staging models that transform raw data') as staging_group:
        # Start of staging group
        start_staging = DummyOperator(task_id='start_staging')
        
        # 3.1 HCP Master
        run_stg_hcp_master = DbtRunOperator(
            task_id='run_stg_hcp_master',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_hcp_master',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 3.2 HCP Visits
        run_stg_hcp_visits = DbtRunOperator(
            task_id='run_stg_hcp_visits',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_hcp_visits',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 3.3 Medical Representatives
        run_stg_medical_reps = DbtRunOperator(
            task_id='run_stg_medical_reps',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_medical_representatives',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 3.4 Prescriptions
        run_stg_prescriptions = DbtRunOperator(
            task_id='run_stg_prescriptions',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_prescriptions',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 3.5 Products
        run_stg_products = DbtRunOperator(
            task_id='run_stg_products',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_products',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 3.6 Territories
        run_stg_territories = DbtRunOperator(
            task_id='run_stg_territories',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='stg_territories',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # Test staging models
        test_staging_models = DbtTestOperator(
            task_id='test_staging_models',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='staging',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # End of staging group
        end_staging = DummyOperator(task_id='end_staging')
        
        # Define staging group dependencies
        start_staging >> [
            run_stg_hcp_master,
            run_stg_hcp_visits,
            run_stg_medical_reps,
            run_stg_prescriptions,
            run_stg_products,
            run_stg_territories
        ] >> test_staging_models >> end_staging
    
    # 4. INTERMEDIATE LAYER: Task Group
    with TaskGroup(group_id='intermediate_layer', tooltip='Intermediate models that join staging models') as intermediate_group:
        # Start of intermediate group
        start_intermediate = DummyOperator(task_id='start_intermediate')
        
        # 4.1 HCP enriched
        run_int_hcp_enriched = DbtRunOperator(
            task_id='run_int_hcp_enriched',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='int_hcp_enriched',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 4.2 HCP visits enriched  
        run_int_hcp_visits_enriched = DbtRunOperator(
            task_id='run_int_hcp_visits_enriched',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='int_hcp_visits_enriched',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 4.3 Prescriptions enriched
        run_int_prescriptions_enriched = DbtRunOperator(
            task_id='run_int_prescriptions_enriched',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='int_prescriptions_enriched',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # Test intermediate models
        test_intermediate_models = DbtTestOperator(
            task_id='test_intermediate_models',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='intermediate',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # End of intermediate group
        end_intermediate = DummyOperator(task_id='end_intermediate')
        
        # Define intermediate group dependencies
        start_intermediate >> [run_int_hcp_enriched, run_int_hcp_visits_enriched, run_int_prescriptions_enriched] >> test_intermediate_models >> end_intermediate
    
    # 5. MARTS LAYER: Task Group
    with TaskGroup(group_id='marts_layer', tooltip='Marts models for final consumption') as marts_group:
        # Start of marts group
        start_marts = DummyOperator(task_id='start_marts')
        
        # 5.1 HCP Dimension
        run_dim_hcp = DbtRunOperator(
            task_id='run_dim_hcp',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='dim_hcp',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 5.2 Visits Fact
        run_fct_visits = DbtRunOperator(
            task_id='run_fct_visits',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='fct_visits',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # 5.3 Prescriptions Fact
        run_fct_prescriptions = DbtRunOperator(
            task_id='run_fct_prescriptions',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='fct_prescriptions',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # Test marts models
        test_marts_models = DbtTestOperator(
            task_id='test_marts_models',
            project_dir='/opt/airflow/dbt',
            profiles_dir='/opt/airflow/dbt',
            select='marts',
            target='dev',
            env_vars=dbt_env_vars
        )
        
        # End of marts group
        end_marts = DummyOperator(task_id='end_marts')
        
        # Define marts group dependencies
        start_marts >> run_dim_hcp >> [run_fct_visits, run_fct_prescriptions] >> test_marts_models >> end_marts
    
    # End of pipeline
    end_pipeline = DummyOperator(task_id='end_pipeline')
    
    # Define the main pipeline dependencies
    start_pipeline >> get_connection >> dbt_debug >> staging_group >> intermediate_group >> marts_group >> end_pipeline 