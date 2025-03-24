from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from python_youtube_api.ytcomments_api import prepare_video_and_comments_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 6),
    "email": ["marek.k.swiderek@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    "ytcomments_data_pipeline",
    default_args = default_args,
    description = "Pipeline for extracting comments data from youtube videos."
)

generate_source_files = PythonOperator(
    task_id = "generate_source_files",
    python_callable = prepare_video_and_comments_data,
    dag = dag
)

wh_drop_database = SQLExecuteQueryOperator(
    task_id = "wh_drop_database",
    conn_id = "postgresql_conn_postgres_db",
    sql = "DROP DATABASE IF EXISTS video_comments_wh;",
    split_statements = True,
    autocommit = True
)

wh_create_database = SQLExecuteQueryOperator(
    task_id = "wh_create_database",
    conn_id = "postgresql_conn_postgres_db",
    sql = "CREATE DATABASE video_comments_wh WITH OWNER = postgres ENCODING = 'UTF8' TABLESPACE = pg_default CONNECTION LIMIT = -1;",
    split_statements = True,
    autocommit = True
)

wh_init_schemas = SQLExecuteQueryOperator(
    task_id = "wh_init_schemas",
    conn_id = "postgresql_conn_wh",
    sql = ["sql_scripts/02_initialize_schemas.sql"]
)

wh_create_bronze_layer_tables = SQLExecuteQueryOperator(
    task_id = "wh_create_bronze_layer_tables",
    conn_id = "postgresql_conn_wh",
    sql = ["sql_scripts/03_create_bronze_tables.sql"]
)

wh_load_bronze_layer_data = SQLExecuteQueryOperator(
    task_id = "wh_load_bronze_layer_data",
    conn_id = "postgresql_conn_wh",
    sql = ["sql_scripts/04_load_bronze_layer.sql"]
)

wh_create_dbt_role = SQLExecuteQueryOperator(
    task_id = "wh_create_dbt_role",
    conn_id = "postgresql_conn_wh",
    sql = ["sql_scripts/05_create_dbt_role.sql"]
)

dbt_source_tests = BashOperator(
    task_id = "dbt_source_tests",
    bash_command = 'dbt test --project-dir ${AIRFLOW_HOME}/ytcomments_dags/dbt_files/ --select "source:*"'
)

dbt_singular_tests = BashOperator(
    task_id = "dbt_singular_tests",
    bash_command = 'dbt test --project-dir ${AIRFLOW_HOME}/ytcomments_dags/dbt_files/ --select "test_type:singular"',
    trigger_rule = "all_done"
)

dbt_run = BashOperator(
    task_id = "dbt_run",
    bash_command = 'dbt run --project-dir ${AIRFLOW_HOME}/ytcomments_dags/dbt_files/'
)

generate_source_files >> [wh_drop_database, wh_create_database] >> wh_init_schemas >> wh_create_bronze_layer_tables >> wh_load_bronze_layer_data >> wh_create_dbt_role >> [dbt_source_tests, dbt_run] >> dbt_singular_tests