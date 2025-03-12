from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    "ytcomments_dag",
    default_args = default_args,
    description = "Pipeline for extracting comments data from youtube videos."
)

generate_source_files = PythonOperator(
    task_id = "ytcomments_api_generate_source_files",
    python_callable = prepare_video_and_comments_data,
    dag = dag
)

generate_source_files