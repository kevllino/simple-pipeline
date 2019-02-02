from datetime import timedelta

import os

import sys
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

os.environ["SPARK_HOME"] = "/usr/local/spark-2.3.2"
sys.path.append(os.path.join(os.environ['SPARK_HOME'], 'bin'))

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2019-01-01',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'project_id': 'local_dag',
    'schedule_interval': None  # setup to a daily cron task later
}

ZIP_PATH = "simple-pipe.zip"

with DAG('main_dag', default_args=DEFAULT_DAG_ARGS) as dag:

    t_add = SparkSubmitOperator(
        task_id='add',
        application="spark_jobs/add_job.py",  # main?
        py_files=ZIP_PATH,
        application_args=["--run_date", "{{ execution_date }}"],
    )

    t_add