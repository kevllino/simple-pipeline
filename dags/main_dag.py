from datetime import timedelta, datetime

from airflow import DAG
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, DataprocClusterDeleteOperator, \
    DataProcPySparkOperator

from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

BUCKET = Variable.get('gcs_bucket')  # GCS bucket with our data.

DAG_VERSION = 4

DEFAULT_DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'ignore_first_depends_on_past':True,
    'wait_for_downstream': True,
    'start_date': datetime.utcnow(),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
    'project_id': Variable.get('gcp_project'),
    'schedule_interval': None  # setup to a daily cron task later
}

ZIP_PATH = 'gs://' + BUCKET + "/artifacts/simple-pipe.zip"

with DAG('main_dag_{}'.format(DAG_VERSION), default_args=DEFAULT_DAG_ARGS, catchup=False, concurrency=2, max_active_runs=2) as dag:
    t_create_cluster = DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        # ds_nodash is an airflow macro for "[Execution] Date string no dashes"
        # in YYYYMMDD format. See docs https://airflow.apache.org/code.html?highlight=macros#macros
        cluster_name='simple-pipeline',
        master_machine_type='n1-standard-2',
        worker_machine_type='n1-standard-2',
        num_workers=2,
        zone='asia-south1-c'
        #init_actions_uris=['gs://' + BUCKET + '/scripts/install_airflow.sh']
    )
    t_add = DataProcPySparkOperator(
        task_id='add',
        main='gs://' + BUCKET + "/spark_jobs/add_job.py",  # main?
        pyfiles=ZIP_PATH,
        arguments=["--run_date={{ execution_date }}", "--bucket="+BUCKET],
        cluster_name='simple-pipeline',
    )

    t_substract = DataProcPySparkOperator(
        task_id='substract',
        main='gs://' + BUCKET + "/spark_jobs/substract_job.py",  # main?
        pyfiles=ZIP_PATH,
        arguments=["--run_date={{ execution_date }}", "--bucket="+BUCKET],
        cluster_name='simple-pipeline',
    )

    t_join = DataProcPySparkOperator(
        task_id='join',
        main='gs://' + BUCKET + "/spark_jobs/join.py",  # main?
        pyfiles=ZIP_PATH,
        arguments=["--run_date={{ execution_date }}", "--bucket=" + BUCKET],
        cluster_name='simple-pipeline',
    )

    t_delete_cluster = DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        # Obviously needs to match the name of cluster created in the prior two Operators.
        cluster_name='simple-pipeline',
        # This will tear down the cluster even if there are failures in upstream tasks.
        trigger_rule=TriggerRule.ALL_DONE
    )

    t_create_cluster.set_downstream([t_add, t_substract])
    t_join.set_upstream([t_add, t_substract])
    t_join.set_downstream([t_delete_cluster])