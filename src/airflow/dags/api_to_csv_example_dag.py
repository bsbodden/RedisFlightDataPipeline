import airflow
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retry_delay": timedelta(minutes=5),
}

spark_dag = DAG(
    dag_id="api_to_csv_example",
    default_args=default_args,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    description="use case of sparkoperator in airflow",
    start_date=airflow.utils.dates.days_ago(1),
)

extract = SparkSubmitOperator(
    application="/usr/local/spark/applications/api_to_csv_example.py",
    conn_id="spark_local",
    task_id="spark_submit_task",
    dag=spark_dag,
)

extract
