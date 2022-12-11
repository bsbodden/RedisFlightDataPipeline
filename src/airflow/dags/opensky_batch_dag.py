import os
import tarfile
import urllib.request
from airflow import models
from airflow.utils.dates import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from webdav4.client import Client

import logging

LOGGER = logging.getLogger("airflow.task")


# DAG START DATE
START_DATE = datetime(2022, 12, 1)
# DAG END DATE
END_DATE = None
# Default S3 Bucket
S3_BUCKET = os.environ.get("S3_BUCKET", "opensky")
# Variable with path to temporary data storage on Airflow container
STORAGE = os.environ.get("AIRFLOW_DATA_PATH", "/tmp/data")
# S3 CONNECTION ENV VARIABLE
S3_CONN = os.environ.get("S3_CONN", "seaweedfs_local")
DATE_FOLDER = "2020-05-25"
SEQUENCE = "01"
# URL to retrieve data
URL = f"https://opensky-network.org/datasets/states/{DATE_FOLDER}/{SEQUENCE}/"
# SOURCE FILENAME
SOURCE_FILE = f"states_{DATE_FOLDER}-{SEQUENCE}.avro.tar"
# DESTINATION FILENAME
DESTINATION_FILE = f"states_{DATE_FOLDER}-{SEQUENCE}.avro"
# STORAGE DIRECTORY
STORAGE_DIRECTORY = os.path.join(STORAGE, f"{DATE_FOLDER}/{SEQUENCE}")
# DESTINATION FILENAME
LOCAL_TAR_FILE = os.path.join(STORAGE_DIRECTORY, SOURCE_FILE)
UNTARRED_DEST = os.path.join(STORAGE_DIRECTORY, "untarred")
LOCAL_AVRO_FILE = os.path.join(UNTARRED_DEST, DESTINATION_FILE)
# WEBDAV DESTINATION
WEBDAV_ENDPOINT = "http://seaweedfs-webdav-1:7333"
WEBDAV_DESTINATION = f"/buckets/opensky/{DESTINATION_FILE}"


def download_task(**kwargs) -> None:
    filename = kwargs.get("filename")
    base_url = kwargs.get("base_url")

    url = os.path.join(base_url, filename)

    # Create local storage on first DAG execution
    if not os.path.exists(STORAGE_DIRECTORY):
        os.makedirs(STORAGE_DIRECTORY)

    # Get full path to local file
    file_path = os.path.join(STORAGE_DIRECTORY, filename)

    # Check if file already exits
    is_file = os.path.exists(file_path)

    if not is_file:
        urllib.request.urlretrieve(url, file_path)


def untar_task(**kwargs) -> None:
    LOGGER.info(f">>>> IN untar_task - about to untar {LOCAL_TAR_FILE}")
    dir_list = os.listdir(STORAGE_DIRECTORY)
    # prints all files
    LOGGER.info(f">>>> DIR LIST FOR {STORAGE_DIRECTORY} ==> {dir_list}")
    if not os.path.exists(LOCAL_AVRO_FILE):
        file = tarfile.open(LOCAL_TAR_FILE)
        file.extractall(UNTARRED_DEST)
        file.close()


def upload_to_webdav_task(**kwargs) -> None:
    if os.path.exists(LOCAL_AVRO_FILE):
        LOGGER.info(f">>>> IN upload_to_webdav_task - {WEBDAV_ENDPOINT}")
        client = Client(WEBDAV_ENDPOINT)
        if not client.exists(WEBDAV_DESTINATION):
            LOGGER.info(f">>>> LS / => {client.ls('/')}")
            client.upload_file(LOCAL_AVRO_FILE, WEBDAV_DESTINATION)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": START_DATE,
    "schedule": "@once",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with models.DAG(
    "opensky_batch_dag",
    default_args=default_args,
) as dag:

    start = DummyOperator(
        task_id="start",
        dag=dag,
    )

    download = PythonOperator(
        task_id="download",
        python_callable=download_task,
        provide_context=True,
        op_kwargs={"base_url": URL, "filename": SOURCE_FILE},
        dag=dag,
    )

    untar = PythonOperator(
        task_id="untar",
        python_callable=untar_task,
        provide_context=True,
        op_kwargs={},
        dag=dag,
    )

    upload_to_webdav = PythonOperator(
        task_id="upload_to_webdav",
        python_callable=upload_to_webdav_task,
        provide_context=True,
        op_kwargs={},
        dag=dag,
    )

    # --jars src/spark/jars/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar
    # --packages org.apache.spark:spark-avro_2.12:3.3.1
    # src/spark/applications/opensky_state_vectors_to_redis.py --avro_input_file data/states_2020-05-25-00.avro/states_2020-05-25-00.avro
    avro_to_redis = SparkSubmitOperator(
        task_id="avro_to_redis",
        application="/usr/local/spark/applications/opensky_state_vectors_to_redis.py",
        conn_id="spark_local",
        application_args=[f"--avro_input_file={WEBDAV_ENDPOINT}{WEBDAV_DESTINATION}"],
        jars="/usr/local/spark/jars/spark-redis_2.12-3.1.0-SNAPSHOT-jar-with-dependencies.jar",
        packages="org.apache.spark:spark-avro_2.12:3.3.1",
        dag=dag,
    )

    clean_data = SparkSubmitOperator(
        task_id="clean_data",
        application="/usr/local/spark/applications/clean_data.py",
        conn_id="spark_local",
        dag=dag,
    )

    end = DummyOperator(
        task_id="end",
        dag=dag,
    )

    start >> download >> untar >> upload_to_webdav >> avro_to_redis >> clean_data >> end
