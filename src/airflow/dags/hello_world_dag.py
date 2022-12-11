# datetime
from datetime import timedelta, datetime

# The DAG object
from airflow import DAG

# Operators
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# initializing the default arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime(2022, 12, 5),
    "retry_delay": timedelta(minutes=5),
}

# Instantiate a DAG object
hello_world_dag = DAG(
    "spark_hello_world",
    default_args=default_args,
    description="Spark Hello World",
    schedule_interval="* * * * *",
    catchup=False,
    tags=["example, helloworld"],
)

# python callable function
def say_hello():
    return "Hello World!"


# Creating first task
start_task = DummyOperator(task_id="start_task", dag=hello_world_dag)

# Creating second task
hello_world_task = PythonOperator(
    task_id="hello_world_task", python_callable=say_hello, dag=hello_world_dag
)

# Creating third task
end_task = DummyOperator(task_id="end_task", dag=hello_world_dag)

# Set the order of execution of tasks.
start_task >> hello_world_task >> end_task
