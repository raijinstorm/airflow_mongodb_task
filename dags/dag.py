from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta


default_args = {
    "owner":"???",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

dag = DAG(
    "first_dag_1",
    default_args = default_args,
    schedule="@daily",
    catchup=False
)

wait_for_file = FileSensor(
    task_id = "wait_for_file",
    filepath = "/opt/airflow/data/tiktok_google_play_reviews.csv",
    poke_interval = 30,
    timeout = 600,
    dag = dag
)

def print_file_found():
    for i in range(10):
        print("yo, we found the guy")
        
print_found = PythonOperator(
    task_id = "print_found",
    python_callable = print_file_found,
    dag = dag
)

wait_for_file >> print_found
