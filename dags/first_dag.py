from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'file_sensor_and_bash_dag_2',
    default_args=default_args,
    description='A simple DAG with FileSensor and BashOperator',
    schedule='@daily',
    catchup=False,
)

# Task 1: Wait for the file to exist
wait_for_file = FileSensor(
    task_id='wait_for_file',
    fs_conn_id='fs_default',      # Connection id for filesystem
    filepath='/opt/airflow/data/file.txt',
    poke_interval=30,             # Check every 30 seconds
    timeout=600,                  # Timeout after 10 minutes
    dag=dag,
)

# Task 2: Print Success message
print_success = BashOperator(
    task_id='print_success',
    bash_command='echo "Success"',
    dag=dag,
)

# Set task dependencies
wait_for_file >> print_success
