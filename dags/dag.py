from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.utils.edgemodifier import Label
from airflow import Dataset
from datetime import timedelta
import pandas as pd
import os
import logging

FINAL_CSV_DATASET = Dataset("file:///opt/airflow/temp/tiktok_google_play_reviews_final.csv")

default_args = {
    "owner":"???",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

dag = DAG(
    "transform_csv",
    default_args = default_args,
    schedule="@daily",
    catchup=False
)

wait_for_file = FileSensor(
    task_id = "wait_for_file",
    filepath = "/opt/airflow/data/tiktok_google_play_reviews.csv",
    poke_interval = 30,
    timeout = 600,
    fs_conn_id = "fs_default", 
    dag = dag
)


def task_branch(**kwargs):
    try:
        df = pd.read_csv("/opt/airflow/data/tiktok_google_play_reviews.csv")
        if len(df) == 0:
            return "log_empty_file"
        return "transform_df.clean_nulls"
    except:
        logging.warning("File was empty. You can see logs for empty files in logs/file_empty.log")
        return "log_empty_file"

branch = BranchPythonOperator(
    task_id = "branch_task",
    python_callable = task_branch,
    dag = dag
)

log_empty_file = BashOperator(
    task_id = "log_empty_file",
    bash_command = 'echo "$(date) - The csv file was empty" >> /opt/airflow/logs/file_empty.log',
    dag = dag
)

def clean_null_values():
    df = pd.read_csv("/opt/airflow/data/tiktok_google_play_reviews.csv", header=0)
    df = df.fillna("-")
    df = df.replace("null", "-")
    os.makedirs("/opt/airflow/temp", exist_ok=True)
    df.to_csv("/opt/airflow/temp/tiktok_google_play_reviews_1.csv", index = False)
    logging.info("Null values were cleaned")
    

def sort_by_dates():
    df = pd.read_csv("/opt/airflow/temp/tiktok_google_play_reviews_1.csv", header=0)
    # "at" = "created_date"
    df = df.sort_values("at", ascending= False)
    df.to_csv("/opt/airflow/temp/tiktok_google_play_reviews_2.csv", index = False)
    logging.info("Data was sorted by dates")
    

def clean_content_column():
    df = pd.read_csv("/opt/airflow/temp/tiktok_google_play_reviews_2.csv", header=0)
    df["content"] = df["content"].str.replace(r"[^a-zA-z0-9!,.?: ]", " ", regex = True)
    df.to_csv(FINAL_CSV_DATASET.uri.replace("file://", ""), index = False)
    logging.info("Content column was cleaned")
    
@task_group(group_id="transform_df", dag = dag)
def transform_df():
    clean_nulls = PythonOperator(
    task_id = "clean_nulls",
    python_callable = clean_null_values,
    dag = dag
    )
    
    sort_df = PythonOperator(
    task_id = "sort_df",
    python_callable = sort_by_dates,
    dag = dag
    )
    
    clean_content = PythonOperator(
    task_id = "clean_content",
    python_callable = clean_content_column,
    outlets = [FINAL_CSV_DATASET],
    dag = dag
    )
    
    clean_nulls >> sort_df >> clean_content
    
transform_group = transform_df()
    
wait_for_file >> branch >> Label("The file is empty") >>log_empty_file 
branch >> Label("The file is NON-empty") >> transform_group