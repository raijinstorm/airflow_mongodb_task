from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import Dataset
from datetime import timedelta
import pandas as pd
import os
import logging

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
TEMP_DATA_DIR = os.getenv("TEMP_DATA_DIR", "/opt/airflow/temp")
REVIEWS_CSV_NAME = os.getenv("REVIEWS_CSV_NAME", "tiktok_google_play_reviews")

raw_csv_path = os.path.join(DATA_DIR, f"{REVIEWS_CSV_NAME}.csv")
temp_csv_path_1 = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_1.csv")
temp_csv_path_2 = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_2.csv")
final_csv_path = os.path.join(TEMP_DATA_DIR, f"{REVIEWS_CSV_NAME}_final.csv")

FINAL_CSV_DATASET = Dataset(f"file://{final_csv_path}")

MONGO_CONN_ID= "mongo_default"
MONGO_DB = "airflow_demo_db"
MONGO_COLLECTION = "tiktok_reviews_collection"

default_args = {
    "owner":"airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}

dag = DAG(
    "load_to_mongoDB",
    default_args = default_args,
    schedule=[FINAL_CSV_DATASET],
    catchup=False
)

def load_csv_to_mongo():
    try:    
        df = pd.read_csv(final_csv_path, header = 0)
        
        records = df.to_dict(orient ="records")
        
        hook = MongoHook(mongo_conn_id = MONGO_CONN_ID)
        collection = hook.get_conn()[MONGO_DB][MONGO_COLLECTION]
        
        if records:
            collection.insert_many(records)
            
        logging.info("Data was loaded into MongoDB")
    except Exception as e:
        logging.error(f"Error while inserting data into mongoDB: {e}")


load_to_mongo = PythonOperator(
    task_id = "load_to_mongo",
    python_callable = load_csv_to_mongo,
    dag = dag
)

def clear_temp_files_csv():
    path = TEMP_DATA_DIR
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
    logging.info("Temp data was deleted")


clear_temp_files = PythonOperator(
    task_id = "clear_temp_files",
    python_callable=clear_temp_files_csv,
    dag = dag
)

load_to_mongo >> clear_temp_files



