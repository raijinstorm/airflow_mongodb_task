from airflow import DAG
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow import Dataset
from datetime import timedelta
import pandas as pd

FINAL_CSV_DATASET = Dataset("file:///opt/airflow/data/tiktok_google_play_reviews_final.csv")
MONGO_CONN_ID= "mongo_default"
MONGO_DB = "airflow_demo_db"
MONGO_COLLECTION = "tiktok_reviews_collection"

default_args = {
    "owner":"???",
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
        df = pd.read_csv(FINAL_CSV_DATASET.uri.replace("file://", ""), header = 0)
        
        records = df.to_dict(orient ="records")
        
        hook = MongoHook(mongo_conn_id = MONGO_CONN_ID)
        collection = hook.get_conn()[MONGO_DB][MONGO_COLLECTION]
        
        if records:
            collection.insert_many(records)
    except Exception as e:
        print(f"Error while inserting data into mongoDB: {e}")


load_to_mongo = PythonOperator(
    task_id = "load_to_mongo",
    python_callable = load_csv_to_mongo,
    dag = dag
)



