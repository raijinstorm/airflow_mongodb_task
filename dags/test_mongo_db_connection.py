from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.mongo.hooks.mongo import MongoHook

@dag(
    dag_id="mongo_connection_test_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None,  # This DAG will only run when manually triggered
    doc_md="""
    ### MongoDB Connection Test DAG

    A simple DAG to verify the connection to MongoDB.
    It uses the MongoHook to establish a connection and sends a 'ping' command.
    If the task succeeds, the connection is working.
    """,
    tags=["mongo", "test"],
)
def mongo_connection_test_dag():
    """
    This DAG tests the connection to MongoDB.
    """

    @task
    def check_mongo_connection():
        """
        Connects to MongoDB using MongoHook and pings the server.
        """
        try:
            # Instantiate the hook, which uses the connection with the ID 'mongo_default'
            hook = MongoHook(mongo_conn_id="mongo_default")
            
            # Get the underlying pymongo client
            client = hook.get_conn()
            
            # The 'ping' command is a lightweight way to check if the server is responsive.
            # It will raise an exception if it cannot connect.
            server_info = client.admin.command('ping')
            
            print("Successfully connected to MongoDB!")
            print(f"Server response: {server_info}")
            
            return "Connection successful."

        except Exception as e:
            print(f"Failed to connect to MongoDB: {e}")
            # Re-raise the exception to make the Airflow task fail
            raise

    check_mongo_connection()

# Instantiate the DAG
mongo_connection_test_dag()
