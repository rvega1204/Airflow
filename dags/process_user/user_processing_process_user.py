from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from datetime import datetime

@dag(
    dag_id="user_processing_process_user",
    #schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["user"]
)
def user_processing_process_user():
    # Task to create the users table if it doesn't exist
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS users (
            id INT PRIMARY KEY,
            firstname VARCHAR(255),
            lastname VARCHAR(255),
            email VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    
    # Sensor task to check if the API is available and fetch fake user data
    @task.sensor(poke_interval=30, timeout=300)
    def is_api_available() -> PokeReturnValue:
        import requests
        response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
        print(response.status_code)
        if response.status_code == 200:
            condition = True
            fake_user = response.json()
        else:
            condition = False
            fake_user = None
        return PokeReturnValue(is_done=condition, xcom_value=fake_user)
    
    # Task to extract relevant user fields from the fake user data
    @task
    def extract_user(fake_user):
        return {
            "id": fake_user["id"],
            "firstname": fake_user["personalInfo"]["firstName"],
            "lastname": fake_user["personalInfo"]["lastName"],
            "email": fake_user["personalInfo"]["email"],
        }
        
    # Task to process user info and write it to a CSV file
    @task
    def process_user(user_info):
        import csv
        
        # Example user info (should use the passed user_info argument)
        user_info = {
            "id": "123",
            "firstname": "John",
            "lastname": "Doe",
            "email": "john.doe@example.com",
        }
        with open("/tmp/user_info.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=user_info.keys())
            writer.writeheader()
            writer.writerow(user_info)
            
    # Define task dependencies
    fake_user = is_api_available()
    user_info = extract_user(fake_user)
    process_user(user_info)
            
user_processing_process_user()