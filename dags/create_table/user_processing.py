from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def _extract_user(ti):
    #fake_user = ti.xcom_pull(task_ids="is_api_available")
    # Simulating the API call to fetch user data
    # In a real scenario, you would use requests or another HTTP client to fetch the data
    import requests
    response = requests.get("https://raw.githubusercontent.com/marclamberti/datasets/refs/heads/main/fakeuser.json")
    fake_user = response.json()
    return {
        "id": fake_user["id"],
        "firstname": fake_user["personalInfo"]["firstName"],
        "lastname": fake_user["personalInfo"]["lastName"],
        "email": fake_user["personalInfo"]["email"],
    }

@dag(
    dag_id="user_processing_create_table",
    #schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["user"]
)
def user_processing_create_table():
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

    # Sensor task to check if the API is available
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
    
    # Task to extract user data from the API response
    extract_user = PythonOperator(
        task_id="extract_user",
        python_callable=_extract_user
    )

    # Call the sensor task (should be used in a dependency chain)
    is_api_available() 
    
user_processing_create_table()