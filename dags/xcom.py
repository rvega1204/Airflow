from airflow.sdk import dag, task, Context
from typing import Dict, Any

# This DAG demonstrates how to use XComs to pass data between tasks.
@dag
def xcom_dag():
    # Task t1 pushes data to XCom by returning a dictionary
    @task
    def t1() -> Dict[str, Any]:
        my_val = 42
        my_sentence = "Hello, World!"
        # Return values will be automatically pushed to XCom
        return {
            "my_val": my_val,
            "my_sentence": my_sentence
        }
    
    # Task t2 pulls data from XCom by accepting the output of t1 as input
    @task
    def t2(data: Dict[str, Any]):
        # Print the values received from t1
        print(data['my_val'])
        print(data['my_sentence'])
    
    # Set up task dependencies: t1 runs before t2
    val = t1()
    t2(val)

# Instantiate the DAG
xcom_dag()