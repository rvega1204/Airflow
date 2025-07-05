from airflow.sdk import dag, task

@dag
def branch():
    # Task a: returns the integer 1
    @task
    def a():
        return 1

    # Branching task: chooses which downstream task to run based on value
    @task.branch
    def b(val: int):
        if val == 1:
            return "equal_1"  # If value is 1, run equal_1 task
        return "different_than_1"  # Otherwise, run different_than_1 task

    # Task to run if value is equal to 1
    @task
    def equal_1(val: int):
        print(f"Equal to {val}")

    # Task to run if value is different than 1
    @task
    def different_than_1(val: int):
        print(f"Different than 1: {val}")

    # Define task dependencies
    val = a()
    b(val) >> [equal_1(val), different_than_1(val)]

# Instantiate the DAG
branch()