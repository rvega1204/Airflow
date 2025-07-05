from airflow.sdk import dag, task, task_group

@dag
def group():
    # Define a simple task 'a' that returns 42
    @task
    def a():
        return 42

    # Define a task group with default_args (retries=2)
    @task_group(default_args={
        "retries": 2
    })
    def my_group(val: int):

        # Task 'b' prints the sum of its input and 42
        @task
        def b(my_val: int):
            print(my_val + 42)

        # Nested task group with its own default_args (retries=3)
        @task_group(default_args={
            "retries": 3
        })
        def my_nested_group():

            # Task 'c' prints "c"
            @task
            def c():
                print("c")

            c()  # Call task 'c' inside the nested group

        # Set dependencies: 'b' runs before 'my_nested_group'
        b(val) >> my_nested_group()

    val = a()  # Call task 'a' and store its result
    my_group(val)  # Call the task group with the result of 'a'

group()