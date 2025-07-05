from airflow.sdk import dag, task
from time import sleep

# This DAG demonstrates the use of CeleryExecutor with tasks that simulate a delay.
# It includes four tasks: `a`, `b`, `c`, and `d`.
@dag
def celery_dag():
    
    @task
    def a():
        sleep(5)
        
    @task(
            queue='high_cpu'  # This task will run on a worker with high CPU resources
    )
    def b():
        sleep(5)

    @task(
            queue='high_cpu'
    )
    def c():
        sleep(5)
        
    @task
    def d():
        sleep(5)
        
    a() >> [b(), c()] >> d()
    

celery_dag()