from airflow.sdk import dag, task

# Define the DAG using the @dag decorator
@dag
def sql_dag():

    # Define a SQL task to count the number of XComs in the database
    @task.sql(
        conn_id="postgres"  # Connection ID for the Postgres database
    )
    def get_nb_xcoms():
        """
        Returns the number of XComs in the database.
        """
        # SQL query to count rows in the xcom table
        return "SELECT COUNT(*) FROM xcom"
    
    # Call the SQL task
    get_nb_xcoms()

# Instantiate the DAG
sql_dag()