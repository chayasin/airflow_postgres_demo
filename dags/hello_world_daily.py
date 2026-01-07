"""
Hello World Airflow DAG
This DAG runs daily and prints "Hello World"
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def print_hello():
    """Simple Python function to print Hello World"""
    print("Hello World from Python!")
    return "Hello World"


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='hello_world_daily',
    default_args=default_args,
    description='A simple Hello World DAG that runs daily',
    schedule='@daily',  # Runs every day
    catchup=False,  # Don't run for past dates
    tags=['example', 'hello_world'],
) as dag:

    # Task 1: Print Hello World using Bash
    hello_bash = BashOperator(
        task_id='hello_world_bash',
        bash_command='echo "Hello World from Bash!"',
    )

    # Task 2: Print Hello World using Python
    hello_python = PythonOperator(
        task_id='hello_world_python',
        python_callable=print_hello,
    )

    # Task 3: Print timestamp
    print_timestamp = BashOperator(
        task_id='print_timestamp',
        bash_command='echo "Current timestamp: $(date)"',
    )

    # Set task dependencies
    hello_bash >> hello_python >> print_timestamp
