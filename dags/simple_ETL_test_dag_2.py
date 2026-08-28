from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,  # Number of retries before the task fails
    'retry_delay': timedelta(minutes=5)  # Delay between retries
}


# Testing
# Function to simulate data extraction
def extract_data():
    print("Extracting data...")  # Here you would write the logic to extract data


# Function to simulate data transformation
def transform_data():
    print("Transforming data...")  # Logic for transforming data goes here


# Function to simulate loading data
def load_data():
    print("Loading data...")  # Logic for loading data into a database or destination


# test
# Define the DAG and its schedule
with DAG(
        'simple_ETL_test_dag_2',
        default_args=default_args,
        description='A simple ETL test DAG',
        schedule_interval='@daily',
        start_date=datetime(2023, 1, 1),
        catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')

    # Define the ETL tasks
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # Set the task dependencies
    start >> extract >> transform >> load
