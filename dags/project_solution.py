from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os

# Define the base path for the project (specified in docker-compose.yaml)
BASE_PATH = os.getenv('BASE_PATH', '/opt/airflow/dags')

# Description of the DAG (Directed Acyclic Graph)
"""
This DAG defines a multi-step pipeline for processing data in a Data Lake.

Pipeline steps:
1. Landing to Bronze: Loading and preprocessing data.
2. Bronze to Silver: Cleaning and preparing data for analytics.
3. Silver to Gold: Aggregating and storing final data in the Gold Layer.

Uses:
- BashOperator to execute Python scripts at each stage.

"""

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': days_ago(1),  # Start date of the DAG (yesterday for easier testing)
}

# Initialize the DAG
dag = DAG(
    'zhenya_datsenko_etl_pipeline', 
    default_args=default_args, 
    description='ETL pipeline Data Lake.',  
    schedule_interval=None,  
    tags=['zhenya_datsenko'],  
)

# Task to run landing_to_bronze.py
landing_to_bronze = BashOperator(
    task_id='landing_to_bronze',  
    bash_command=f'python {BASE_PATH}/landing_to_bronze.py', 
    dag=dag,  
)

# Task to run bronze_to_silver.py
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',  
    bash_command=f'python {BASE_PATH}/bronze_to_silver.py',  
    dag=dag,  
)

# Task to run silver_to_gold.py
silver_to_gold = BashOperator(
    task_id='silver_to_gold', 
    bash_command=f'python {BASE_PATH}/silver_to_gold.py', 
    dag=dag, 
)

# Define the sequence of task execution
"""
landing_to_bronze >> bronze_to_silver >> silver_to_gold

This order specifies that:
1. First, landing_to_bronze is executed (loading data into the Bronze Layer).
2. Then, bronze_to_silver is executed (cleaning data in the Silver Layer).
3. Finally, silver_to_gold is executed (aggregating data in the Gold Layer).
"""
landing_to_bronze >> bronze_to_silver >> silver_to_gold