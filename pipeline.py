from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pyodbc
import pandas as pd
from sqlalchemy import create_engine

# Define the connection parameters for MS SQL Server
ms_sql_server_conn_str = "Driver={SQL Server};Server=<server_name>;Database=<database_name>;Uid=<username>;Pwd=<password>"
ms_sql_server_query = "<your_sql_query>"

# Define the connection parameters for Azure SQL Server
azure_sql_server_conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=<server_name>.database.windows.net;Database=<database_name>;Uid=<username>;Pwd=<password>"
azure_sql_server_table_name = "<target_table_name>"

# Extract data from MS SQL Server
def extract_data():
    ms_sql_server_conn = pyodbc.connect(ms_sql_server_conn_str)
    data = pd.read_sql(ms_sql_server_query, ms_sql_server_conn)
    ms_sql_server_conn.close()
    return data

# Transform data
def transform_data(data):
    transformed_data = data.copy()  # Make a copy of the data to avoid modifying the original
    
    # Example transformation: Convert a column to uppercase
    transformed_data['Column_Name'] = transformed_data['Column_Name'].str.upper()
    
    # Example transformation: Add a new calculated column
    transformed_data['New_Column'] = transformed_data['Column_A'] + transformed_data['Column_B']
    
    return transformed_data

# Load data into Azure SQL Server
def load_data(transformed_data):
    azure_sql_server_conn = create_engine(azure_sql_server_conn_str)
    transformed_data.to_sql(azure_sql_server_table_name, azure_sql_server_conn, if_exists='replace', index=False)
    azure_sql_server_conn.dispose()

# Define the DAG
dag = DAG(
    'etl_pipeline',
    description='Automated ETL Pipeline',
    schedule_interval='0 8 * * *',  # Run the pipeline every day at 8:00 AM
    start_date=datetime(2023, 7, 2),  # Define the start date of the DAG
    catchup=False  # Disable backfilling for past dates
)

# Define the tasks in the DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

# Set the task dependencies
extract_task >> transform_task >> load_task
