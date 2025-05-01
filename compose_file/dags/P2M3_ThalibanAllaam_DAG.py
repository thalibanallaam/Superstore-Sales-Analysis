'''
=========================================================
Milestone 3

Name  : Thaliban Allaam
Batch : HCK - 025

This program is made to automate the process of loading
and transforming data from PostgreSQL to ElasticSearch. 
The dataset used is the data of sales revenue of a 
Superstore.

=========================================================
'''

# Import Librarie
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
import psycopg2
import re
from elasticsearch import Elasticsearch

# 1. FETCH DATA
def fetch():
    '''
    This function is used to fetch data from PostgreSQL. The data later will be processed
    in the next function.

    Parameters: (none)

    Return
    - P2M3_ThalibanAllaam_data_raw.csv
    '''
    # Start connection between psycopg2 and postgres
    conn = psycopg2.connect(
        dbname="m3db",
        user="postgres",
        password="postgres",
        host="postgres", 
        port="5432"
    )

    # Find the designated data in postgres and save it in "df" variable
    df = pd.read_sql_query("SELECT * FROM table_m3", conn)

    # Export the data in "df" to a csv file
    df.to_csv("/opt/airflow/data/P2M3_ThalibanAllaam_data_raw.csv", index=False)

    # Close connection
    conn.close()

# 2. CLEAN DATA
def clean():
    '''
    This function is used to process the previously fetched data.

    Processes:
    - Normalization
    - Data type conversion (object to datetime)
    - Drop duplicates
    - Drop missing values

    Return
    - P2M3_ThalibanAllaam_data_clean.csv
    '''
    # Read the previously fetched data
    df = pd.read_csv("/opt/airflow/data/P2M3_ThalibanAllaam_data_raw.csv", encoding='latin-1')

    # Function to normalize data
    def normalize_column(col):
        col = col.strip() # Remove whitespaces
        col = re.sub(r'[^\w\s]', '', col) # Remove any characters matching the following regex pattern
        col = col.lower() # Convert all cases to lower case
        col = re.sub(r'\s+', '_', col) # Replace spaces with "_"
        return col

    # Convert data type to datetime
    df['order_date'] =pd.to_datetime(df['order_date'])
    df['ship_date'] =pd.to_datetime(df['ship_date'])

    # Drop duplicates
    df = df.drop_duplicates()

    # Normalize data
    df.columns = [normalize_column(col) for col in df.columns]

    # Drop missing values
    df = df.dropna()

    # Export the processed data to a new csv file
    df.to_csv("/opt/airflow/data/P2M3_ThalibanAllaam_data_clean.csv", index=False)

# 3. POST TO ELASTICSEARCH
def post_elasticsearch():
    # Define connection to ElasticSearch
    es = Elasticsearch("http://elasticsearch:9200")

    # Read the data we want to send
    df = pd.read_csv("/opt/airflow/data/P2M3_ThalibanAllaam_data_clean.csv")

    # Send the data to ElasticSearch
    for _, row in df.iterrows():
        es.index(index="milestone3_clean", body=row.to_dict())

# Define DAG
default_args = {
    'owner': 'thaliban',
    'start_date': datetime(2024, 11, 1),
    'retries': 1
}

with DAG(
    # Define initial parameters
    dag_id='p2m3_data_pipeline',
    default_args=default_args,
    schedule_interval="10-30/10 9 * * 6",
    catchup=False,
    description="Fetch -> Clean -> Post to Elasticsearch") as dag:

    # Define operations within the DAG

    # Fetch Data
    fetch_data = PythonOperator(
        task_id='fetch_from_postgresql',
        python_callable=fetch
    )
    # Clean Data
    clean_data = PythonOperator(
        task_id='data_cleaning',
        python_callable=clean
    )
    # Post Data
    post_data = PythonOperator(
        task_id='post_to_elasticsearch',
        python_callable=post_elasticsearch
    )

    # DAG Flow
    fetch_data >> clean_data >> post_data
