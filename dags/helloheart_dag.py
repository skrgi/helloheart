from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import ShortCircuitOperator
import pandas as pd
import json
import psycopg2
from psycopg2 import sql
from psycopg2 import extras
import requests
from config import dbname, user, password, host
from scripts.sql_statements import create_schema_query, max_date_query, create_table_query, insert_query, outcome_results_query, state_results_query, smoothed_results_query, total_results_query

def extract_data():
    def fetch_all_data(url):
        all_data = []
        offset = 0
        limit = 1000

        # API can only be queried for 1000 rows at a time
        while True:
            params = {
                '$offset': offset,
                '$limit': limit
            }
            response = requests.get(url, params=params)
            data = response.json()
            all_data.extend(data)

            if len(data) < limit:
                break

            offset += limit
        return all_data

    conn = None
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        cur = conn.cursor()
        cur.execute(sql.SQL(create_schema_query()))
        conn.commit()

        url = 'https://healthdata.gov/resource/j8mb-icvb.json'
        all_results = fetch_all_data(url)
        if len(all_results) == 0:
            return False

        return json.dumps(all_results, indent=2)
    
    except Exception as e:
        print(f"Error in extract_data: {str(e)}")
        raise
    
    finally:
        if conn is not None:
            conn.close()

def transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_data')

    try:
        df = pd.read_json(data)

        # Convert datetime to string
        for col in df.select_dtypes(include=['datetime64[ns]']).columns:
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        transformed_data = df.to_dict(orient='records')

        return transformed_data
    
    except Exception as e:
        print(f"Error in transform_data: {str(e)}")
        raise

def load_data(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_data')

    conn = None
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        cur = conn.cursor()
        cur.execute(sql.SQL(create_table_query()))

        # Insert into structured table
        data_to_insert = [(item['state'], item['state_name'], item['state_fips'], item['fema_region'], item['overall_outcome'], item['date'], item['new_results_reported'], item['total_results_reported']) for item in transformed_data]
        extras.execute_values(cur, insert_query(), data_to_insert)
        conn.commit()
    
    except Exception as e:
        print(f"Error in load_data: {str(e)}")
        raise
    
    finally:
        if conn is not None:
            conn.close()

def aggregate_data(**kwargs):
    conn = None
    try:
        conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
        cur = conn.cursor()

        #Aggregate results
        cur.execute(sql.SQL(outcome_results_query()))
        cur.execute(sql.SQL(state_results_query()))
        cur.execute(sql.SQL(smoothed_results_query()))
        cur.execute(sql.SQL(total_results_query()))
        conn.commit()

    except Exception as e:
        print(f"Error in aggregate_data: {str(e)}")
        raise
    
    finally:
        if conn is not None:
            conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('healthdata_etl_dag',
          default_args=default_args,
          schedule_interval=timedelta(days=1))

extract_task = ShortCircuitOperator(
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

aggregate_task = PythonOperator(
    task_id='aggregate_data',
    python_callable=aggregate_data,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task >> aggregate_task
