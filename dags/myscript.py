from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from minio import Minio
import pandas as pd
import io 
import logging
from datetime import datetime


default_args = {
        'owner' : 'Abdo',
        'start_date' : datetime(2022, 11, 12)
}

with DAG(dag_id='DAG-1',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
) as dag:
    extract=BashOperator(
        task_id='Extract',
        bash_command='python /usr/local/airflow/include/scripts/extract.py'
    )
    
    transform = SparkSubmitOperator(
        task_id="Transform",
        application='/usr/local/airflow/include/scripts/transform.py',
        conn_id='my_spark_conn',
        verbose=True
    )

    load = SparkSubmitOperator(
        task_id="Load",
        application='/usr/local/airflow/include/scripts/load.py',
        conn_id='my_spark_conn',
        jars='/usr/local/airflow/include/postgresql-42.7.3.jar',
        verbose=True
    )
   
extract >> transform >> load