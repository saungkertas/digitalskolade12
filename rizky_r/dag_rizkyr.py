import os
from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'rizkyr',
    'start_date': pendulum.datetime(2023, 5, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    dag_id='dag_rizky',
    schedule_interval='20 2 * * *',
    start_date=pendulum.datetime(2023, 5, 23, tz="Asia/Jakarta"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
)

start = DummyOperator(
    task_id='START',
    dag=dag,
)

end = DummyOperator(
    task_id='END',    
    dag=dag,
)

data_replica = DummyOperator(
    task_id='data_replica',    
    dag=dag,
)

datamart = BashOperator(
    task_id='datamart',
    bash_command='python3 /home/rizky_romadhon22/datamart.py'
)

start >> data_replica >> datamart >> end