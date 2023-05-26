#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Example DAG demonstrating the usage of the BashOperator."""
from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Kevin',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="UTC"),
    'email': ['kevinity310@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': datetime.timedelta(minutes=5)
}

with DAG(
    dag_id="dag_kevin",
    schedule="0 0 * * *",
    default_args=default_args,
    description="DAG for final project digital skola",
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    data_ingestion = EmptyOperator(
        task_id="ingest_data_airbyte",
    )
    
    datamart_layer_1 = BashOperator(
        task_id="gross_revenue_daily_task",
        bash_command="python3 /root/airflow/dags/kevin/gross_revenue_daily.py",
    )
    
    datamart_layer_2 = BashOperator(
        task_id="gross_revenue_product_task",
        bash_command="python3 /root/airflow/dags/kevin/gross_revenue_product_monthly.py",
    )
    
    datamart_layer_3 = BashOperator(
        task_id="total_order_product_task",
        bash_command="python3 /root/airflow/dags/kevin/total_order_per_product_monthly.py",
    )
    
    datamart_layer_4 = BashOperator(
        task_id="total_order_category_task",
        bash_command="python3 /root/airflow/dags/kevin/total_order_per_category_mothly.py",
    )
    
    datamart_layer_5 = BashOperator(
        task_id="total_order_country_task",
        bash_command="python3 /root/airflow/dags/kevin/total_order_per_country_mothly.py",
    )

    
    data_ingestion >> [datamart_layer_1, datamart_layer_2,datamart_layer_3, datamart_layer_4, datamart_layer_5]


if __name__ == "__main__":
    dag.test()
