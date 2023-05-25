from __future__ import annotations

import datetime

import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id="dag_cahya",
    default_args={
        "owner": "Achmad Dwi Cahya",
        "retries": 2,
        "retry_delay": datetime.timedelta(minutes=5),
    },
    schedule="0 18 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:
    
    #DATA INGESTION
    data_ingestion = EmptyOperator(
        task_id="data_ingestion_UsingAirByte",
    )

    #CREATE DATAMART
    datamart_1 = BashOperator(
        task_id="Create_Datamart_DailyGrossRevenue",
        bash_command="python3 /root/coba2/cahya/cahya_Daily_Gross_Revenue.py",
    )

    datamart_2 = BashOperator(
        task_id="Create_Datamart_GrossRevenuePerProductPerMonth",
        bash_command="python3 /root/coba2/cahya/cahya_Gross_Revenue_Per_Product_Per_Month.py",
    )

    datamart_3 = BashOperator(
        task_id="Create_Datamart_TotalPurchasePerProductPerMonth",
        bash_command="python3 /root/coba2/cahya/cahya_Total_Purchase_Per_Product_Per_Month.py",
    )

    datamart_4 = BashOperator(
        task_id="Create_Datamart_TotalPurchasePerCategoryPerMonth",
        bash_command="python3 /root/coba2/cahya/cahya_Total_Purchase_Per_Category_Per_Month.py",
    )

    datamart_5 = BashOperator(
        task_id="Create_Datamart_TotalPurchasePerCountryPerMonth",
        bash_command="python3 /root/coba2/cahya/cahya_Total_Purchase_Per_Country_Per_Month.py",
    )
    

    #DATA VISUALIZATION
    data_visualization_1 = DummyOperator(
        task_id="data_visualization_DailyGrossRevenue",
    )

    data_visualization_2 = DummyOperator(
        task_id="data_visualization_GrossRevenuePerProductPerMonth",
    )

    data_visualization_3 = DummyOperator(
        task_id="data_visualization_TotalPurchasePerProductPerMonth",
    )

    data_visualization_4 = DummyOperator(
        task_id="data_visualization_TotalPurchasePerCategoryPerMonth",
    )

    data_visualization_5 = DummyOperator(
        task_id="data_visualization_TotalPurchasePerCountryPerMonth",
    )

    data_ingestion >> [datamart_1, datamart_2, datamart_3, datamart_4, datamart_5]
    datamart_1 >> data_visualization_1
    datamart_2 >> data_visualization_2
    datamart_3 >> data_visualization_3
    datamart_4 >> data_visualization_4
    datamart_5 >> data_visualization_5
    