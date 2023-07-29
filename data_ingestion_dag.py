 import datetime

 from airflow import DAG
 from airflow.operators.empty import EmptyOperator

'''
This DAG will pull NYT Book Review data and GoodReads data and compare reviews on popular books.
It will flag any book where the review values differ beyond a given threshold.

'''

 with DAG(
     dag_id="data_ingestion",
     start_date=datetime.datetime(2023, 7, 28),
     schedule="@daily",
 ):
     start = EmptyOperator(task_id="start")

     end = EmptyOperator(task_id="end")

start >> end
