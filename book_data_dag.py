import datetime
import requests
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

'''
This DAG will pull NYT Book Review data and GoodReads data and compare reviews on popular books.
It will flag any book where the review values differ beyond a given threshold.

'''
S3_BUCKET = "veda-project-mwaa"
DATE = datetime.date.today().isoformat()
NYT_S3_KEY = "nyt_data/bestsellers_2010_2019.csv"
GOODREADS_S3_KEY = f"goodreads/{DATE}/reviews.json"
REDSHIFT_TABLE = "default-workgroup.616454454624.us-east-2.redshift-serverless.amazonaws.com:5439/dev"
REDSHIFT_CONN_ID = "redshift_conn_id"


with DAG(
     dag_id="book_data_ingestion",
     start_date=datetime.datetime(2023, 8, 2),
     schedule="@daily",
):
    start = EmptyOperator(task_id="start")

    nyt_data_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_nyt_s3_to_redshift",
        redshift_conn_id=REDSHIFT_CONN_ID,
        schema="public",
        s3_bucket=S3_BUCKET,
        s3_key=NYT_S3_KEY,
        table=REDSHIFT_TABLE,
    )

    get_isbn_values = SQLExecuteQueryOperator(
        task_id='get_isbn_values', sql='SELECT "isbn13" FROM "dev"."public"."bestsellers_nyt_2010_2019";'
    )

    @task(task_id="get_subject_from_isbn")
    def get_subject_from_isbn(**kwargs: Any) -> Dict[List]:
        isbns = kwargs["ti"].xcom_pull(task_ids='get_isbn_values')

        subject_dict_list = [{"isbn13": "book_subjects"}]

        for dict in isbns_json:
            for num in dict.values():
                try:
                    res_json = requests.get(f"https://openlibrary.org/isbn/{num}.json").json()
                    res_json["subjects"]
                    subjects = res_json["subjects"]
                    subject_dict_list.append({num: subjects})
                except ValueError:
                    continue
                except KeyError:
                    subject_dict_list.append({num: None})

        return subject_dict_list

    subject_data_to_Redshift = SQLExecuteQueryOperator(
        task_id="subject_data_to_Redshift",
        sql= "CREATE TABLE subject_by_isbn_airflow FROM {{ ti.xcom_pull(task_ids='get_subject_from_isbn')}}"
    )

    end = EmptyOperator(task_id="end")

start >> nyt_data_s3_to_redshift >> get_isbn_values >> get_subject_from_isbn >> subject_data_to_Redshift >> end
