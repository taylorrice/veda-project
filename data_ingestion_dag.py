import datetime
import json
import csv

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

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
     dag_id="data_ingestion",
     start_date=datetime.datetime(2023, 8, 2),
     schedule="@daily",
):
    start = EmptyOperator(task_id="start")

    nyt_data_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_nyt_s3_to_redshift",
        redshift_conn_id=REDSHIFT_CONN_ID,
        s3_bucket=S3_BUCKET,
        s3_key=NYT_S3_KEY,
        table=REDSHIFT_TABLE,
    )

    get_isbn_values = RedshiftSQLOperator(
        task_id='get_isbn_values', sql="SELECT 'isbn13' FROM 'dev'"
    )
    '''
    pull_goodreads_reviews_task = BashOperator(
        task_id="pull_nytimes_data",
        bash_command='curl "http://api.nytimes.com/svc/books/v3/lists/full-overview.json?api-key=An6NraSaBG3oUkAHretwK4m3wZUSK3wZ"',
    )


    nyt_data_to_S3_task = S3CreateObjectOperator(
        task_id="create_nyt_data_s3_object",
        s3_bucket=S3_BUCKET,
        s3_key=NYT_S3_KEY,
        data="{{ ti.xcom_pull(task_ids='pull_nytimes_data') }}",
        replace=True,
    )

    '''
    end = EmptyOperator(task_id="end")
start >> nyt_data_s3_to_redshift >> get_isbn_values >> end
