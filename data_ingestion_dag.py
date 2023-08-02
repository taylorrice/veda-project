import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

'''
This DAG will pull NYT Book Review data and GoodReads data and compare reviews on popular books.
It will flag any book where the review values differ beyond a given threshold.

'''
S3_BUCKET = "s3://veda-project-mwaa"
DATE = datetime.date().isoformat()
NYT_S3_KEY = f"/nyt/{DATE}/bestsellers.json"
GOODREADS_S3_KEY = f"/goodreads/{DATE}/reviews.json"
REDSHIFT_TABLE = "default-workgroup.616454454624.us-east-2.redshift-serverless.amazonaws.com:5439/dev"

with DAG(
     dag_id="data_ingestion",
     start_date=datetime.datetime(2023, 7, 28),
     schedule="@daily",
):
 start = EmptyOperator(task_id="start")

 pull_nytimes_data_task = SimpleHttpOperator(
    task_id="pull_nytimes_data",
    method="GET",
    endpoint="http://api.nytimes.com/svc/books/v3/lists/current/full-overview.json?api-key=An6NraSaBG3oUkAHretwK4m3wZUSK3wZ",
    response_filter=lambda response: response.json()["results"]["lists"]["list_id","books"]["author","rank","title","description","weeks_on_list"],
    dag=dag,
)

nyt_data_to_S3_task = S3CreateObjectOperator(
    task_id="create_nyt_data_s3_object",
    s3_bucket=S3_BUCKET,
    s3_key=NYT_S3_KEY,
    data=ti.xcom_pull(task_ids='pull_nytimes_data'),
    replace=True,
)

nyt_data_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id="transfer_nyt_s3_to_redshift",
    redshift_conn_id=conn_id_name,
    s3_bucket=S3_BUCKET,
    s3_key=NYT_S3_KEY,
    schema="PUBLIC",
    table=REDSHIFT_TABLE,
    copy_options=["json"],
)

get_booklist_task = RedshiftSQLOperator(
    task_id='get_booklist',
    sql= "",### WRITE SQL
)

pull_goodreads_data_task = SimpleHttpOperator(
    task_id="pull_goodreads_data",
    method="GET",
    endpoint="http://api.nytimes.com/svc/books/v3/lists/current/full-overview.json?api-key=An6NraSaBG3oUkAHretwK4m3wZUSK3wZ",
    response_filter=lambda response: response.json()["results"]["lists"]["list_id","books"]["author","rank","title","description","weeks_on_list"],
    dag=dag,
)

goodreads_data_to_S3_task = S3CreateObjectOperator(
    task_id="create_goodreads_data_s3_object",
    s3_bucket=S3_BUCKET,
    s3_key=GOODREADS_S3_KEY,
    data=ti.xcom_pull(task_ids='pull_goodreads_data'),
    replace=True,
)

goodreads_data_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id="transfer_goodreads_s3_to_redshift",
    redshift_conn_id=conn_id_name,
    s3_bucket=S3_BUCKET,
    s3_key=GOODREADS_S3_KEY,
    schema="PUBLIC",
    table=REDSHIFT_TABLE,
    copy_options=["json"],
)

transform_data_task = RedshiftSQLOperator(

)

 end = EmptyOperator(task_id="end")

 start >> pull_nytimes_data >> nyt_data_to_S3_task >> nyt_data_s3_to_redshift_task >> end #get_booklist_task
 #get_booklist_task >> pull_goodreads_data_task >> goodreads_data_to_S3_task >> goodreads_data_s3_to_redshift_task
