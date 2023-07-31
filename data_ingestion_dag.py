 import datetime

 from airflow import DAG
 from airflow.operators.empty import EmptyOperator
 from airflow.providers.http.operators.http import SimpleHttpOperator
 from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

'''
This DAG will pull NYT Book Review data and GoodReads data and compare reviews on popular books.
It will flag any book where the review values differ beyond a given threshold.

'''
S3_BUCKET = "s3://veda-project-mwaa"
DATE = datetime.date().isoformat()

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
        s3_key=f"/nyt/{date}/bestsellers.json",
        data=ti.xcom_pull(task_ids='pull_nytimes_data'),
        replace=True,
    )



     end = EmptyOperator(task_id="end")

     start >> pull_nytimes_data >> nyt_data_to_S3_task >> end
