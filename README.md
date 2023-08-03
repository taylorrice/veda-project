# veda-project
* This code sample is a work-in-progress that utilized AWS managed services and serverless databases to pull information about NYT Bestselling books and looking for trends in the subject matter of those bestsellers.
* `mwaa-public-network.yml` is a generic CloudFormation Template that sets up a basic Managed Workflow for Apache Airflow. This runs successfully and provides everything you need to run Airflow in AWS.

## Airflow Pipeline
* `book_data_dag.py` is an Airflow Pipeline that pulls the historical bestseller data (in csv format) from S3 and moves it into a Redshift table. If this were to continuously run, that Redshift operation would be an Append so as to not lose historical data.
* Once the bestseller data is in Redshift, it pulls only the isbn data, and uses that to make a request to the OpenLibrary API which contains more details about each book. If that data contains a list of subjects relating to the book, that is pulled and associated with that isbn.
* Once we have this list of subjects and associated books, push into a new Redshift table.
* This completes the automated portion of the pipeline.

## Redshift Querying
* Serverless Redshift provides an interactive console for running queries and creating tables
* From here, you can leverage the unique isbns to look for patterns between time/rank on the bestseller list and book subject matter.
* These queries can also be automated using boto3 (and potentially even added to the airflow pipeline) depending on use-case and data consumer requirements.

## Diagram of Project Workflow
![Screenshot 2023-08-03 at 12 55 37 AM](https://github.com/taylorrice/veda-project/assets/21068202/73fdecf2-85a7-468f-b4ce-d74bd2ce55f5)





