# BigQuery-Ingestion-Script

Glue Script to Ingest data from BigQuery table (require connection and change variables). This code can also be autogenerated by Glue

This code required to previously create a connection in Glue.

[AWS Glue: Integrate data fron Google BigQuery](https://www.youtube.com/watch?v=n6fkX5LpEYY)

[Push down queries using Google BigQuery Connector for AWS Glue](https://repost.aws/articles/ARvGO4zmZbSkm8RpuWuVso-w/push-down-queries-when-using-the-google-bigquery-connector-for-aws-glue)

Modify your variables in the code you will find those in:

- connection_options: parentProject": "your_bq_project", tables and query.
- path="s3://your-s3-bucket/"
