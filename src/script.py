import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Google BigQuery
GoogleBigQuery_node = glueContext.create_dynamic_frame.from_options(connection_type="bigquery", connection_options={"connectionName": "bq_connection", "parentProject": "projectname", "table": "datasetname.tablename"}, transformation_ctx="GoogleBigQuery_node")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node = glueContext.write_dynamic_frame.from_catalog(frame=GoogleBigQuery_node, database="glue_db", table_name="glue_table", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="AWSGlueDataCatalog_node")

job.commit()