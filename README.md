# BigQuery to AWS Data Pipeline

Automated data pipeline that extracts data from Google BigQuery and loads it into AWS S3 using AWS Glue, with daily scheduling via Step Functions and EventBridge. Deployed using AWS CDK.

![Architecture Diagram](images/diagram.png)

## Architecture Overview

This solution creates a serverless ETL pipeline that:
- Extracts data from BigQuery tables
- Transforms and stores data in S3 as Parquet files
- Updates AWS Glue Data Catalog automatically
- Runs on a daily schedule (configurable)

## Resources Deployed

| Resource | Purpose |
|----------|----------|
| **S3 Bucket** | Stores Glue scripts and output data |
| **Glue Job** | Executes ETL process from BigQuery to S3 |
| **Glue Database & Table** | Data catalog for schema management |
| **Secrets Manager** | Stores BigQuery service account credentials |
| **Step Functions** | Orchestrates job execution |
| **EventBridge Rule** | Daily scheduling (11:15 PM CST) |
| **IAM Roles** | Secure access permissions |

## ETL Process

The `script.py` performs these steps:
1. Connects to BigQuery using stored credentials
2. Executes SQL query against specified table
3. Converts data to AWS Glue DynamicFrame
4. Writes data to S3 in Parquet format
5. Updates Glue Data Catalog with metadata

## Prerequisites

### 1. BigQuery Connection Setup
Create a BigQuery connection in AWS Glue Console following this guide:
[Setting up BigQuery connections in AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-bigquery)

### 2. Required Configuration Updates
Before deployment, update these parameters in the code:

- **S3 Bucket Name**: Update `bucketName` in `lib/bigqueryingestiontoaws.ts`
- **bigQuerySecret Secret values**: Replace credentials in Secrets Manager section in `lib/bigqueryingestiontoaws.ts`
- **BigQuery Project**: Update `project_id` in `src/script.py`
- **BigQuery Table**: Update `table` parameter in `src/script.py`
- **Connection Name**: Update `connectionName` to match your Glue connection in `src/script.py`


### 3. AWS Prerequisites
- AWS CLI installed and configured
- AWS CDK v2 installed
- Appropriate AWS permissions for resource creation

## Deployment

```bash
# Install dependencies
npm install

# Bootstrap CDK (first time only)
cdk bootstrap

# Deploy the stack
cdk deploy
```

## Configuration

### Schedule Modification
To change the execution schedule, modify the EventBridge rule in `lib/bigqueryingestiontoaws.ts`:
```typescript
schedule: events.Schedule.cron({ minute: '15', hour: '5' }) // 5:15 AM UTC = 11:15 PM CST
```

### Query Customization
Update the SQL query in `src/script.py`:
```python
"query": "SELECT * FROM `your-project.dataset.table`;"
```

## Important Notes

> **⚠️ Security**: Replace the hardcoded service account credentials with your own before deployment

> **🗑️ Development Setup**: S3 bucket has `RemovalPolicy.DESTROY` - change for production use

> **🔄 Extensibility**: Architecture supports multiple BigQuery sources and destinations

> **📊 Monitoring**: Check CloudWatch logs for job execution status and errors