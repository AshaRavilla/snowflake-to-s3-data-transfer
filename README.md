# Snowflake to S3 Data Transfer using AWS Glue

## Overview
This project demonstrates how to transfer data from Snowflake to Amazon S3 using Spark and AWS Glue.

## Prerequisites
- AWS Account with necessary permissions for Glue, S3, and IAM.
- Snowflake account with database and table setup.
- Python environment to run the Glue job script.
- AWS CLI installed and configured.

## Snowflake Setup
1. **Create a database and table in Snowflake:**
    ```sql
    CREATE OR REPLACE TABLE MYDB.PUBLIC.EMPDATA (
        ID VARCHAR(16777216),
        FIRST_NAME VARCHAR(16777216),
        LAST_NAME VARCHAR(16777216),
        EMAIL VARCHAR(16777216),
        GENDER VARCHAR(16777216),
        IP_ADDRESS VARCHAR(16777216)
    );
    ```

2. **Insert data into the Snowflake table:**
    - Generate 1000 rows of data and save it as `generated_employee_data.csv`.
    - Upload the CSV file to Snowflake using the Snowflake UI or SnowSQL.

## AWS Glue Setup
1. **Upload JAR Files to S3:**
    - `snowflake-jdbc-3.13.22.jar`
    - `spark-snowflake_2.12-2.13.0-spark_3.3.jar`

2. **Create and Configure Glue Job:**
    - Navigate to the AWS Glue console.
    - Create a new Glue job and add the following parameters:
      ```plaintext
      --extra-jars s3://your-bucket/spark-snowflake_2.12-2.13.0-spark_3.3.jar,s3://your-bucket/snowflake-jdbc-3.13.22.jar
      ```

3. **Glue Job Script:**
    Save the following script as `scripts/snowflake_to_s3_glue_job.py`:
    ```python
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

    # @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'URL', 'ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'
    ])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    sfOptions = {
        "sfURL": args['URL'],
        "sfAccount": args['ACCOUNT'],
        "sfUser": args['USERNAME'],
        "sfPassword": args['PASSWORD'],
        "sfDatabase": args['DB'],
        "sfSchema": args['SCHEMA'],
        "sfWarehouse": args['WAREHOUSE'],
    }

    # Reading data from Snowflake table
    df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "EMPDATA") \
        .load()

    # Writing data to S3
    df.write.mode("overwrite").parquet("s3://your-bucket/employeedata/")

    df.show()

    job.commit()
    ```

## Running the Job
1. **Start the Glue Job:**
    - Use the AWS Glue console or AWS CLI to start the job.
    - Example using AWS CLI:
    ```sh
    aws glue start-job-run --job-name your-job-name --arguments '--extra-jars=s3://your-bucket/spark-snowflake_2.12-2.13.0-spark_3.3.jar,s3://your-bucket/snowflake-jdbc-3.13.22.jar'
    ```

2. **Monitor the Job:**
    - Check the logs in CloudWatch to ensure the job runs successfully.

## Issues Faced and Solutions
1. **Driver Compatibility:**
    - Initially used an incompatible driver (`spark-snowflake_2.11-2.9.3-spark_2.4.jar`).
    - Resolved by using the correct driver (`spark-snowflake_2.12-2.13.0-spark_3.3.jar`).

## Conclusion
This project successfully demonstrates how to transfer data from Snowflake to Amazon S3 using Spark and AWS Glue.

## References
- [Snowflake Spark Connector](https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.13.0-spark_3.3/)
- [Snowflake JDBC Driver](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.13.22/)
