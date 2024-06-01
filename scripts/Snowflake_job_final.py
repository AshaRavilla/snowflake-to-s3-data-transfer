import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake";

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','URL','ACCOUNT','WAREHOUSE','DB', 'SCHEMA', 'USERNAME','PASSWORD'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm,"net.snowflake.spark.snowflake")
#Specifying the connection string
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions ={
"sfURL" : args['URL'],
"sfAccount" : args['ACCOUNT'],
"sfUser" : args['USERNAME'],
"sfPassword" : args['PASSWORD'],
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
}

df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable","EMPDATA").load()
df.show()

# for writing data to the S3 location

df.write.option('header','true').csv("s3a://snowflake-driver-2024/csv/")

job.commit()