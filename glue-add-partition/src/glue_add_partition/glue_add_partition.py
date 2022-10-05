import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step 1: Read Input S3 bucket as a Spark Data Frame that infers the "%2F" encoded characters automatically 
input_format = 'csv'
input_bucket = 'date-format-test-pke'
input_prefix = 'nytaxi'
input_df = spark.read.option("header","true").format(input_format).load('s3://' + input_bucket + "/" + input_prefix)

# Step 2: Convert the Spark Data Frame into Glue Dynamic Frame 
gluedf = DynamicFrame.fromDF(input_df, glueContext, "gluedf")


# Step 3: Load the input glue dynamic frame to output table using enableUpdateCatalog property set to True
sink = glueContext.write_dynamic_frame.from_catalog(
    frame=gluedf,
    database="default",
    table_name="date_format_nytaxi_new",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE",
        "partitionKeys": ["partition_date"],
    },
    transformation_ctx="sink",
)

job.commit()
