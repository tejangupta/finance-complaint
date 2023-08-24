from dotenv import load_dotenv
from finance_complaint.constants.environment.variable_key import (AWS_ACCESS_KEY_ID_ENV_KEY,
                                                                  AWS_SECRET_ACCESS_KEY_ENV_KEY)
import os
from pyspark.sql import SparkSession

load_dotenv()

access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)

spark_session = SparkSession.builder.master('local[*]').appName('finance_complaint') \
    .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0') \
    .config('spark.hadoop.fs.s3a.access.key', access_key_id) \
    .config('spark.hadoop.fs.s3a.secret.key', secret_access_key) \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config('spark.hadoop.fs.s3a.endpoint', 's3.ap-south-1.amazonaws.com') \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider',
            'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config('spark.hadoop.com.amazonaws.services.s3.enableV4', 'true') \
    .getOrCreate()

# Optional configurations
# spark_session.conf.set("spark.executor.instances", "1")
# spark_session.conf.set("spark.executor.memory", "6g")
# spark_session.conf.set("spark.driver.memory", "6g")
# spark_session.conf.set("spark.executor.memoryOverhead", "8g")

# Now you can use the spark_session for your processing
