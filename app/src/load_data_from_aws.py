from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pathlib import Path

import os

os.environ['PYSPARK_SUBMIT_ARGS'] = "--packages=org.apache.hadoop:hadoop-aws:3.0.0 pyspark-shell"

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
hadoop_conf = sc._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3aFileSystem")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")

df=spark.read.json("s3a://dataminded-academy-capstone-resources/raw/open_aq/data_part_1.json")
df.show(n = 5)

df.repartition(3).write.json(
    str("resources" + "/" + "demo_data"), mode="overwrite"
)

#print(access_id)