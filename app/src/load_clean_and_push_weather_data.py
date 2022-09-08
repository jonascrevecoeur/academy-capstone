from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

spark = SparkSession.builder.getOrCreate()

def load_data() -> DataFrame:
    spark = SparkSession.getActiveSession()
    return(spark.read.json("resources/demo_data"))

def clean_data(frame: DataFrame):

    frame = frame.withColumn("latitude", psf.col("coordinates.latitude")) \
        .withColumn("longitude", psf.col("coordinates.longitude")) \
        .withColumn("local_date", psf.col("date.local")) \
        .withColumn("utc_date", psf.col("date.utc")) \
        .drop("coordinates", "date")

    frame.printSchema()

    frame.show(n = 5)

clean_data(load_data())