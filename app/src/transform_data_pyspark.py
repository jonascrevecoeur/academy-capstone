from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.getOrCreate()

# The verbose way
fields = [
    StructField("name", StringType(), nullable=True),
    StructField("age", IntegerType(), nullable=True),
]
users = spark.createDataFrame(
    data=[
        ("Wim", 1),
        (None, 2),
    ],
    schema=StructType(fields),
)

users.show()
