from pyspark.sql.types import *
from pyspark.sql import SparkSession


BUCKET = "s3-etl"

INPUT_PATH = f"s3a://{BUCKET}/task2/input/online_retail_II.csv"
OUTPUT_PATH = f"s3a://{BUCKET}/task2/output"

spark = SparkSession.builder \
    .appName("create-table") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField("Invoice", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("Price", DoubleType(), True),
    StructField("Customer ID", LongType(), True),
    StructField("Country", StringType(), True)
])

df = spark.read.option("header", "true").schema(schema).csv(INPUT_PATH)

spark.sql("DROP TABLE IF EXISTS result")

df.write.mode("overwrite").option("path", OUTPUT_PATH).saveAsTable("result")