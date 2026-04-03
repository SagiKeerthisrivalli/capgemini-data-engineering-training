from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col, row_number, desc
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("FullETLPipeline").getOrCreate()

#  Read CSV files
customers = spark.read.format("csv") \
    .option("header", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .load("/samples/sales.csv")
