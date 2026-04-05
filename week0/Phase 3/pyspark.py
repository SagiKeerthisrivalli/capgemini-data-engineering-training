from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, col, row_number, desc
from pyspark.sql.window import Window
spark = SparkSession.builder.appName("ETLPipeline").getOrCreate()

#  Read CSV files
customers = spark.read.format("csv") \
    .option("header", "true") \
    .load("/samples/customers.csv")

sales = spark.read.format("csv") \
    .option("header", "true") \
    .load("/samples/sales.csv")

# Cleaning the data
customers_new = customers.dropna(subset=["customer_id", "city"])
sales_new = sales.dropna(subset=["customer_id", "total_amount", "sale_date"]) \
                   .filter(col("total_amount") > 0)
df = sales_new.join(customers_new, on="customer_id", how="inner")

# Calculating daily sales
print("Calculating daily sales")
daily_sales = df.dropna(subset=["sale_date", "total_amount"]) \
                .groupBy("sale_date") \
                .agg(
                    sum("total_amount").alias("total_sales")
                )
daily_sales.show()

# Calculating City wise Revenue
city_revenue = df.dropna(subset=["city", "total_amount"]) \
                 .groupBy("city") \
                 .agg(sum("total_amount").alias("city_total_revenue"))
print("City wise Revenue")
city_revenue.show()

#Repeat customers
repeat_customers = df.groupBy("customer_id") \
                     .agg(count("*").alias("order_count")) \
                     .filter(col("order_count") > 2)
print("Repeat customers")
repeat_customers.show()

# Highest Spending Customer per City
customer_spend = df.groupBy("customer_id", "city") \
                   .agg(sum("total_amount").alias("total_spend"))

windowSpec = Window.partitionBy("city").orderBy(desc("total_spend"))

top_customers = customer_spend.withColumn("rank", row_number().over(windowSpec)) \
                              .filter(col("rank") == 1) \
                              .drop("rank")

print("Highest Spending Customer per City")
top_customers.show()

#Final Reporting Table
final_report = df.groupBy("customer_id", "city") \
                 .agg(
                     sum("total_amount").alias("total_spend"),
                     count("*").alias("order_count")
                 )

print("Final Reporting Table")
final_report.show()
