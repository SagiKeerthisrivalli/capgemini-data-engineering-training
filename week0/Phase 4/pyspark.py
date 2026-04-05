from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("app").getOrCreate()

# Read datasets
sales=spark.read.format('csv').option("header","true").option("inferSchema", "true").load("/samples/sales.csv")
customers=spark.read.format('csv').option("header","true").option("inferSchema", "true").load("/samples/customers.csv")

#  1.Daily Sales → Output: date, total_sales
sales.groupby("sale_date").agg(sum("total_amount")).show()

#  2.City-wise Revenue → Output: city, total_revenue
df = customers.join(sales, on="customer_id", how="inner")
df.groupBy("city").agg(sum("total_amount").alias("total_spend")).show()

#  3.Top 5 Customers → Output: customer_name, total_spend
df.groupby(concat_ws(" ","first_name","last_name").alias("customer_name")).agg(sum("total_amount").alias("total_spend")).orderBy("total_spend", ascending=False).limit(5).show()

#  4.Repeat Customers (>1 order) → Output: customer_id, order_count
data=customers.join(sales,on="customer_id",how="left")
data.groupby("customer_id") \
.agg(count("sale_id").alias("total_orders")) \
.filter(col("total_orders")>1) \
.show()

#  5.Customer Segmentation
# Step 1: Aggregate first
agg_df = df.groupby(
    concat_ws(" ","first_name","last_name").alias("customer_name")
).agg(
    sum("total_amount").alias("total_spend")
)
# Step 2: Apply segmentation
segmentation = agg_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)
segmentation.show()

#  6.Final Reporting Table
df = sales.join(customers, "customer_id")
name = df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
agg_df = name.groupBy("full_name", "city") \
    .agg(
        sum("total_amount").alias("total_spend"),
        count("sale_id").alias("order_count")
    )
segmentation = agg_df.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)
segmentation.show()
