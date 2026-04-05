from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Bucketizer
from pyspark.sql import Window

spark=SparkSession.builder.appName("app").getOrCreate()
# Reading datasets
customers=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/samples/customers.csv")
sales=spark.read.format('csv').option("header","true").option("inferSchema", "true").load("/samples/sales.csv")
# cleaning datasets
customers_clean=customers.dropna(subset=["customer_id"]).dropDuplicates(["customer_id"])
sales_clean=sales.dropna(subset=["sale_id"]).dropDuplicates(["customer_id"]).filter(col("total_amount")>0)
# Join sales and customers
df = sales.join(customers, "customer_id")
# Calculate total spend per customer
amount=df.groupby("customer_id").agg(sum("total_amount").alias("total_spend"))

#1. Create Gold/Silver/Bronze segmentation using conditional logic
#---- conditional logic----
#If total_spend>10000 then gold
#If total_spend>=5000 and total_spend<=10000 then silver
#Otherwise bronze
segmentation = amount.withColumn(
    "segment",
    when(col("total_spend") > 10000, "Gold")
    .when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")
    .otherwise("Bronze")
)
segmentation.select("customer_id","total_spend","segment").show()
# 2. Group data by segment and count customers
segmentation.groupby("segment").agg(count(col("customer_id"))).show()


# 3. Try quantile-based segmentation
quantiles = amount.approxQuantile("total_spend", [0.33, 0.66], 0)
q1 = quantiles[0]
q2 = quantiles[1]
quantile = amount.withColumn(
    "quantile_segment",
    when(col("total_spend") <= q1, "Bronze")
    .when((col("total_spend") > q1) & (col("total_spend") <= q2), "Silver")
    .otherwise("Gold")
)
quantile.select("total_spend", "quantile_segment").show()

#4. Compare results of different methods
window = Window.orderBy("total_spend")
rank= amount.withColumn("rank_pct", percent_rank().over(window))

# Assign segment based on rank percentage
df_window = rank.withColumn(
    "segment_rank", percent_rank().over(window))

splits = [-float("inf"), 5000, 10000, float("inf")]
bucketizer = Bucketizer(splits=splits, inputCol="total_spend", outputCol="bucket")
bucket= bucketizer.transform(amount)

comparison = segmentation \
    .join(quantile.select("customer_id", "quantile_segment"), "customer_id") \
    .join(bucket.select("customer_id", "bucket"), "customer_id") \
    .join(df_window.select("customer_id","segment_rank"), "customer_id")

comparison.select(
    "customer_id",
    "total_spend",
    "segment",
    "quantile_segment",
  "bucket",
    "segment_rank"
).show()

