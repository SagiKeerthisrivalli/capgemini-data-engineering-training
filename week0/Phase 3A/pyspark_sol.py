from pyspark.sql import SparkSession
from pyspark.sql functions import count,col,sum,avg
spark=SparkSession.builder.getOrCreate()

data = [
(1, "Ravi", "Hyderabad", 25),
(2, None, "Chennai", 32),
(None, "Arun", "Hyderabad", 28),
(4, "Meena", None, 30),
(4, "Meena", None, 30),
(5, "John", "Bangalore", -5)
]
columns = ["customer_id", "name", "city", "age"]
df = spark.createDataFrame(data, columns)

#before cleaning count
print("Before cleaning Rows:", df.count())

# Cleaning data issues (nulls, duplicates, invalid values)
clean_df = df.dropna(subset=["customer_id"])
clean_df = clean_df.fillna({
    "name": "Unknown",
    "city": "Unknown"
})
clean_df = clean_df.dropDuplicates()
clean_df = clean_df.filter(col("age") > 0)

#After cleaning count
print("After cleaning Rows:", clean_df.count())

#Perform aggregation (customers per city)
clean_df.groupBy("city") \
        .agg(count("customer_id").alias("customer_count")) \
        .show()


  
