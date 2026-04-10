## Objective:
The main objective of this day was to perform data analysis using PySpark on given datasets. The goal is to understand given data from datsaets and analyse the data and to print the required output.

Additionally, the project aimed to strengthen skills in data transformation, joins, aggregations, and window functions, which are essential for real-world data engineering tasks.

## Implementation:
1. Data Loading
Loaded multiple datasets using Spark:
2. Data Processing Steps
Joined multiple tables:
orders + customers + order_items
Calculated: total customer spend, daily sales, product-level sales
3. Window Functions
Applied ranking:
Used:
rank()
dense_rank()
4. Aggregations
Used:
groupBy()
sum()
count()
5. Data Transformation
Converted timestamp → date
Created new columns:
segments (Gold, Silver, Bronze)
6. Final Output
Created a final dataset containing:customer_id, city, total spend, segment, order count

## Key Learnings:
1.Understanding of joins in PySpark
2.Usage of window functions (rank, dense_rank)
3.Performing aggregations on large datasets
4.Handling date transformations
5.Importance of data cleaning before analysis
6.Difference between:
rank() vs dense_rank()

## Challenges Faced
1. Syntax errors in joins and functions
2. Understanding correct usage of window functions
3. Understanding business logic for segmentation
4. Managing multiple datasets together
5. partitioning in Spark


## Outcomes
Identified:
1.Top 3 customers per city
2.Daily sales trends
3.Best performing products
4.Customer segmentation (Gold, Silver, Bronze)
