## Objective:
The objective of this phase is to focus on understanding how to apply data transformations, joins, aggregations, and conditional logic to solve real-world problems such as revenue analysis and customer segmentation.


## Implementation:
1. Data Processing & Transformation
Loaded datasets using PySpark
Performed data type conversions using cast()
Applied transformations using withColumn()
2. Daily Sales Analysis
Calculated total revenue per day
3. City-wise Revenue
Joined customer and sales datasets
Computed total revenue for each city
4. Top 5 Customers
Created full customer name using concat_ws()
5. Customer Segmentation
Categorized customers into:
Gold → spend > 10000
Silver → 5000 to 10000
Bronze → < 5000
Implemented using when() conditional logic


## Key Learnings:
Strong understanding of:
1.groupBy() and aggregations
2.joins in PySpark
3.column transformations using withColumn()
4.Importance of combining multiple datasets for analysis
5.Understanding how business logic is applied on data


## Challenges Faced:
1. Handling data type conversions for numeric fields
2. Writing multiple joins correctly
3. Designing correct segmentation logic
4. Structuring workflow in proper sequence

## Outcomes:
Generated insights such as:
daily revenue trends
city-wise revenue
top customers
repeat customers


👉 Overall, this task significantly improved practical understanding and confidence in working with PySpark for real-time data analysis.
