## Objective:
The objective of this project is to analyze customer and order data using PySpark by applying data cleaning, transformation, and aggregation techniques. The goal is to handle missing and inconsistent data, identify customers without orders and invalid records, and generate meaningful business insights such as total customer spending and ranking based on purchase behavior.

## Methodology:

This project follows a structured data processing pipeline:
1. Created DataFrames for customers and orders
2. Converted date columns into proper format
3. Applied data cleaning techniques to handle NULL and negative values
4. Performed joins (inner, left, anti) to combine datasets and identify unmatched records

Additionally, transformations and aggregations were applied:

Used groupBy() and sum() for calculating total spend
Applied window functions like rank() for ranking customers
Cleaned string data using trim() and regexp_replace()


## Key Learnings:
Gained understanding of different join types in PySpark:
1. Inner join
2. Left join
3. Left anti join
4. Learned how to handle missing and inconsistent data effectively
5. Understood importance of preprocessing before analysis
6. Window functions for ranking and analytics
7. Data transformation using built-in PySpark functions


## Challenges Faced:
1. Handling NULL values after joins required careful filtering
2. Negative and inconsistent data affected calculations
3. Duplicate columns appeared after joins
4. Window function warning due to missing partition
5.Understanding difference between anti join and filtering


## Outcomes:

The project resulted in a clean and structured dataset suitable for analysis. It successfully identified customers with no orders and removed invalid data entries.
1. Computed total spending per customer
2. Ranked customers based on spending
3. Extracted meaningful insights from raw data
