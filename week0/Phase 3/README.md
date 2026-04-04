Objective:
In this phase, I focused on building data pipeline using PySpark by applying ETL (Extract, Transform, Load) concepts. It also includes concepts like data cleaning, transformation, and generating final analytical outputs.


Problem Statement (Summary):
This mainly focuses on
1. Reading data from multiple file formats (CSV, JSON, Parquet)
2. Checking schema
3. Performing data cleaning (removing nulls, duplicates values)
4. Applying transformations (aggregation, joins)
5. Building a complete ETL pipeline
6. Obtaining required results


Approach:
1. Loading the datasets into PySpark DataFrames.
2. Cleaned data by handling null values and duplicate values
3. Converted invalid datatypes into required data types
4. Joined datasets to combine customer and transaction data
5. Applied aggregations functions
6. Obtained the outputs


Key Transformations:
• dropna() and fillna() for handling missing data 
• filter() for removing invalid records 
• join() for combining datasets 
• groupBy() with aggregations 

Output / Results:
• Daily sales aggregation 
• City-wise revenue analysis 
• Finding of repeat customers 
• Highest spending customer per city 
• Final reporting table with customer insights

Challenges Faced: 
• Handling missing and inconsistent data 
• File path issues while accessing datasets 
• Learning about window functions

Learnings:
• Importance of data cleaning before analysis the data
• Combining multiple tables to get required results

Files in this Folder:
• queries.sql → sql queries
• solution.py / notebook → Implementation 
• outputs/ → Results (if any)
