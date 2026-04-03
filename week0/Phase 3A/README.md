Objective:
In this phase, I came to know about the real world data like data having missing values and how can we clean them before ETL pipeline.

Approach:
1. Created a PySpark DataFrame with messy data
2. Identified null values, duplicates
3. Cleaned these null values and also duplicate values
4. Filtered invalid values 
5. Validated cleaning by comparing row counts before and after
6. Performed aggregation operations on cleaned data

Key Transformations:
• dropna() for removing null key values 
• fillna() for handling missing data 
• dropDuplicates() for removing duplicate rows 
• filter() for removing invalid values 
• groupBy() with count() for aggregation

Output / Results:
1. Cleaned dataset with valid and consistent data without any null values and duplicates
2. Row count comparison before and after cleaning 
3. Customer count per city after cleaning

Challenges Faced:
1. Finding data issues in the dataset 
2. Deciding how to handle missing values properly

Files in this Folder:
• Outputs : Containing all the outputs of queries 
• pyspark_sol → pyspark code for queries
