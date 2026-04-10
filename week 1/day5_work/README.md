## Objective:
The objective of this task is to gain hands-on experience in PySpark data processing and transformations by working with real datasets such as the Iris dataset and Airlines dataset. The focus was on understanding how to load data, perform transformations, handle duplicates, apply functions, and build structured DataFrames.



## Implementation:
1. Data Loading
Loaded CSV data 
2. Data Exploration
Checked schema using:
printSchema()
Displayed data using:
display()
3. Data Cleaning
Removed duplicates using:distinct(), dropDuplicates()
4. Column Operations
Renamed columns using:withColumnRenamed()
Added new columns using:
lit()
current_date()
Created derived columns like:
current date
future date using date_add()
5. Filtering & Limiting
Used:
limit() to restrict rows
Dropped unwanted columns using:
drop()
6. Union Operations
Combined datasets using:
union()
unionByName()
7. Handling Null Values
Used:
dropna("any")
dropna("all")
8. Aggregations
Grouped data using:
groupBy()


## Key Learnings:
Learned how to:
1. load and explore datasets in PySpark
2. perform column transformations
3. remove duplicates and clean data
4. Gained understanding of union operations and date functions (current_date, date_add)
5. Importance of schema inference and data cleaning before processing


## Challenges Faced:
1.Handling correct syntax for reading data and transformations
2. Understanding when to use dropDuplicates() vs distinct()
3. Structuring transformations step-by-step
4. Working with real datasets required careful schema handling and correct column 

## Outcomes:
Successfully:
cleaned datasets
transformed columns
combined datasets

