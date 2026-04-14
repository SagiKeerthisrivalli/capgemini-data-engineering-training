## Objective:
The objective of this phase was to build a strong foundation in data preprocessing using PySpark by focusing on handling null values, and working with string, numeric, and date data types. The goal was to understand how real-world datasets contain inconsistencies and how to clean, transform, and standardize data effectively before further processing or analysis.

## Steps / Methodology:
Loaded datasets into PySpark DataFrames
Identified null values using functions like isNull()
Handled nulls using:
fillna()
drop()
Performed string operations:
upper(), lower(), trim()
Applied numeric transformations:
Casting (cast())
Arithmetic operations
Converted string to date using to_date()
Formatted dates using date_format()
Extracted date components (year, month, day)
Validated and displayed transformed data

 
## Key Learnings:
Learned techniques to handle missing/null values
Understood importance of data cleaning before processing
Gained knowledge of string transformations in PySpark
Learned how to cast and manipulate numeric data
Understood date conversion and formatting
Improved understanding of data type consistency

## Challenges Faced:
Handling null values across different data types required careful selection of appropriate methods, as incorrect handling could lead to data loss or incorrect results. Another challenge was dealing with inconsistent data formats, especially in date and numeric fields, where improper casting or formatting caused errors during transformations.

1. Data type mismatches (string vs int vs date)
2. Errors during casting operations
3. Handling null values without losing important data
4. Formatting inconsistent date values
5. Understanding correct functions for each data type

## Outcomes:
This phase resulted in a clear understanding of how to clean and preprocess raw data effectively using PySpark. The processed datasets became structured, consistent, and ready for further transformations and analysis, forming a strong base for building reliable data pipelines.

1. Cleaned and standardized datasets
2. Successfully handled null values
3. Applied string, numeric, and date transformations
4. Improved data quality and consistency
5. Prepared data for downstream processing
