## Objective:
The objective of this day is to understand and implement advanced SQL concepts, specifically window functions (row_number, rank, dense_rank) and case-based conditional logic on employee datasets.

## Implementation:
1. Table Creation & Data Setup
Created employee tables with fields:
emp_id, emp_name, department, salary, join_date,experience, performance_rating
2. Window Functions
Used:
row_number() → unique ranking
rank() → handles ties with gaps
dense_rank() → handles ties without gaps
3. Partitioning
Used partition by: to rank employees within each department
4. Case Statements (Business Logic)
It checks conditions one by one and returns a value as soon as a matching condition is found


## Key Learnings:
1. Deep understanding of: row_number(), rank(), dense_rank()
2. Learned how to: apply window functions with partitioning
3. Difference between: rank() vs dense_rank()
4.How to implement: real-world business rules in SQL

## Challenges Faced:
1. when to use row_number vs rank
2.Writing complex nested case statements
3. Handling overlapping conditions
4.Queries became lengthy due to: multiple nested conditions

## Outcomes:
Ranked employees based on:
salary
joining date
department
Identified:
top performers
risk categories
promotion eligibility
💡 Skill Outcomes
Improved ability to:
write advanced SQL queries
implement business logic
use window functions effectively
