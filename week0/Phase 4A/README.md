Objective:
In this phase, I learnt about how continuous date is converted into categorical data. And grouping of data and various methods like conditional logic, bucketizer and quantile based ranking and many more.

Datasets used:
In this I used sales.csv datset, which Contains total_amount representing customer spending.

Key Concepts Covered:
1. Data segmentation (bucketing)
2. Data-driven segmentation
3. Window functions for ranking
4. Aggregation and grouping

Methods Implemented:
1. Conditional Segmentation (Business Logic):
        Rule-based categorization using conditions
2. Bucketizer (MLlib):
        Uses predefined numeric ranges (splits) to divide data into buckets
        Outputs numerical bucket labels (e.g., 0, 1, 2)
        Commonly used in machine learning pipelines
3. Window-based Ranking:
        Uses percent_rank() function
        Assigns values between 0 and 1 based on relative position
4. Quantile-based Segmentation:
         Uses values 0.33 and 0.66 to measure the quantile


Measures used:
Gold : total_spend > 10000
Silver : 5000 ≤ total_spend ≤ 10000
Bronze : total_spend < 5000


5. Comparison of Methods
This project compares multiple segmentation techniques:
Conditional (rule-based)
Quantile (data-driven)
Bucketizer (ML approach)
Ranking (relative positioning)

Output Insights:
1. Window ranking (percent_rank) shows relative position of each customer
2. High-value customers (Gold) often contribute major portion of revenue
3. Results vary depending on data distribution and method used

Key Learnings
1. Conditional logic is ideal for implementing business rules
2. No single method is best → choice depends on use case
3. Combining methods gives better and more reliable insights
4. Understanding data distribution is crucial before segmentation
Proper segmentation helps in targeted marketing and decision making
