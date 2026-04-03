-- 1. Total Order Amount for Each Customer

SELECT customer_id, SUM(amount) AS total_amount
FROM orders
GROUP BY customer_id;

-- 2. Top 3 Customers by Total Spend

SELECT customer_id, SUM(amount) AS total_spend
FROM orders
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 3;

-- 3. Customers with No Orders

SELECT c.*
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id
WHERE o.order_id IS NULL;

-- 4. City-wise Total Revenue

SELECT c.city, SUM(o.amount) AS total_revenue
FROM customers c
JOIN orders o
ON c.customer_id = o.customer_id
GROUP BY c.city;

-- 5. Average Order Amount per Customer

SELECT customer_id, AVG(amount) AS avg_amount
FROM orders
GROUP BY customer_id;

-- 6. Customers with More Than One Order

select o1.customer_id
from Orders o1 join Orders o2
where o1.customer_id=o2.customer_id and o1.order_id <> o2.order_id

-- 7. Sort Customers by Total Spend Descending

SELECT customer_id, SUM(amount) AS total_spend
FROM orders
GROUP BY customer_id
ORDER BY total_spend DESC;
