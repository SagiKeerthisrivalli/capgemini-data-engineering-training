--Daily sales
select sale_date, sum(total_amount) as total_sales
from sales
where total_amount>0
group by sale_date

--city-wise revenue
select c.city, sum(s.total_amount) as total_sales
from sales s join customers c
on s.customer_id=c.customer_id
where c.city is not null and s.total_amount>0
group by c.city

--Find repeat customers (>2 orders)
select customer_id
from sales
where total_amount>0
group by customer_id
having count(*) > 2;

--Find highest spending customer in each city
WITH customer_spend as(select s.customer_id,c.city, sum(s.total_amount) as total_spend
    from sales s join customers c
    on s.customer_id = c.customer_id
    where s.total_amount > 0
    group by s.customer_id, c.city
)
select customer_id, city, total_spend
from (select *,
      ROW_NUMBER() OVER (PARTITION by city order by total_spend DESC) as rank
    from customer_spend
) t
where rank = 1
order by city;

--Build final reporting table with customer, city, total spend, order count
select s.customer_id, c.city, sum(s.total_amount) AS total_spend,
       count(*) as order_count
from sales s join customers c
on s.customer_id = c.customer_id
where s.total_amount > 0 and c.city IS NOT NULL
group by s.customer_id, c.city
order by s.customer_id;

