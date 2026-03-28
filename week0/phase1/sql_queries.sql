1.
select *
from customers;

2.
select *
from customers
where city="Chennai";

3.
select *
from customers
where age>25;

4.
select customer_name,city
from customers;

5.
select city,count(*) as total_no.of_customers
from customers
group by city;
