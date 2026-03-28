1.
customers.show()

2.
df=customers.filter(customers.city=="Chennai")
df.show()

3.
df=customers.filter(customers.age>25)
df.show()

4.
df=customers.select("customer_name","city")
df.show()

5.
df=customers.groupBy("city").count()
df.show()

