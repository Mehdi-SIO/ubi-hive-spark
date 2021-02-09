# ubi-hive-spark


## Questions

Part 1 – Create the following table using HIVE
- Generate a column which gives the number of distinct IP address LTD (live to date)
per customerId.
- Generate a column which gives the amount spent UTD (up to date) per customerId.
- Assuming that you write this table to HDFS, how much disk space would it occupy?
- In the Hadoop ecosystem, which mechanism(s) ensure(s) fault tolerance as far as
Data Storage is concerned?

Part 2 – Create the following table using SPARK
- Generate a column which gives the number of distinct characters for an address (e.g
hello has 4 distinct characters).
- Generate a column which flags “True” if the address is located in “rue de Paris”.
- What is the abstraction type in Spark that doesn’t benefit from the engine’s
optimization capabilities?

Part 3 – Create the following table using either SPARK or HIVE
- Generate a column which gives the total amount spent per customer in the last 10
minutes (without including the current transaction).
- Furthermore, imagine that the schemaTransaction table contains 100 million
transactions for a 10k distinct users over a year of data. How would you write this
table to disk in order to optimize subsequent queries on the data?

Part 4 – Optimization
- In the join function, what is the broadcasting operation used for?
- You will find a script below which creates some features. How would you optimize it?
Please describe a few potential solutions.