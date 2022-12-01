### Data Modeling with Apache Cassandra
The ipynb file shows the usage example of Apache Cassandra. The file creates different tables for 3 different use cases/queries.
Data it is working on is event_datafile_new.csv.

#### Steps:
1. Create Cassandra cluster
2. Create a Keyspace
3. Write a query that solves the use case
4. Create a table that serves the query with the best performance. Consider the best partition keys and clustering keys (if needed). 
5. Insert data into the new table from event_datafile_new.csv
6, Check result of the query
7. Drop table
8. Close the session 
9. Close cluster connection
