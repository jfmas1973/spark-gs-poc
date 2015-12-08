# spark-gs-poc
Proof of concept with Spark

This project: 

- Reads from a csv file and store the lines as rows in a database table.
- Reads from a db table, transforms to Json and stores in another table.

The goal is to use Spark for parallelize the jobs and reach the maximum connections allowed by db connection pool: HikariCP. 

The database is an oracle-xe instance, and the schema needed for run this code is inside:

/spark-gs-poc/db/flight.SQL

The csv files to get a lot of data has been taken from:

http://stat-computing.org/dataexpo/2009/the-data.html 