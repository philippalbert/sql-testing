# SQL Testing

The goal of this project is to simplify SQL testing in a project setting
where SQL and Python is used.

## Motivation

Data driven teams often work in a multi-tool landscape
where not only Python is used but also other tools. Often
this landscape looks similar to:

**Main_Database_Architecture**
-> **Use_Case_Specific_Tables_Views_Procedures**
-> **Code_For_Pre_Processing_Modelling_And_Post_Processing**

The goal is not to test the main database architecture as
this is a general structure which is static and hopefully (in one way or another way)
deeply tested. The assumption is that this part will not change
over time as it is the base data for lots of different products.

What you want to test is of course the Python code you will write to solve your
specific problem. There are already a lot of libraries and packages which provide
a lot of functions to test your Python code.

What this package wants to address is testing the second point mentioned above.
The use-case specific database architecture is very likely to change at least in the
POC phase of a project and even after some time there can be some changes.
These changes can include tables, views and also procedures which can change over time for
multiple reasons, e.g. performance issues, readability, ...

Consider the following (maybe too simple example):

```sql
CREATE TABLE table2 AS
SELECT * FROM TABLE1 WHERE something = 1 OR something =2 OR something = 3;
```

```sql
CREATE TABLE table2 AS
SELECT /*+ parallel(8) */ * FROM TABLE1 WHERE something IN(1,2,3);
```

Obviously the output table of both queries and admittedly this example is maybe oversimplified
but in a real world setting sql-queries can become pretty long and also hard to read.
So you want to ensure that the output of a query still is the output you would expect
which is the goal here.

## Use Cases

This package divides SQL testing into two categories

1) General SQL commands
2) Dialect-specific SQL commands

In general, we don't want to use a productive database for testing. It is preferred
to use a local one like the internal database provided by Python itself: sqlite.
Sadly, in some specific cases, where we have to use dialect exclusive expressions
this is not possible.

### General SQL commands



### Dialect-specific SQL commands



## Author notes

!!!! Requirements -> installing driver!!!!
!!!! Requirements -> Providing database string!!!!


-  **Engine** is the starting point of any SQLAlchemy application
    - works with the connection pool and the Dialect component
      to deliver the SQL statements from the SQLAlchemy to the database
-  **Dialect** is the system SQLAlchemy uses to communicate with various types
    - The Dialect is created from the supplied connection string

- **Tranaction** with engine.begin() starts a transaction
    - if transaction is successful it will be committed
    - if not it will be rolled back
    - some words to nested transactions
        - https://docs.sqlalchemy.org/en/14/core/connections.html
    - The problem is that engine.begin() commits if successful and rollbacks if not
        - in case are working with a real db we want a rollback even when it was successful
          because we don't want the tables to remain in the db. Therefore we have to work
          with `connection = engine.connect()` and `transaction = connection.begin()`

- **INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE** will be autocomitted (if not included in transaction)
   - can be changed by changing **isolation level**
    
- **Reflect** can be used in ORM to obtain already existing db tables 
