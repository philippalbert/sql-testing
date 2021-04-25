# SQL Testing

The goal of this project is to simplify SQL testing.

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

