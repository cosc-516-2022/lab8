# COSC 516 - Cloud Databases<br/>Lab 8 - Amazon Redshift

## Setup

Create a Amazon AWS free tier account at: [https://aws.amazon.com/free](https://aws.amazon.com/free).

The free tier account allows for free trials forever and 12-months free offers from your sign up period. You will need an email address to use. The sign-up also asks for a credit card. If you do not have a credit card, then a pre-paid credit card with a small amount should work.

## AWS Console

Login to AWS. In the AWS console, click on `Services` then select `Amazon Redshift`.


![AWS RDS Dashboard](img/Screenshot%20(12).png)

## Amazon Redshift

Click on `Create cluster`. 

<img src="img/Screenshot%20(13).png" alt="Create Cluster">

For cluster identifier use a unique identifier and select free trial. 

<img src="img/Screenshot%20(14).png" alt="Database Configuration" >

Enter a login ID and password for admin user of your database. Finally click on `Create cluster`.

<img src="img/Screenshot%20(15).png" alt="Database Configuration" >



## Configuring VPC and Network Access

In addition to making the database public, you must also configure the database VPC to allow inbound traffic from your machine. 

Similar to the Amazaon RDS lab, once the database is created, click on the database identifier to get to an overview screen. Click on the VPC security group (in the figure it is `default (sg-00bb5776c03caa8c6)`). Then select `Inbound rules`. 

Click on `Edit the inbound rules`. In the next screen, `Add rule` that allows all traffic from your IP address. Only your machine will have access to the database. You can add other rules as required.


### Run Queries Using SQuirreL SQL or SQL Workbench/J

You can test your queries in SQuirreL SQL or SQL Workbench/J

Before working with SQuirreL, you have to configure it with the JDBC driver given in the lib folder and other cluster information.
<img src="img/Screenshot%20(20).png" alt="Database Configuration" >
<img src="img/Screenshot%20(21).png" alt="Database Configuration" >

You can also use the Redshift Query Editor V2 to test your queries directly on your database without installing any other GUI softwares.


## Tasks

To test your database, write Java code using [Visual Studio Code](https://code.visualstudio.com/). The file to edit is `AmazonRedshift.java`.  The test file is `TestAmazonRedshift.java`.  Fill in the methods requested (search for **TODO**).  Marks for each method are below.  You receive the marks if you pass the JUnit tests AND have followed the requirements asked in the question (including documentation and proper formatting).

- +1 mark - Write the method `connect()` to make a connection to the database. 
- +1 mark - Method `close()` to close the connection to the database.
- +1 mark - Method `drop()` to drop all the tables from the database. Note: The database schema name will be `dev`. 
- +3 marks - Method `create()` to create the database `dev` and the tables. 

- +3 marks - Write the method  `insert()` to add the standard TPC-H data into the database. The DDL files are in the ddl folder. **Hint: Files are designed so can read entire file as a string and execute it as one statement. May need to divide up into batches for large files.**

- +4 marks - Write the method `query1()` that returns the most recent top 10 orders with the total sale and the date of the order for customers in `America`.

- +4 marks - Write the method `query2()` that returns the customer key and the total price a customer spent in descending order, for all urgent orders that are not failed for all customers who are outside Europe and belong to the largest market segment. The largest market segment is the market segment with the most customers.

- +3 marks - Write the method `query3()` that returns a count of all the lineitems that were ordered within the six years starting on April 1st, 1997 group by order priority. Make sure to sort by orderpriority in ascending order.

**Total Marks: 20**

## Bonus Marks: (up to 3)

- Up to +3 bonus marks for demonstrating some other unique feature of Amazon Redshift such as Machine Learning.

## Submission

The lab is marked immediately by the professor by showing the output of the JUnit tests and by a quick code review.  Otherwise, submit the URL of your GitHub repository on Canvas. **Make sure to commit and push your updates to GitHub.**

