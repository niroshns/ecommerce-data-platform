# E-commerce Data Platform 
[IBM Data Engineering Capstone Project](https://www.coursera.org/learn/data-enginering-capstone-project)

Tool(s): Python, MySQL(SQL), MongoDB(NoSQL), Linux Shell Scripting, Hadoop, Airflow, Spark, PostgreSQL, DB2 on Cloud, IBM Cognos Analytics

## Overview 
I assume the role of a Data Engineer who has recently joined a fictional online e-Commerce company named SoftCart. I was presented with a business challenge that requires building a data platform for retailer data analytics. 

### Objectives
- Demonstrate proficiency in skills required for an entry-level data engineering role
- Design and implement various concepts and components in the data engineering lifecycle such as data repositories
- Showcase working knowledge with relational databases, NoSQL data stores, big data engines, data warehouses, and data pipelines
- Apply skills in Linux shell scripting, SQL, and Python programming languages to Data Engineering problems

## Scenario 
SoftCart's online presence is primarily through its website, which customers access using a variety of devices like laptops, mobiles and tablets. All the catalogue data of the products is stored in the MongoDB NoSQL server. All the transactional data like inventory and sales are stored in the MySQL database server. SoftCart's webserver is driven entirely by these two databases. Data is periodically extracted from these two databases and put into the staging data warehouse running on PostgreSQL. Production data warehouse is on the cloud instance of IBM DB2 server. BI teams connect to the IBM DB2 for operational dashboard creation. IBM Cognos Analytics is used to create dashboards. SoftCart uses Hadoop cluster as its big data platform where all the data collected for analytics purposes. Spark is used to analyse the data on the Hadoop cluster. To move data between OLTP, NoSQL and the data warehouse, ETL pipelines are used and these run on Apache Airflow.

![Data Platform Architecture](/Images/0-data-platform-architecture.png)


## Table of Contents
1. [MySQL Online Transactional Processing Database](#MySQL-Online-Transactional-Processing-Database)
2. [MongoDB NoSQL Catalog Database](#MongoDB-NoSQL-Catalog-Database)
3. [PostgreSQL Staging Data Warehouse](#PostgreSQL-Staging-Data-Warehouse)
4. [IBM Db2 Production Data Warehouse](#IBM-Db2-Production-Data-Warehouse)
5. [Python Scripts & Automation](#Python-Scripts-And-Automation)
6. [Apache Airflow ETL & Data Pipelines](#Apache-Airflow-ETL-And-Data-Pipelines)
7. [Apache Spark Big Data Analytics](#Apache-Spark-Big-Data-Analytics)

   
## MySQL Online Transactional Processing Database
SoftCart will be using MySQL for our online transactional processing (OLTP), such as storing inventory and sales data. Based on the sample data given, I designed a database schema and created a database to store their sales data. I then created an index on the timestamp column and wrote an administrative bash script that exported sales data into a SQL file. 

Tool(s): MySQL (Cloud IDE on Theia in Docker), phpMyAdmin


Based on the sample data given, the schema contained the following columns: `product_id`, `customer_id`, `price`, `quantity`, `timestamp`
![Sample OLTP Sales Data - MySQL](/Images/1.1-sample-oltp-data.png)

I created a database named `sales` with a table named `sales_data` to store their sales data. I made sure none of the columns allowed `NULL`. Also, I used data type `INT` for the `price` column instead of `FLOAT`, based on the sample data given.

```sql
CREATE DATABASE sales;

CREATE TABLE `sales_data` (
	`product_id` INT NOT NULL,
	`customer_id` INT NOT NULL,
	`price` INT NOT NULL,
	`quantity` INT NOT NULL,
	`timestamp` TIMESTAMP NOT NULL ON UPDATE CURRENT_TIMESTAMP
);
```

After downloading the sample data `oltpdata.csv`, I imported it into `sales_data` using phpMyAdmin.

![Import Data](/Images/1.2-importdata.png)

Using MySQL CLI I listed the tables in the sales database to verify the new sales table was created. 
```sql
USE sales;
SHOW tables;
```
>```sql
> +-----------------+
> | Tables_in_sales |
> +-----------------+
> | sales_data      |
> +-----------------+
> 1 row in set (0.01 sec)
> ```

Next, I wrote a simple test query to display the record count from `sales_data`.
```sql
SELECT COUNT(*)
FROM sales_data;
```
> ```
> +----------+
> | COUNT(*) |
> +----------+
> |     2605 |
> +----------+
> 1 row in set (0.00 sec)
> ```

To set up administration tasks, I first created an index named `ts` on the timestamp column.
```sql
CREATE INDEX ts ON sales_data (timestamp);
```
> ```
> Query OK, 0 rows affected (0.09 sec)
> Records: 0  Duplicates: 0  Warnings: 0
> ```

Using the following command, I will list all indexes from `sales_data` to confirm the creation of the `ts` index.
```sql
SHOW INDEX FROM sales_data;
```
> ```
> +------------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+---------+------------+
> | Table      | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment | Visible | Expression |
> +------------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+---------+------------+
> | sales_data |          1 | ts       |            1 | timestamp   | A         |        2605 |     NULL |   NULL |      | BTREE      |         |               | YES     | NULL       |
> +------------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+---------+------------+
> 1 row in set (0.00 sec)
> ```

After seeing that the timestamp index was successfully created, I wrote a bash script `datadump.sh` that exports all records from `sales_data` into a SQL file named `sales_data.sql`.

```console
sudo touch datadump.sh

sudo nano datadump.sh
```

In the nano editor, I wrote the following bash script using `mysqldump` to export the records. 
```console
#!/bin/bash
mysqldump -u root -p sales sales_data > sales_data.sql
```

After saving the file and exiting the editor, I updated the permissions so the script can be executed.
```console
sudo chmod u+x datadump.sh
```

The `sales_data` export process can can now be automated or executed manually with the following command:
```console
sudo ./datadump.sh
```

## MongoDB NoSQL Catalog Database
All of SoftCart's catalog data is to be stored on a MongoDB NoSQL server. I createed the database `catalog` and import their electronics products from `catalog.json` into a collection named `electronics`. I ran test queries against the data and exported the collection into a file named `electronics.csv` using only the `_id`, `type`, and `model` fields.

Tool(s): MongoDB Server, MongoDB Command Line Backup Tools

I will need `mongoimport` and `mongoexport` commands from the **MongoDB CLI Database Tools** library, so I checked to see if the library is installed.
```console
mongoimport
```
> ```
> bash: mongoimport: command not found
> ```

Since the command is not recognized, I needed to manually install the library. First. I deteremined the Linux Platform and Architecture using `lsb_release -a` and the processor type using `uname -p`. 
```console
lsb_release -a
```
> ```
> No LSB modules are available.
> Distributor ID: Ubuntu
> Description:    Ubuntu 18.04.6 LTS
> Release:        18.04
> Codename:       bionic
> ```
```console
uname -p
```
> ```
> x86_64
> ```

After, I selected the appropriate package for MongoDB Database Tools from the following link: https://www.mongodb.com/try/download/database-tools. The package I installed was `Ubuntu 18.04 x86_64` with the `.deb` extension.

```console
sudo wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu1804-x86_64-100.6.1.deb

sudo apt install ./mongodb-database-tools-ubuntu1804-x86_64-100.6.1.deb
```

I verified the installation by running the `mongoimport` command again. The command was recognized, meaning the package has successfully been installed.

```console
mongoimport
```
> ```
> 2023-02-05T15:11:01.000-0500    no collection specified
> 2023-02-05T15:11:01.000-0500    using filename '' as collection
> 2023-02-05T15:11:01.000-0500    error validating settings: invalid collection name: collection name cannot be an empty string
> ```

Using MongoDB CLI, I created a database `catalog` and a collection named `electronics`

```console
sudo service mongodb start
use catalog
db.createCollection("electronics")
```

I then downloaded the data from `catalog.json` and loaded it into the collection.
```console
sudo wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/nosql/catalog.json
mongoimport --host localhost --port 27017 --db catalog --collection electronics --authenticationDatabase admin --username root --password PASSWORD catalog.json
```
> ```
> 2025-04-05T19:11:01.726-0500    connected to: mongodb://localhost/
> 2025-04-05T19:11:01.751-0500    438 document(s) imported successfully. 0 document(s) failed to import.
> ```

I listed out all the databases and collections on mongodb server to check if catalog database and electronics collection were created successfully.
```console
db.adminCommand( { listDatabases: 1 } )
```
> ```
> {
>         "databases" : [
>                 {
>                         "name" : "admin",
>                         "sizeOnDisk" : 32768,
>                         "empty" : false
>                 },
>                 {
>                         "name" : "catalog",
>                         "sizeOnDisk" : 40960,
>                         "empty" : false
>                 },
>                 {
>                         "name" : "config",
>                         "sizeOnDisk" : 69632,
>                         "empty" : false
>                 },
>                 {
>                         "name" : "local",
>                         "sizeOnDisk" : 32768,
>                         "empty" : false
>                 }
>         ],
>         "totalSize" : 176128,
>         "ok" : 1
> }
> ```

```console
use catalog
db.runCommand( { listCollections: 1 } )
```
> ```
> {
>         "cursor" : {
>                 "id" : NumberLong(0),
>                 "ns" : "catalog.$cmd.listCollections",
>                 "firstBatch" : [
>                         {
>                                 "name" : "electronics",
>                                 "type" : "collection",
>                                 "options" : {
> 
>                                 },
>                                 "info" : {
>                                         "readOnly" : false,
>                                         "uuid" : UUID("ed60269b-0bd7-41f1-87e2-d7c405b446fe")
>                                 },
>                                 "idIndex" : {
>                                         "v" : 2,
>                                         "key" : {
>                                                 "_id" : 1
>                                         },
>                                         "name" : "_id_",
>                                         "ns" : "catalog.electronics"
>                                 }
>                         }
>                 ]
>         },
>         "ok" : 1
> }
> ```

I created an index on the field `type` to improve query time. 
```console
db.electronics.createIndex({"type" : 1})
```
> ```
> {
>         "createdCollectionAutomatically" : false,
>         "numIndexesBefore" : 1,
>         "numIndexesAfter" : 2,
>         "ok" : 1
> }
> ```

Now that the collection is indexed, I ran a few more test queries. I started by displaying the record count for product type `laptops`. 

```console
db.electronics.find( {"type":"laptop"} ).count()
```
> ```
> 389
> ```

Then, I displayed the record count for product type `smart phones` with screen size of `6 inches`.
```console
db.electronics.find( {"type":"smart phone", "screen size":6} ).count()
```
> ```
> 8
> ```

Finally, I wrote an aggregration query to display the average `screen size` of product type `smart phones`.
```console
db.electronics.aggregate([{$match: {"type": "smart phone"}},{$group: {_id:"$type", avg_val:{$avg:"$screen size"}}}])
```
> ```
> { "_id" : "smart phone", "avg_val" : 6 }
> ```

I exported the `electronics` collection into a file named `electronics.csv` using only the `_id`, `type`, and `model` columns.
```console
mongoexport --host localhost --port 27017 --authenticationDatabase admin --username root --password PASSWORD --db catalog --collection electronics --fields _id,type,model --out electronics.csv
```
> ```
> 2023-02-05T18:22:54.806-0500    connected to: mongodb://localhost/
> 2023-02-05T18:22:54.819-0500    exported 438 records
> ```


## PostgreSQL Staging Data Warehouse
Sales data from MySQL and catalog data from MongoDB will be periodically extracted and stored into a staging data warehouse running on PostgreSQL. The data will then be transformed and loaded into a production data warehouse running on IBM Db2 to generate reports such as:
- total sales per year per country
- total sales per month per category
- total sales per quarter per country
- total sales per category per country

I designed a data warehouse star schema using the pgAdmin ERD design tool, ensuring the table can generate yearly, monthly, daily, and weekly reports. I exported the schema SQL and created a staging database.

Tool(s): ERD Design Tool of pgAdmin, PostgreSQL Database Server

![Sample Order Data](/Images/3.1-sample_order_data.png)

Based on the sample order data and report requirements, the fact of measure will be constructed in the following table:
- `softcartFactSales`
   - <b>orderid</b> <sup><sub>PRIMARY KEY</sub></sup>
   - <b>dateid</b> <sup><sub>FOREIGN KEY</sub></sup>
   - <b>categoryid</b> <sup><sub>FOREIGN KEY</sub></sup>
   - <b>countryid</b> <sup><sub>FOREIGN KEY</sub></sup>
   - <b>itemid</b> <sup><sub>FOREIGN KEY</sub></sup>
   - <b>amount</b>

and will contain the following dimensions:
- `softcartDimDate`
   - <b>dateid</b> <sup><sub>PRIMARY KEY</sub></sup>
   - <b>date</b>
   - <b>Year</b>
   - <b>Quarter</b>
   - <b>QuarterName</b>
   - <b>Month</b>
   - <b>MonthName</b>
   - <b>Day</b>
   - <b>Weekday</b>
   - <b>WeekdayName</b>
- `softcartDimCategory`
   - <b>categoryid</b> <sup><sub>PRIMARY KEY</sub></sup>
   - <b>category</b>
- `softcartDimCountry`
   - <b>countryid</b> <sup><sub>PRIMARY KEY</sub></sup>
   - <b>country</b>
- `softcartDimItem`
   - <b>itemid</b> <sup><sub>PRIMARY KEY</sub></sup>
   - <b>item</b>

I started by creating a temporary database `softcart` to design the star schema using pgAdmin's ERD Design Tool. I later created tables for each dimension and designed the relationships between them.

![ERD Schema](/Images/3.2-erd_schema.png)

With the star schema designed, I exported it as a SQL file and created our `staging` database.
```sql
-- This script was generated by a beta version of the ERD tool in pgAdmin 4.
-- Please log an issue at https://redmine.postgresql.org/projects/pgadmin4/issues/new if you find any bugs, including reproduction steps.
BEGIN;


CREATE TABLE public."DimCategory"
(
    categoryid smallint NOT NULL,
    category character varying(32)[] NOT NULL,
    PRIMARY KEY (categoryid)
);

CREATE TABLE public."DimCountry"
(
    countryid smallint NOT NULL,
    country character varying(32)[] NOT NULL,
    PRIMARY KEY (countryid)
);

CREATE TABLE public."DimDate"
(
    dateid smallint NOT NULL,
    date date NOT NULL,
    "Year" smallint NOT NULL,
    "Quarter" smallint NOT NULL,
    "QuarterName" character varying(2)[] NOT NULL,
    "Month" smallint NOT NULL,
    "Monthname" character varying(9)[] NOT NULL,
    "Day" smallint NOT NULL,
    "Weekday" smallint NOT NULL,
    "WeekdayName" character varying(9)[] NOT NULL,
    PRIMARY KEY (dateid)
);

CREATE TABLE public."FactSales"
(
    orderid bigint NOT NULL,
    dateid smallint NOT NULL,
    countryid smallint NOT NULL,
    categoryid smallint NOT NULL,
    amount bigint NOT NULL,
    PRIMARY KEY (orderid)
);

ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (dateid)
    REFERENCES public."DimDate" (dateid)
    NOT VALID;


ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (countryid)
    REFERENCES public."DimCountry" (countryid)
    NOT VALID;


ALTER TABLE public."FactSales"
    ADD FOREIGN KEY (categoryid)
    REFERENCES public."DimCategory" (categoryid)
    NOT VALID;

END;
```

```shell
COMMIT

Query returned successfully in 181 msec.
```

## IBM Db2 Production Data Warehouse
With the adjusted schema design, I created an instance of IBM DB2 and loaded the sample datasets into their respective tables. I wrote aggregation queries and created a Materialized Query Table for future reports.

Tool(s): Cloud instance of IBM DB2 database

To prepare an instance of IBM DB2, I first created the following tables and loaded their respective datasets.
- `DimDate`
- `DimCategory`
- `DimCountry`
- `DimItem`
- `FactSales`

Then, I created and ran queries to check if the created data warehouse can create reports. The first aggregation query will be a `GROUPING SETS` query using the columns `country`, `category`, and `totalsales`. For full output: [`grouping_sets_data.csv`](/Data/grouping_sets_data.csv)
```sql
SELECT category, country, sum(amount) as totalsales
FROM factsales as sales
INNER JOIN dimcategory as cat ON cat.categoryid = sales.categoryid
INNER JOIN dimcountry as country ON country.countryid = sales.countryid
GROUP BY GROUPING SETS (
	(cat.category, country.country),
	(cat.category),
	(country.country),
	()
)
ORDER BY country.country
```

| CATEGORY    | COUNTRY              | TOTALSALES |
|-------------|----------------------|------------|
|             | Argentina            | 21755581   |
| Books       | Argentina            | 4285010    |
| Electronics | Argentina            | 4338757    |
| Software    | Argentina            | 4292153    |
| Sports      | Argentina            | 4450354    |
| Toys        | Argentina            | 4389307    |
|             | Australia            | 21522004   |
| Books       | Australia            | 4340188    |
| Electronics | Australia            | 4194740    |
| Software    | Australia            | 4410360    |

Next I wrote a `ROLLUP` query using the columns `year`, `country`, and `totalsales`. For full output: [`rollup_data.csv`](/Data/rollup_data.csv)
```sql
SELECT
	year,
	country,
	sum(amount) as totalsales

FROM
	factsales as sales
	
INNER JOIN dimdate
	ON dimdate.dateid = sales.dateid

INNER JOIN dimcountry as country
	ON country.countryid = sales.countryid

GROUP BY
	ROLLUP (
		dimdate.year,
		country.country
	)

ORDER BY
	dimdate.year
```
 
| YEAR | COUNTRY              | TOTALSALES |
|------|----------------------|------------|
| 2019 |                      | 399729036  |
| 2019 | Argentina            | 7163167    |
| 2019 | Australia            | 7259016    |
| 2019 | Austria              | 7320233    |
| 2019 | Azerbaijan           | 7097729    |
| 2019 | Belgium              | 7093441    |
| 2019 | Brazil               | 7116253    |
| 2019 | Bulgaria             | 7312912    |
| 2019 | Canada               | 7158271    |
| 2019 | Cyprus               | 7250287    |

Finally, I wrote a `CUBE` query using the columns `year`, `country`, and `averagesales`. Full output: [`cube_data.csv`](/Data/cube_data.csv)

```sql
SELECT
	year,
	country,
	avg(amount) as averagesales

FROM
	factsales as sales
	
INNER JOIN dimdate
	ON dimdate.dateid = sales.dateid

INNER JOIN dimcountry as country
	ON country.countryid = sales.countryid

GROUP BY
	CUBE (
		dimdate.year,
		country.country
	)

ORDER BY
	dimdate.year,
	country.country
```
| YEAR | COUNTRY              | AVERAGESALES |
|------|----------------------|--------------|
| 2019 | Argentina            | 4017         |
| 2019 | Australia            | 4066         |
| 2019 | Austria              | 4078         |
| 2019 | Azerbaijan           | 3967         |
| 2019 | Belgium              | 3978         |
| 2019 | Brazil               | 4025         |
| 2019 | Bulgaria             | 4087         |
| 2019 | Canada               | 4005         |
| 2019 | Cyprus               | 4045         |
| 2019 | Czech Republic       | 3979         |


I created a Materialized Query Table (MQT) named `total_sales_per_country` based on the columns `country` and `totalsales`. MQTs improve the performance of complex queries that operate on very large amounts of data. Db2 uses a MQT to precompute the results of data that is derived from one or more tables. When you submit a query, Db2 can use the results that are stored in a MQT rather than compute the results from the underlying source tables on which the materialized query table is defined.
```sql
CREATE TABLE total_sales_per_country (total_sales, country) AS (
    SELECT sum(amount), country
    FROM FactSales
    LEFT JOIN DimCountry
    ON FactSales.countryid = DimCountry.countryid
    GROUP BY (), country
)
DATA INITIALLY DEFERRED
REFRESH DEFERRED
MAINTAINED BY SYSTEM;
```
Because of the configurations `DATA INITIALLY DEFERRED` and `REFRESH DEFERRED`, the MQT did not contain any data initially. To populate it, I ran the REFRESH statement. After, I displayed the contents of the table to confirm the population of the data.

```sql
REFRESH TABLE total_sales_per_country;
SELECT * FROM total_sales_per_country;
```

| TOTAL_SALES | COUNTRY              |
|-------------|----------------------|
| 21755581    | Argentina            |
| 21522004    | Australia            |
| 21365726    | Austria              |
| 21325766    | Azerbaijan           |
| 21498249    | Belgium              |
| 21350771    | Brazil               |
| 21410716    | Bulgaria             |
| 21575438    | Canada               |
| 21500526    | Cyprus               |
| 21334142    | Czech Republic       |
| 21331097    | Denmark              |
| 21379967    | Egypt                |
| 21493054    | Estonia              |
| 21336188    | Finland              |
| 21341055    | France               |
| 21301931    | Germany              |
| 21412393    | Greece               |
| 21338015    | Hungary              |
| 21330883    | India                |
| 21498960    | Indonesia            |
| 21095982    | Ireland              |
| 21581548    | Israel               |
| 21844913    | Italy                |
| 21765341    | Japan                |
| 21429604    | Jordan               |
| 21452597    | Malaysia             |
| 21455404    | Mexico               |
| 21383019    | Netherlands          |
| 21329461    | New Zealand          |
| 21544439    | Norway               |
| 21433121    | Oman                 |
| 21704462    | Peru                 |
| 21651762    | Philippines          |
| 21845131    | Poland               |
| 21554611    | Portugal             |
| 21473762    | Qatar                |
| 21738876    | Russia               |
| 21326437    | Saudi Arabia         |
| 21534966    | Singapore            |
| 21394755    | South Africa         |
| 21598368    | South Korea          |
| 21465342    | Spain                |
| 21415934    | Sudan                |
| 21000525    | Sweden               |
| 21526097    | Switzerland          |
| 21060208    | Taiwan               |
| 21517513    | Tajikistan           |
| 21848776    | Thailand             |
| 21270737    | Turkey               |
| 21474740    | Ukraine              |
| 21444221    | United Arab Emirates |
| 21415113    | United Kingdom       |
| 21007997    | United States        |
| 21321382    | Uruguay              |
| 21416080    | Uzbekistan           |
| 21280572    | Vietnam              |


After setting up the data warehouse, I moved on to design a reporting dashboard that reflected the key metrics of the business. I first loaded the data from “ecommerce.csv” into the data warehouse (IBM DB2) and imported it into a table named sales_history.
![Data Import](/Images/4.1-dataimport.PND)

I listed the first 10 rows in the sales_history table to check the loaded data.
![First 10 Rows](/Images/4.2-first10rows.png)

I then created the data source in IBM Cognos. I created a connection between sales_history table in IBM DB2 and Watson Studio with added Cognos Dashboard Embedded (CDE) service to use it as a data source.
![Data Source](/Images/4.3-datasource.PNG)

I created a line chart of month wise total sales for the year 2020.
![Line Chart](/Images/4.4-linechart.PNG)

I created a bar chart of Quarterly sales of mobile phones.
![Bar Chart](/Images/4.5-barchart.PNG)

I created a pie chart of category wise total sales. 
![Bar Chart](/Images/4.6-piechart.PNG)


## Python Scripts And Automation
SoftCart needs to keep data synchronized between different databases/data warehouses as a part of our daily routine. One task that is routinely performed is the sync up of staging data warehouse and production data warehouse. I wrote a script that automated the process of regularly updating the DB2 instance with new records from MySQL.

Tool(s): MySQL Server, IBM DB2 Database on IBM Cloud

First I installed the `mysql-connector-python` dependency using pip.
```console
python3 -m pip install mysql-connector-python==8.0.31
```
> ```
> Successfully installed mysql-connector-python-8.0.31 protobuf-3.19.6
> ```

I connected to the MySQL database `sales` using Python. In a new file `mysqlconnect.py`, I used `mysql.connector` to connect to the database `sales`. Then, I created a table for `products` and inserted the product data. I outputed the table records for confirmation.
```python
import mysql.connector
username = 'root'
pw = 'PW'
host = '127.0.0.1'
db = 'sales'

# connect to database
connection = mysql.connector.connect(user=username, password=pw,host=host,database=db)
cursor = connection.cursor()

# create table
SQL = """CREATE TABLE IF NOT EXISTS products(
rowid int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
product varchar(255) NOT NULL,
category varchar(255) NOT NULL
)"""
cursor.execute(SQL)
print("Table created")

# insert data
SQL = """INSERT INTO products(product,category)
	 VALUES
	 ("Television","Electronics"),
	 ("Laptop","Electronics"),
	 ("Mobile","Electronics")
	 """
cursor.execute(SQL)
connection.commit()

# query data
SQL = "SELECT * FROM products"
cursor.execute(SQL)
for row in cursor.fetchall():
	print(row)

# close connection
connection.close()
```

I ran the `mysqlconnect.py` script using the command line.
```console
python3 mysqlconnect.py
```

> ```
> Table created
> (1, 'Television', 'Electronics')
> (2, 'Laptop', 'Electronics')
> (3, 'Mobile', 'Electronics')
> ```


In order to connect to IBM DB2 with Python, I installed the `ibm-db` dependency using pip.
```console
python3 -m pip install ibm-db
```
> ```
> Successfully installed ibm-db-3.1.4
> ```

I created a new file `db2connect.py`, in which I used `ibm_db` to connect to the IBM DB2 instance. In this script, I also createed a table for `products` and inserted the product data. I outputed the table records for confirmation

```python
import ibm_db

# connectction details
dsn_hostname = "6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud" 
dsn_uid = "prx70334"        
dsn_pwd = "PWD"     
dsn_port = "30376"                
dsn_database = "bludb"            
dsn_driver = "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            
dsn_security = "SSL"            

#Create the dsn connection string
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

# create connection
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# create table
SQL = """CREATE TABLE IF NOT EXISTS products(rowid INTEGER PRIMARY KEY NOT NULL,product varchar(255) NOT NULL,category varchar(255) NOT NULL)"""
create_table = ibm_db.exec_immediate(conn, SQL)
print("Table created")

# insert data
SQL = "INSERT INTO products(rowid,product,category)  VALUES(?,?,?);"
stmt = ibm_db.prepare(conn, SQL)
row1 = (1,'Television','Electronics')
ibm_db.execute(stmt, row1)
row2 = (2,'Laptop','Electronics')
ibm_db.execute(stmt, row2)
row3 = (3,'Mobile','Electronics')
ibm_db.execute(stmt, row3)

# query data
SQL="SELECT * FROM products"
stmt = ibm_db.exec_immediate(conn, SQL)
tuple = ibm_db.fetch_tuple(stmt)
while tuple != False:
    print (tuple)
    tuple = ibm_db.fetch_tuple(stmt)

# close connection
ibm_db.close(conn)
```
> ```
> Connected to database:  bludb as user:  prx70334 on host:  6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud
> Table created
> (1, 'Television', 'Electronics')
> (2, 'Laptop', 'Electronics')
> (3, 'Mobile', 'Electronics')
> ```


By default, the price and timestamp columns in our `sales_data` table have nullable records. I ran the following statement to fix this:

```sql
ALTER TABLE sales_data
ALTER COLUMN timestamp SET DATA TYPE TIMESTAMP;

ALTER TABLE sales_data
ALTER COLUMN timestamp SET NOT NULL;

ALTER TABLE sales_data
ALTER COLUMN timestamp SET DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE sales_data
ALTER COLUMN price SET NOT NULL;

ALTER TABLE sales_data
ALTER COLUMN price SET DEFAULT 0;
```

Using the methods above, I wrote a script `automation.py` that automates the syncronization process between our MySQL table and IBM DB2 instance. I used `mysql.connector` and `ibm_db` to connect to MySQL and IBM DB2 respectively. I returned the `rowid` value of the last column in DB2 as `last_row_id`. I also returned a list of all records from MySQL whose `rowid` is greater than `last_row_id` as `new_records`. I then inserted `new_records` into the DB2 instance
```python
#import libraries
import mysql.connector
import ibm_db

# Connect to MySQL
username = 'root'
pw = 'PW'
host = '127.0.0.1'
db = 'sales'
connection = mysql.connector.connect(user=username, password=pw,host=host,database=db)
cursor = connection.cursor()

# Connect to DB2
dsn_hostname = "6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud"
dsn_uid = "prx70334"        
dsn_pwd = "PW"      
dsn_port = "30376"               
dsn_database = "bludb"           
dsn_driver = "{IBM DB2 ODBC DRIVER}"           
dsn_protocol = "TCPIP"            
dsn_security = "SSL"              
dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};").format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)
conn = ibm_db.connect(dsn, "", "")
print ("Connected to database: ", dsn_database, "as user: ", dsn_uid, "on host: ", dsn_hostname)

# Find out the last rowid from DB2 data warehouse
def get_last_rowid():
    last_rowid_sql = 'SELECT rowid FROM sales_data ORDER BY rowid DESC LIMIT 1'
    last_rowid_stmt = ibm_db.exec_immediate(conn, last_rowid_sql)
    while ibm_db.fetch_row(last_rowid_stmt) != False:
        return ibm_db.result(last_rowid_stmt, 0)
last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
def get_latest_records(rowid):
    latest_records_sql = f'SELECT * FROM sales_data WHERE rowid > {last_row_id} ORDER BY rowid ASC'
    cursor.execute(latest_records_sql)
    return cursor.fetchall()
new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))

# Insert the additional records from MySQL into DB2 data warehouse.
def insert_records(records):
    insert_sql = "INSERT INTO sales_data (rowid, product_id, customer_id, quantity) VALUES (?,?,?,?);"
    insert_stmt = ibm_db.prepare(conn, insert_sql)
    for row in records:
        ibm_db.execute(insert_stmt, row)
insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# Disconnect 
connection.close()
ibm_db.close(conn)
```

Before I can call this script, I needed to call a statement on our DB2 instance to avoid `Exception Error: Code "7" SQLSTATE=57007 SQLCODE=-668`. More info here: https://www.ibm.com/docs/en/db2-for-zos/11?topic=codes-668
```sql
call sysproc.admin_cmd('reorg table PRX70334.SALES_DATA');
```

I ran `automation.py` and tested the syncronization of the data. The console outputs are returned as expected. After inspecting the DB2 instance data, I confirmed the syncronization has succeeded.

```console
python3 automation.py
```
> ```
> Connected to database:  bludb as user:  prx70334 on host:  6667d8e9-9d4d-4ccb-ba32-21da3bb5aafc.c1ogj3sd0tgtu0lqde00.databases.appdomain.cloud
> Last row id on production datawarehouse =  12289
> New rows on staging datawarehouse =  1650
> New rows inserted into production datawarehouse =  1650
> ```

## Apache Airflow ETL And Data Pipelines
SoftCart has imported web server log files as `accesslog.txt`. I wrote an Airflow DAG pipeline that analyzed the log files, extracted the required lines and fields, transformed and loaded the data to an existing file. Airflow DAGs uses Python scripts to automate ETL pipelines. I wrote a script called `process_web_log.py` that extracts web server log data, filters out a specified IP address, and loads the new data into a log file `weblog.tar`.

Tool(s): Apache AirFlow

First, I loaded the dependences. This script will be using the `airflow` library along with the `bash_operator` for executing commands and `datetime` for specifying date/time fields.
```python
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
import datetime as dt
```

Next, I defined the DAG arguments. 
```python
default_args = {
  'owner': 'me',
  'start_date': dt.datetime(2023,2,14),
  'email': ['brandon@ibmcapstone.org'],
}
```

Then, I defineed the DAG. The DAG id will be labeled `process_web_log` and will be scheduled to run daily.
```python
dag=DAG(
  'process_web_log',
  description='SoftCart access log ETL pipeline',
  default_args=default_args,
  schedule_interval=dt.timedelta(days=1),
)
```

After, I defined the 3 tasks - `extract_data`, `transform_data`, and `load_data`. The first task `extract_data` will use the `BashOperator` to cut all IP adddresses from `accesslog.txt` and output them into a new file `extracted_data.txt`. The `transform_data` task will remove all instances of the IP `198.46.149.143` and output the remaining list into `transformed_data.txt`. The `load_data` task will archive the contents of `transformed_data.txt` into a .tar file named `weblog.tar`.

```python
extract_data = BashOperator(
  task_id='extract_data',
  bash_command='cut -f1 -d" " $AIRFLOW_HOME/dags/capstone/accesslog.txt > $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
  dag=dag,
)

transform_data = BashOperator(
  task_id='transform_data',
  bash_command='grep -vw "198.46.149.143" $AIRFLOW_HOME/dags/capstone/extracted_data.txt > $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
  dag=dag,
)

load_data = BashOperator(
  task_id='load_data',
  bash_command='tar -zcvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
  dag=dag,
)
```

Finally, I defined the task pipeline. 
```python
extract_data >> transform_data >> load_data
```

I submitted the DAG. 
```console
cp process_web_log.py $AIRFLOW_HOME/dags
```

I used `airflow dags list` along with `grep` to confirm the submission of the new DAG.
```console
airflow dags list | grep 'process_web_log'
```
> ```
> process_web_log.py | me | True
> ```

I unpaused the DAG. 
```console
airflow dags unpause process_web_log
```
> ```
> Dag: process_web_log, paused: False
> ```

Before I ran the DAG, I changed the permissions of the working directory.
```console
sudo chmod 777 /home/project/airflow/dags/capstone
```

Finally, I used `airflow dags trigger` to run the DAG.
```console
airflow dags trigger process_web_log
```
> ```
> Created <DagRun process_web_log @ 2025-02-14T05:42:50+00:00: manual__2023-02-14T05:42:50+00:00, externally triggered: True>
> ```

Full log of the successful run can be viewed here: [extract_data.log](/Data/extract_data.log)

## Apache Spark Big Data Analytics
There is a set of data containing search terms on the e-Commerce platform. I downloaded the data and ran analytic queries on it using `pyspark` and `JupyterLab`. I used a pretrained sales forecasting model to predict the sales for 2023.

First, I installed `pyspark`. 

```python
!pip install pyspark
!pip install findspark
```

Then I created a spark session with a context class. 
```python
import findspark
findspark.init()

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# Creating a spark context class
sc = SparkContext()

# Creating a spark session
spark = SparkSession \
    .builder \
    .appName("Saving and Loading a SparkML Model").getOrCreate()
```

I downloaded the source file that contained the search term data `searchterms.csv` and loaded it into a dataframe `seach_term_dataset`.
```python
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/searchterms.csv
src = 'searchterms.csv'
search_term_dataset = spark.read.csv(src)
```

I then inspected the dataframe by displaying the number of rows and columns.
```python
row_count = search_term_dataset.count()
col_count = len(search_term_dataset.columns)
print(f'Row count for search_term_dataset: {row_count}')
print(f'Column count for search_term_dataset: {col_count}')
```
> ```
> Row count for search_term_dataset: 10001
> Column count for search_term_dataset: 4
> ```

I viewed the 5 rows of the dataset to get an overview of the data I'm working with. There were 4 columns `_c0`, `_c1`, `_c2`, and `_c3` attributed to search `day`, `month`, `year`, and `searchterm` respectively.
```python
search_term_dataset.show(5)
```
> ```
> +---+-----+----+--------------+
> |_c0|  _c1| _c2|           _c3|
> +---+-----+----+--------------+
> |day|month|year|    searchterm|
> | 12|   11|2021| mobile 6 inch|
> | 12|   11|2021| mobile latest|
> | 12|   11|2021|   tablet wifi|
> | 12|   11|2021|laptop 14 inch|
> +---+-----+----+--------------+
> only showing top 5 rows
> ```

I then displayed the datatype for the `searchterm` column.
```python
print(search_term_dataset.schema['_c3'].dataType)
```
> ```
> StringType
> ```

To know how many times the term `gaming laptop` was searched. I created a Spark view from the dataframe and ran a simple COUNT() query with `spark.sql` where `_c3` is equal to `gaming laptop`. 
```python
search_term_dataset.createOrReplaceTempView('searches')
spark.sql('SELECT COUNT(*) FROM searches WHERE _c3="gaming laptop"').show()
```
> ```
> +--------+
> |count(1)|
> +--------+
> |     499|
> +--------+
> ```

To know the top 5 most frequently used search terms, I used `spark.sql` to select the search terms and group them by their count, with a descending order by count.
```python
spark.sql('SELECT _c3, COUNT(_c3) FROM searches GROUP BY _c3 ORDER BY COUNT(_c3) DESC').show(5)
```
> ```
> +-------------+----------+
> |          _c3|count(_c3)|
> +-------------+----------+
> |mobile 6 inch|      2312|
> |    mobile 5g|      2301|
> |mobile latest|      1327|
> |       laptop|       935|
> |  tablet wifi|       896|
> +-------------+----------+
> only showing top 5 rows
> ```

## 4. Sales Forecasting Model

To predict sales, I used a pretrained sales forecasting model. After downloading the model, I created a dataframe `sales_prediction_df` from the parquet and printed the schema.

```python
!wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0321EN-SkillsNetwork/Bigdata%20and%20Spark/model.tar.gz
!tar -xvzf model.tar.gz

sales_prediction_parquet = 'sales_prediction.model/data/part-00000-1db9fe2f-4d93-4b1f-966b-3b09e72d664e-c000.snappy.parquet'
sales_prediction_df = spark.read.parquet(sales_prediction_parquet)
sales_prediction_df.printSchema()
```
> ```
> root
>  |-- intercept: double (nullable = true)
>  |-- coefficients: vector (nullable = true)
>  |-- scale: double (nullable = true)
> ```

Next, I loaded the model into `sales_prediction_model`
```python
from pyspark.ml.regression import LinearRegressionModel
sales_prediction_model = LinearRegressionModel.load('sales_prediction.model')
```

I used the sales forecast model to predict the sales for the year of 2023.
```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
def predict(year):
    assembler = VectorAssembler(inputCols=["year"],outputCol="features")
    data = [[year,0]]
    columns = ["year", "sales"]
    _ = spark.createDataFrame(data, columns)
    __ = assembler.transform(_).select('features','sales')
    predictions = sales_prediction_model.transform(__)
    predictions.select('prediction').show()
predict(2023)
```
> ```
> +------------------+
> |        prediction|
> +------------------+
> |175.16564294006457|
> +------------------+
> ```


