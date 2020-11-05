# Spark Data Pipeline to MySQL and MongoDB

This data pipeline is part of a COVID-19 Dashboard that is in work. This part of the project involves 
following steps and aims to demonstrate a simple data pipeline involving data manipulation and query to remote databases on cloud
1) Reading raw CSV data, converting to Spark DataFrame and cleansing data
2) Insert COVID data aggregated by date and country and country latitude/longitude information onto MySQL On AWS EC2
3) Insert COVID data aggregated by date to MongoDB on AWS EC2

Later, React + Node based dashboard communicate with the databases and fetch necessary data to display according to user's input


## Note about working environment

* The code was developed on Anaconda Package based on Python version 3.6.9 and pyspark version 2.4.4.


## File Explanation

* covid_19_data.csv -> Accumulated Covid dataset from Jan.22 ~ Sep.23rd listing number of confirmed, deaths and recovered by date and country
* country.csv -> CSV containing name of countries and their latitude/longitude
* spark_analysis.py -> Python script used to handle data manipulation through Spark DataFrame and update data to remote databases using mysql.connector and pymongo
