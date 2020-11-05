# -*- coding: utf-8 -*-
"""
Created on Sun Nov  1 21:42:49 2020

@author: Charlie
"""

import findspark
findspark.init()

import pyspark.sql.functions as fc
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

import mysql.connector
from pymongo import MongoClient

def covid_data_extract(spark, file_name):
    '''Read in the csv data of COVID cases by each date and country and create 
    1) aggregated data by date and country (will go to MySQL)
    2) aggregated data by date (will go to MongoDB)
    '''
    covid_data = spark.read.csv(file_name, header = 'true')
    covid_data_filtered = covid_data[['SNo', 'ObservationDate', 'Country/Region', 'Confirmed', 'Deaths', 'Recovered']]
    
    '''Some country names in the raw COVID files do not match the country names in country.json. In order to maintain
    the FK to PK integrity, some names will be fixed'''
    covid_data_filtered = covid_data_filtered.withColumn("Country/Region", when(fc.col("Country/Region") == "Mainland China", "China")\
                                             .when(fc.col("Country/Region") == "US", "United States")\
                                             .when(fc.col("Country/Region") == "UK", "United Kingdom")\
                                             .when(fc.col("Country/Region") == "occupied Palestinian territory", "Palestinian Territories")\
                                             .when(fc.col("Country/Region") == "West Bank and Gaza", "Palestinian Territories")\
                                             .when(fc.col("Country/Region") == "The Gambia", "Gambia")\
                                             .when(fc.col("Country/Region") == "The Bahamas", "Bahamas")\
                                             .when(fc.col("Country/Region") == "South Sudan", "Sudan")\
                                             .when(fc.col("Country/Region") == "Reunion", "Réunion")\
                                             .when(fc.col("Country/Region") == "Republic of the Congo", "Congo [Republic]")\
                                             .when(fc.col("Country/Region") == "Republic of Ireland", "Ireland")\
                                             .when(fc.col("Country/Region") == "Palestine", "")\
                                             .when(fc.col("Country/Region") == "North Macedonia", "Macedonia [FYROM]")\
                                             .when(fc.col("Country/Region") == "North Ireland", "Ireland")\
                                             .when(fc.col("Country/Region") == "Ivory Coast", "Côte d'Ivoire")\
                                             .when(fc.col("Country/Region") == "Holy See", "Vatican City")\
                                             .when(fc.col("Country/Region") == "Gambia, The", "Gambia")\
                                             .when(fc.col("Country/Region") == "Eswatini", "Swaziland")\
                                             .when(fc.col("Country/Region") == "East Timor", "Timor-Leste")\
                                             .when(fc.col("Country/Region") == "Congo (Kinshasa)", "Congo [DRC]")\
                                             .when(fc.col("Country/Region") == "Congo (Brazzaville)", "Congo [Republic]")\
                                             .when(fc.col("Country/Region") == "Channel Islands", "United Kingdom")\
                                             .when(fc.col("Country/Region") == "Cabo Verde", "Cape Verde")\
                                             .when(fc.col("Country/Region") == "Burma", "Myanmar [Burma]")\
                                             .when(fc.col("Country/Region") == "Bahamas, The", "Bahamas")\
                                             .when(fc.col("Country/Region") == " Azerbaijan", "Azerbaijan")\
                                             .otherwise(fc.col("Country/Region")))
    
    '''aggregate by ObservationDate and Country/Region'''
    covid_grouped = covid_data_filtered.groupBy(['ObservationDate', 'Country/Region']).agg(fc.sum('Confirmed'), fc.sum('Deaths'), fc.sum('Recovered'))\
                                       .orderBy(['ObservationDate', 'Country/Region'], ascending = [True, True]).collect()
    
    '''Another aggregate purely by date. This is used to display aggregated amount up to that date'''
    covid_date_grouped = covid_data_filtered.groupBy('ObservationDate').agg(fc.sum('Confirmed'), fc.sum('Deaths'), fc.sum('Recovered'))\
                                            .orderBy('ObservationDate').collect()
                                            
    return covid_grouped, covid_date_grouped

def country_data_extract(spark, file_name):
    '''This function simply reads in csv containing name of the country and its latitude and longitude'''
    country = spark.read.csv('country.csv', header = 'true')
    country_filtered = country[['country', 'latitude', 'longitude']]
    country_collect = country_filtered.collect()
    country_list = []
    for item in country_collect:
        cur_item = (item['country'], float(item['latitude']), float(item['longitude']))
        country_list.append(cur_item)
    return country_list

def insert_country_data(cursor, data, execute):
    '''Insert into country table under dsci551 database name of country, latitude and longitude'''
    if execute:
        query = "INSERT INTO Country (name, latitude, longitude) VALUES (%s, %s, %s)"
        cursor.executemany(query, data)
        connection.commit()

def insert_country_date_aggregate(cursor, data, execute):
    '''Insert into DailyCase table under dsci551 database the number of cases aggregated by date and country'''
    if execute:
        '''For a large dataset like this, you need to chunk up the data when inserting into mysql
        or you can get a timelock error'''
        cur_ind = 0
        cur_chunk = data[cur_ind:100]
        while cur_chunk:
            query = "INSERT INTO DailyCase (datecase, country, confirmed, deaths, recovered) VALUES (%s, %s, %s, %s, %s)"
            cursor.executemany(query, cur_chunk)
            cur_ind += 100
            cur_chunk = data[cur_ind: cur_ind + 100]
            connection.commit()

def insert_date_aggregate(db, data, execute):
    '''Inserts confirmed, deaths and recovered cases to mongoDB'''
    if execute:
        data_cleanse = []
        for i in range(len(data)):
            data_cleanse.append({"_id": i, "date": data[i][0], "confirmed": int(data[i][1]), "death": int(data[i][2]), "recovered": int(data[i][3])})
        db.covidDate.insert_many(data_cleanse)

'''Start spark session'''
spark = SparkSession.builder.appName("Spark Pipeline").config(conf = SparkConf()).getOrCreate()

'''Extract convid data'''
covid_aggregated, covid_date_aggregated = covid_data_extract(spark, 'covid_19_data.csv')

'''Extract country data which consists of name, latitude and longitude'''
country_list = country_data_extract(spark, 'country.csv')
country_dict = {}
for item in country_list:
    country_dict[item[0]] = 1
    
'''Clean up the covid_aggregated data'''
data_cleansed, not_included = [], []
for i in range(len(covid_aggregated)):
    month, day, year = covid_aggregated[i][0].split('/')
    if covid_aggregated[i][1] in country_dict:
        data_cleansed.append((year + "-" + month + "-" + day, covid_aggregated[i][1], covid_aggregated[i][2], covid_aggregated[i][3], covid_aggregated[i][4]))
    else:
        not_included.append(covid_aggregated[i][1])

'''Establish MySQL connection'''
connection = mysql.connector.connect(user='your_mysql_user', password='your_mysql_pwd', host = 'your_mysql_server_id', database = 'your_db')
cursor = connection.cursor()

'''Insert into country database under dsci551 database'''
insert_country_data(cursor, country_list, False)
insert_country_date_aggregate(cursor, data_cleansed, False)

'''Connect to MongoDB and upload confirmed, death and recover cases aggregated by each date'''
mongo_connection = MongoClient('mongodb://%s:%s@your_mongo_host_ip:27017' % ('your_mongo_id', 'your_mongo_pwd'))
db = mongo_connection.project
insert_date_aggregate(db, covid_date_aggregated, False)