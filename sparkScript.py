#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import sys
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,FloatType,DoubleType,StringType,IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row
from itertools import islice


#Setup and Cluster configuration

confCluster = SparkConf().setAppName("Seoul_Pollution_Cluster")
confCluster.set("spark.executor.memory", "8g")
confCluster.set("spark.executor.cores", "4")
repartition_count = 32
sc = SparkContext(conf=confCluster)
sqlContext = SQLContext(sc)

#Reading in Data

def cleanse_address(x):
	result = re.search('(".*?")', x)
	if result:
		address = result.group(1)
		new_address = address[1:-1].replace(",", ";") # remove the " and replace the commata
		x = x.replace(address, new_address)
	return x

pollutionData = sc.textFile("Measurement_summary.csv", minPartitions=repartition_count)
pollutionData = pollutionData.mapPartitionsWithIndex(lambda i, iter: islice(iter, 1, None) if i == 0 else iter)

#test = pollutionData.take(3)
#print()
#print(test)
#print()

# replace all commata in address strings with semicolons
pollutionData = pollutionData.map(cleanse_address)

#test = pollutionData.take(3)
#print()
#print(test)
#print()

pollutionData = pollutionData.map(lambda x: x.split(','))
pollutionData = pollutionData.map(lambda x: (x[0], int(x[1]), x[2], float(x[3]), float(x[4]), float(x[5]), float(x[6]), float(x[7]), float(x[8]), float(x[9]), float(x[10]))).persist()

schema = StructType([ \
    StructField("date",StringType(),False), \
    StructField("station",IntegerType(),False), \
    StructField("address",StringType(),False), \
    StructField("latitude", FloatType(), False), \
    StructField("longitude", FloatType(), False), \
    StructField("SO2", FloatType(), False), \
    StructField("NO2", FloatType(), False), \
    StructField("O3", FloatType(), False), \
    StructField("CO", FloatType(), False), \
    StructField("PM10", FloatType(), False), \
    StructField("PM2_5", FloatType(), False), \
  ])

df = sqlContext.createDataFrame(pollutionData, schema)

#test = df.take(5)
#print()
#print(test)
#print()

#filterTestDF = df.filter(df.station == 101)
#test = filterTestDF.take(5)
#print()
#print(test)
#print()

#First Analysis: average values per week

weekDates = df.withColumn("date", F.weekofyear(F.to_date("date")))

test = weekDates.take(3)
print()
print(test)
print()

# total averages per week
totalWeekAverages = weekDates.groupBy("date").avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
# averages per station per week
weekAverages = weekDates.groupBy("date","station").avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")

test = totalWeekAverages.take(3)
print()
print(test)
print()

test = weekAverages.take(3)
print()
print(test)
print()

