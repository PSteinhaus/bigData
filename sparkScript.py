#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import sys
import re

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType
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

test = pollutionData.take(3)
print()
print(test)
print()

# replace all commata in address strings with semicolons
pollutionData = pollutionData.map(lambda x: cleanse_address(x))

test = pollutionData.take(3)
print()
print(test)
print()

pollutionData = pollutionData.map(lambda x: x.split(','))
pollutionData = pollutionData.map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10])).persist()

df = sqlContext.createDataFrame(pollutionData, ["date", "station", "address", "latitude", "longitude", "SO2", "NO2", "O3", "CO", "PM10", "PM2.5"])

test = df.take(5)
print()
print(test)
print()

filterTestDF = df.filter(df.station == 101)
test = filterTestDF.take(5)
print()
print(test)
print()
