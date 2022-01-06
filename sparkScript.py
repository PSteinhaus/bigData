#!/usr/bin/python
# -*- coding: utf-8 -*-
import time
import sys
import re

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType,StructType,StructField,FloatType,DoubleType,StringType,IntegerType
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
pollutionData = pollutionData.map(lambda x: (x[0], int(x[1]), x[2], float(x[3]), float(x[4]), float(x[5]), float(x[6]), float(x[7]), float(x[8]), float(x[9]), float(x[10])))

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
df = df.withColumn("date", F.to_timestamp("date")).persist()

test = df.take(2)
print()
print(test)
print()

filterTestDF = df.filter(df.date > '2017-01-02')
test = filterTestDF.take(2)
print()
print(test)
print()



#First Analysis: average values per week

weekDates = filterTestDF.withColumn("date", F.weekofyear("date"))

#test = weekDates.take(2)
#print()
#print(test)
#print()

# total averages per week
#totalWeekAverages = weekDates.groupBy("date").avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
# averages per station per week
#weekAverages = weekDates.groupBy("date","station").avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")

#test = totalWeekAverages.take(3)
#print()
#print(test)
#print()

#test = weekAverages.take(3)
#print()
#print(test)
#print()



#Second Analysis: distribution of time spent in the different quality levels

# first, read in the info about the quality levels

infoRaw = sc.textFile("Original Data/Measurement_item_info.csv", minPartitions=repartition_count)
infoRaw = infoRaw.mapPartitionsWithIndex(lambda i, iter: islice(iter, 1, None) if i == 0 else iter)

infoRaw = infoRaw.map(lambda x: x.split(','))
infoRaw = infoRaw.map(lambda x: (int(x[0]), x[1], x[2], float(x[3]), float(x[4]), float(x[5]), float(x[6])))

schema = StructType([ \
    StructField("item_code",IntegerType(),False), \
    StructField("item_name",StringType(),False), \
    StructField("unit",StringType(),False), \
    StructField("good", FloatType(), False), \
    StructField("normal", FloatType(), False), \
    StructField("bad", FloatType(), False), \
    StructField("very_bad", FloatType(), False), \
  ])

infoDF = sqlContext.createDataFrame(infoRaw, schema)

# replace the item name "PM2.5" with "PM2_5" to match what we did earlier
infoDF = infoDF.withColumn("item_name", F.regexp_replace("item_name", "PM2.5", "PM2_5"))

# for each measurement for each item calculate the quality

def calc_quality(df, infoDF, item_name):
	levels = infoDF.filter(infoDF.item_name == item_name).take(1)
	df = df.withColumn(item_name + "_quality", F.expr("""IF("""+ item_name +""" < """+ str(levels[0][3]) +""", 0, IF("""+ \
                                                                     item_name +""" < """+ str(levels[0][4]) +""", 1, IF("""+ \
                                                                     item_name +""" < """+ str(levels[0][5]) +""", 2, IF("""+ \
                                                                     item_name +""" < """+ str(levels[0][6]) +""", 3, 4))))""") )
	return df

quality = calc_quality(df, infoDF, "SO2")
quality = calc_quality(quality, infoDF, "NO2")
quality = calc_quality(quality, infoDF, "O3")
quality = calc_quality(quality, infoDF, "CO")
quality = calc_quality(quality, infoDF, "PM10")
quality = calc_quality(quality, infoDF, "PM2_5")

test = quality.take(2)
print()
print(test)
print()
