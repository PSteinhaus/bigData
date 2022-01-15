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
    StructField("SO2", FloatType(), True), \
    StructField("NO2", FloatType(), True), \
    StructField("O3", FloatType(), True), \
    StructField("CO", FloatType(), True), \
    StructField("PM10", FloatType(), True), \
    StructField("PM2_5", FloatType(), True), \
  ])

# first turn date strings to timestamps
df = sqlContext.createDataFrame(pollutionData, schema)
df = df.withColumn("date", F.to_timestamp("date")).persist()

# then set all measurement data that is -1 to null
df = df.replace(-1,None)



###################################################
# Zeroth Analysis: give an overview over the data #
###################################################
#df.describe().show()


#####################################################
# First Analysis: quality level for each data point #
#####################################################

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
                                                                     item_name +""" < """+ str(levels[0][6]) +""", 3, IF("""+ \
                                                                     item_name +""" IS NULL, NULL, 4)))))""") )
	return df

quality = calc_quality(df, infoDF, "SO2")
df.unpersist()
quality = calc_quality(quality, infoDF, "NO2")
quality = calc_quality(quality, infoDF, "O3")
quality = calc_quality(quality, infoDF, "CO")
quality = calc_quality(quality, infoDF, "PM10")
quality = calc_quality(quality, infoDF, "PM2_5")


##########################################################################################
# Bonus Analysis: show me the values that lie in quality level 4 (worse than "very bad") #
##########################################################################################
horrible = quality.filter(F.col("SO2_quality") == 4)
#horrible.show(200)
horrible = quality.filter(F.col("CO_quality") == 4)
#horrible.show(200)
horrible = quality.filter(F.col("O3_quality") == 4)
#horrible.show(200)
horrible = quality.filter(F.col("NO2_quality") == 4)
#horrible.show(200)
horrible = quality.filter(F.col("PM2_5_quality") == 4)
#horrible.show(200)
horrible = quality.filter(F.col("PM10_quality") == 4)
#horrible.show(200)

# Result: quality level 4 seem to be measurement errors for Particulate Matter (PM), but might be legitimate for the other items (there are only a couple of such datapoints for each item though, so they don't matter really)
#         -> therefore we set the values that lie in level 4 to null for PM
quality = quality.withColumn("PM2_5", F.expr("""IF(PM2_5_quality == 4, NULL, PM2_5)"""))
quality = quality.withColumn("PM10", F.expr("""IF(PM10_quality == 4, NULL, PM10)""")).persist()

#quality.describe().show()


############################################
# Second Analysis: average values per week #
############################################

# preselection: select a certain year:
firstInput = quality #.filter(F.year("date") == 2017)

weekDates = firstInput.withColumn("date", F.weekofyear("date"))

# total averages per week
grouped = weekDates.groupBy("date")
totalWeekAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
totalWeekStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

totalWeekAverages.show(10)
totalWeekStdDeviations.show(10)

# averages per station per week
grouped = weekDates.groupBy("date","station")
weekAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
weekStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

weekAverages.show(8)
weekStdDeviations.show(8)


#################################################################################################
# Third Analysis: percentage of time spent in each quality level per measured item, per station #
#################################################################################################
"""
def q_percentage(qualityDF, item_name):
	# count of datapoints per station
	datapointCount = qualityDF.groupBy("station").count().withColumnRenamed("count", "totalCount")

	# count of datapoints with a certain quality level, per station
	qLevelCount = qualityDF.groupBy(item_name+"_quality", "station", "latitude", "longitude").count()
	
	# join them to make the next operation possible
	qLevelCount = qLevelCount.join(datapointCount, "station")
	
	# divide count of datapoints with a certain quality level through total datapoint-count per station,
	# to get the percentage of time spent in this quality level;
	# add one column per quality level to the given dataframe
	qLevelPercentages = qLevelCount.withColumn("percentage", F.col("count") / F.col("totalCount"))
	
	# keep latitude and longitude for easier visualization later
	return qLevelPercentages.select("station", "latitude", "longitude", item_name+"_quality", "percentage");

SO2percentages = q_percentage(quality, "SO2")
NO2percentages = q_percentage(quality, "NO2")
O3percentages = q_percentage(quality, "O3")
COpercentages = q_percentage(quality, "CO")
PM10percentages = q_percentage(quality, "PM10")
PM2_5percentages = q_percentage(quality, "PM2_5")

#SO2percentages.write.csv("SO2_level_percentages.csv")
#NO2percentages.write.csv("NO2_level_percentages.csv")
#O3percentages.write.csv("O3_level_percentages.csv")
#COpercentages.write.csv("CO_level_percentages.csv")
#PM10percentages.write.csv("PM10_level_percentages.csv")
#PM2_5percentages.write.csv("PM2_5_level_percentages.csv")

PM10percentages.show(3)
"""
