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
repartition_count = 16
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
# skip the first line
pollutionData = pollutionData.mapPartitionsWithIndex(lambda i, iter: islice(iter, 1, None) if i == 0 else iter)

# replace all commata in address strings with semicolons
pollutionData = pollutionData.map(cleanse_address)

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
quality = quality.withColumn("PM10", F.expr("""IF(PM10_quality == 4, NULL, PM10)"""))
# NOW also remove these levels for the other items as they really seem to be mostly measurement errors
quality = quality.withColumn("NO2", F.expr("""IF(NO2_quality == 4, NULL, NO2)"""))
quality = quality.withColumn("SO2", F.expr("""IF(SO2_quality == 4, NULL, SO2)"""))
quality = quality.withColumn("O3", F.expr("""IF(O3_quality == 4, NULL, O3)"""))
quality = quality.withColumn("CO", F.expr("""IF(CO_quality == 4, NULL, CO)"""))
quality.persist()


#quality.describe().show()


############################################
# Second Analysis: average values per week #
############################################

# preselection: select a certain year:
firstInput = quality #.filter(F.year("date") == 2017)

weekDates = firstInput.withColumn("week", F.weekofyear("date"))

weekDates = weekDates.withColumn("year", F.year("date"))

# total averages per week
grouped = weekDates.groupBy("year", "week")
totalWeekAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
totalWeekStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

#totalWeekAverages.show(10)
print("total")
#totalWeekStdDeviations.show(10)
print("total")

#export csv for plots
# mein plot soll zeigen:
#   pro stoff 1 plot
#   pro jahr 1 linie in dem plot
#   fÃ¼r jede woche von linie zu linie
#   erstmal total, nicht pro station
# also will ich ne .csv haben, die zeigt:
#   year | week | avg(item)fÃ¼r alle items
#totalWeekAverages.repartition(1).write.csv("total_week_averages_cleaned.csv")

# averages per station per week
#grouped = weekDates.groupBy("date","station") #!Ã¤ndere "date" zu "week" und "year"
#weekAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
#weekStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

#weekAverages.show(8)
#print("per station")
#weekStdDeviations.show(8)
#print("per station")


##########################
# average values per day #
##########################

# preselection: select a certain year:
firstInput = quality #.filter(F.year("date") == 2017)

dayDates = firstInput.withColumn("day", F.dayofyear("date"))

dayDates = dayDates.withColumn("year", F.year("date"))

# total averages per day
grouped = dayDates.groupBy("year", "day")
totalDayAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
totalDayStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

#totalDayAverages.show(10)
print("total")
#totalDayStdDeviations.show(10)
print("total")

#export csv for plots
# mein plot soll zeigen:
#   pro stoff 1 plot
#   pro jahr 1 linie in dem plot
#   fÃ¼r jede woche von linie zu linie
#   erstmal total, nicht pro station
# also will ich ne .csv haben, die zeigt:
#   year | day | avg(item)fÃ¼r alle items
#totalDayAverages.repartition(1).write.csv("total_day_averages_cleaned.csv")


##############################
# average values per station #
##############################

stationGrouped = quality.groupBy("station")
stationAverages = stationGrouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
#stationAverages.repartition(1).write.csv("station_averages_cleaned.csv")


#################################################################################################
# Third Analysis: percentage of time spent in each quality level per measured item, per station #
#################################################################################################

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

#SO2percentages = q_percentage(quality, "SO2")
#NO2percentages = q_percentage(quality, "NO2")
#O3percentages = q_percentage(quality, "O3")
#COpercentages = q_percentage(quality, "CO")
#PM10percentages = q_percentage(quality, "PM10")
#PM2_5percentages = q_percentage(quality, "PM2_5")

#SO2percentages.repartition(1).write.csv("SO2_level_percentages_cleaned.csv")
#NO2percentages.repartition(1).write.csv("NO2_level_percentages_cleaned.csv")
#O3percentages.repartition(1).write.csv("O3_level_percentages_cleaned.csv")
#COpercentages.repartition(1).write.csv("CO_level_percentages_cleaned.csv")
#PM10percentages.repartition(1).write.csv("PM10_level_percentages_cleaned.csv")
#PM2_5percentages.repartition(1).write.csv("PM2_5_level_percentages_cleaned.csv")

#PM10percentages.show(3)




################
# Tagesverlauf #
################

# add column for hours
dailyProgression = quality.withColumn("hour", F.hour("date")).persist()
#dailyProgression.show(10)

# pre-selection: select a certain year:
#dailyProgression = dailyProgression.filter(F.year("date") == 2017)

# total averages per week
grouped = dailyProgression.groupBy("hour")
totalHourAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
totalHourStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

#totalHourAverages.show(24)
#totalHourStdDeviations.show(24)

# averages per station per week
grouped = dailyProgression.groupBy("hour", "station")
hourAverages = grouped.avg("SO2", "NO2", "O3", "CO", "PM10", "PM2_5")
hourStdDeviations = grouped.agg({"SO2" : "stddev", "NO2" : "stddev", "O3" : "stddev", "CO" : "stddev", "PM10" : "stddev", "PM2_5": "stddev"})

#hourAverages.show(24)
#hourStdDeviations.show(24)

dailyProgression.unpersist()

#export csv for plots
# mein plot soll zeigen:
#   pro stoff 1 plot
#   pro jahr 1 linie in dem plot
#   fÃ¼r jede woche von linie zu linie
#   erstmal total, nicht pro station
# also will ich ne .csv haben, die zeigt:
#   year | week | avg(item)fÃ¼r alle items
#totalHourAverages.repartition(1).write.csv("total_hour_averages_cleaned.csv")


########
# WIND #
########

# read wind data

windData = sc.textFile("WindDataOriginal.csv", minPartitions=repartition_count)
windData = windData.mapPartitionsWithIndex(lambda i, iter: islice(iter, 1, None) if i == 0 else iter)

windData = windData.map(lambda x: x.split(','))
windData = windData.map(lambda x: (x[1], x[2], int(x[3]), int(x[4]), int(x[5]), int(x[6])))

#location,measurement date and time,measurement period,average wind direction 1,average instantaneous wind speed 1,maximum wind angle,maximum wind speed

#0   degrees = north
#90  degrees = east
#180 degrees = south
#270 degrees = west

windSchema = StructType([ \
    StructField("date",StringType(),False), \
    StructField("measurementPeriod",StringType(),False), \
    StructField("averageWindDir",IntegerType(),False), \
    StructField("averageInstWindSpeed",IntegerType(),False), \
    StructField("maxWindAngle",IntegerType(),False), \
    StructField("maxWindSpeed",IntegerType(),False), \
  ])

windDF = sqlContext.createDataFrame(windData, windSchema)
windDF = windDF.withColumn("date", F.to_timestamp("date"))

#windDF.show(3)

# we need hourly values
windDF = windDF.filter(windDF.measurementPeriod == "H")

windDF.show(4)
#windDF.describe().show()

# beide dataframes in 1 dataframe packen
joined_data = quality.join(windDF, "date").persist()

#joined_data.show(5)


# calculate the correlation between average wind speed and pollutant concentration

SO2_corr = joined_data.stat.corr("averageInstWindSpeed", "SO2")
NO2_corr = joined_data.stat.corr("averageInstWindSpeed", "NO2")
CO_corr = joined_data.stat.corr("averageInstWindSpeed", "CO")
O3_corr = joined_data.stat.corr("averageInstWindSpeed", "O3")
PM2_5_corr = joined_data.stat.corr("averageInstWindSpeed", "PM2_5")
PM10_corr = joined_data.stat.corr("averageInstWindSpeed", "PM10")

print("SO2_corr: " + str(SO2_corr))
print("NO2_corr: " + str(NO2_corr))
print("CO_corr: " + str(CO_corr))
print("O3_corr: " + str(O3_corr))
print("PM2_5_corr: " + str(PM2_5_corr))
print("PM10_corr: " + str(PM10_corr))
