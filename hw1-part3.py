from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *

sc = SparkContext("local", "myApp")
# read files
lines = sc.textFile("epa-http.txt")
# map the data to extract the ip address, log time and the bytes to form a RDD
rdd = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[1], x.split(" ")[len(x.split(" ")) - 1]))
rdd = rdd.filter(lambda x: x[2] != "-")
# change the log time from string type into Timestamp time, change bytes from string to integer
rdd = rdd.map(lambda x: (x[0], datetime.strptime("18-01-"+x[1][1:3]+x[1][4:-1],'%y-%m-%d%H:%M:%S'), int(x[2])))

#############################################################
# Use spark SQL to set up the builder and form the database chema type
spark = SparkSession.builder \
    .master("local") \
    .appName("myApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
# schema:
# [IP_address", String]
# ["Time", Datetime](in the format (yyyy-mm-dd hour:min:sec))
# ["Bytes", Integer]
schema = StructType([
    StructField("IP_address", StringType(), True),
    StructField("Time", TimestampType(), True),
    StructField("Bytes", IntegerType(), True)])
# Use the data in RDD and schema to create a dataframe
df = spark.createDataFrame(rdd, schema)
#############################################################

#############################################################
# set up a time tulmbling window with 1 hour time interval
# window(timeColumn, windowDuration, slideDuration=None, startTime=None)
# slideDuration=None: tumbling window
# startTime=None: the window will automatically set up from a integral time point
w = window("Time", "1 hour")
# group the data by "IP_address" and the window time interval
# aggrate the window to compute the bytes from typical ip in every time interval
# order by the start time and show(10) for sample
w1 = df.groupby("IP_address", w).agg(sum("Bytes").alias("Bytes")).orderBy("window.start")
w1.show(10, False)

#############################################################
# print the time interval as we want
print(w1.filter("dayofmonth(window.start) = 29 and hour(window.start) = 23").collect())
print(w1.filter("dayofmonth(window.start) = 30 and hour(window.start) = 00").collect())
print(w1.filter("dayofmonth(window.start) = 30 and hour(window.start) = 01").collect())










# df.show(10)