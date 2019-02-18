from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql.functions import *
import time

sc = SparkContext("local", "myApp")
lines = sc.textFile("epa-http.txt")
rdd = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[1], x.split(" ")[len(x.split(" ")) - 1]))
rdd = rdd.filter(lambda x: x[2] != "-")
rdd = rdd.map(lambda x: (x[0], datetime.strptime("18-01-"+x[1][1:3]+x[1][4:-1],'%y-%m-%d%H:%M:%S'), int(x[2])))

spark = SparkSession.builder \
    .master("local") \
    .appName("myApp") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

schema = StructType([
    StructField("IP_address", StringType(), True),
    StructField("Time", TimestampType(), True),
    StructField("Bytes", IntegerType(), True)])

df = spark.createDataFrame(rdd, schema)

# df.groupBy([window("Time", "1 hour", startTime='10 minutes'), "IP_address"]).count().show(10)

w = window("Time", "1 hour")
df.groupby("IP_address", w).agg(sum("Bytes")).orderBy("window.start").show(10, False)

df.show(10)