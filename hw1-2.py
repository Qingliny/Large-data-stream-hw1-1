from pyspark import SparkConf, SparkContext
sc = SparkContext("local", "myApp")
#read from file
lines = sc.textFile("epa-http.txt")
#map files, split the rdd by " ", extract the ip address and the bytes to form a pair RDD
byteLines = lines.map(lambda x: (x.split(" ")[0], x.split(" ")[len(x.split(" ")) - 1]))
#filter the line to substract the rdds with unreal bytes value "-"
byteLines = byteLines.filter(lambda x: x[1] != "-")
#reduce the value by key (IP address) and apply the function to sum up the bytes from the same IP address
#consider the bytes still stored as string, we need to change the type into int
collectBytes= byteLines.reduceByKey(lambda x,y: int(x) + int(y))
#sort the collectByte by value
# use takeOrdered and set the function as descending orders by value
res1 = collectBytes.takeOrdered(10, key=lambda x: -int(x[1]))

print(res1)
