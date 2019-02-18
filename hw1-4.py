from pyspark import SparkContext

sc = SparkContext("local", "myApp")
lines = sc.textFile("epa-http.txt")
rdd = lines.map(lambda x: (x.replace(".", " ").split(" ")[0],
                           x.replace(".", " ").split(" ")[1],
                           x.replace(".", " ").split(" ")[2],
                           x.replace('.', ' ').split(' ')[len(x.replace('.', ' ').split(' ')) - 1]))
rdd = rdd.filter(lambda x: x[3] != "-")

JudgeDigit = rdd.filter(lambda x: x[0].isdigit() == True and x[1].isdigit() == True and x[2].isdigit() == True)
res1 = JudgeDigit.map(lambda x: (str(x[0]) + '.' + str(x[1]) + '.' + str(x[2]), x[3]))
res1 = res1.reduceByKey(lambda x, y: int(x) + int(y))
print(res1.take(15))

JudgeSubnet = rdd.filter(lambda x: x[0].isdigit() == False or x[1].isdigit() == False or x[2].isdigit() == False)
res2 = JudgeSubnet.map(lambda x: (str(x[0]) + '.' + str(x[1]), x[3]))
res2 = res2.reduceByKey(lambda x, y: int(x) + int(y))
print(res2.take(15))
