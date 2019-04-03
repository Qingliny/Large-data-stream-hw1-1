from pyspark import SparkContext
import time
import matplotlib.pyplot as plt

sc = SparkContext()
data = sc.textFile('data.txt')
data = data.persist()
data = data.map(lambda x: int(x))

X = [0]*100
Y1 = [1]*100
Y2 = [0]*100
num = data.count()

for i in range(20):
    selectivity = float(i)/100
    X[i] = selectivity

    # first round
    # start A
    firstA_start = time.time()
    dataA = data.filter(lambda x: x % 2 ==0)
    for j1 in range(num):
        through_data_1 = dataA.map(lambda x: x)
    firstA_end = time.time()
    # start B
    num_1 = dataA.count()
    firstB_start = time.time()
    dataB = dataA.filter(lambda x: x <= i)
    for j2 in range(num_1):
        through_data_2 = dataB.map(lambda x: x)
    # result
    result1 = dataB.collect()
    firstB_end = time.time()
    time1 = (firstA_end - firstA_start) + (firstB_end - firstB_end)


    # Reordering
    # start B
    secondB_start = time.time()
    dataB2 = data.filter(lambda x: x <= i)
    for j3 in range(num):
        through_data_3 = dataB2.map(lambda x: x)
    secondB_end = time.time()
    # start A
    num_2 = dataB2.count()
    secondA_start = time.time()
    dataA2 = dataB2.filter(lambda x: x % 2 ==0)
    for j4 in range(num_2):
        through_data_4 = dataA2.map(lambda x: x)
    # result
    result2 = dataA2.collect()
    secondA_end = time.time()
    time2 = (secondB_end - secondB_start) + (secondA_end - secondA_start)
    print(i,"th round")
    Y2[i] = time1 / time2
    print(X[i],Y2[i])


plt.figure()
plt.plot(X, Y1, 'b', X, Y2, 'r')
plt.ylabel('throughput')
plt.xlabel('selectivity of B')
plt.show()









