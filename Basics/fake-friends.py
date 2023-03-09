from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("AvgFakeFriends")
sc = SparkContext(conf = conf)

def extractValues (line):
    data = line.split(',')
    age = int(data[2])
    friendsCount=  int(data[3])
    return (age, friendsCount)

lines = sc.textFile(r"..\resources\fakefriends.csv")
rdd = lines.map(extractValues)
tempRdd = rdd.mapValues(lambda x: (x,1))
totalsByAge = tempRdd.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))
averageByAge = totalsByAge.mapValues(lambda x: (x[1], x[0]/x[1])).sortByKey()
results = averageByAge.collect()

for result in results:
    print(f"Age: {result[0]},\t sample count {result[1][0]},\t Average friends count: {result[1][1]}")

