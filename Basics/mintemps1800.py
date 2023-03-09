from pyspark import SparkConf, SparkContext
import datetime
print(f"Init:\t\t{datetime.datetime.now()}")
conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)
print(f"Spark ready:\t{datetime.datetime.now()}")

def extractValues(line):
    data = line.split(',')
    stationID = data[0]
    entryType = data[2]
    temperature = float(data[3])*0.1
    return (stationID, entryType, temperature)

lines = sc.textFile(r"..\resources\1800.csv")
weatherEntries = lines.map(extractValues)
minTemps = weatherEntries.filter(lambda x: "TMIN" in x[1])
minTemps = minTemps.map(lambda x: (x[0], x[2]))
lowestTemps = minTemps.reduceByKey(lambda x,y: min(x,y))
results = lowestTemps.collect()
for temp in results:
    print(temp)
print(f"Finish:\t\t{datetime.datetime.now()}")

