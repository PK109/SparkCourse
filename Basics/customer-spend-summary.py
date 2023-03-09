from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("CustomerSummary")
sc = SparkContext(conf = conf)

def splitData(line):
    dataList = line.split(',')
    id = int(dataList[0])
    value = float(dataList[2])
    return (id, value)

lines = sc.textFile(r"..\resources\customer-orders.csv")
rdd = lines.map(splitData)
customerTotals = rdd.reduceByKey(lambda x,y: x+y).map(lambda x:(x[1],x[0])).sortByKey()

results = customerTotals.collect()

for [total,id] in results:
    print(f"Customer  {id}:\t ${round(total,2)}")
