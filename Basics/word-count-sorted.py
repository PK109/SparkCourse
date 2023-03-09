from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

input = sc.textFile(r"..\resources\Book")
words = input.flatMap(normalizeWords)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1],x[0])).sortByKey()

results = wordCountsSorted.collect()

for count, word in results:
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(str(count) + ":\t\t" + cleanWord.decode())
