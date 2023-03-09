
from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local").setAppName("CustomerSummary")
sc = SparkContext(conf= conf)

text = "Well the main problem with the initial results from our word count script, is that we didn't account \
        for things like punctuation and capitalization and things like that."
def lineExtraction(line):
    fields = line.split(',')
    return fields[0], fields[1]

def loadMovieNames():
    movieNames = {}
    lines = sc.textFile(r"..\resources\ml-100k\moviesWOheader.csv")
    rdd = lines.map(lineExtraction)
    results = rdd.collect()
    for line in results:
        movieNames[int(line[0])] = line[1]
    print(movieNames)
    return movieNames

loadMovieNames()