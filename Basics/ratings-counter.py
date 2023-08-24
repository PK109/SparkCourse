from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile(r"C:\Users\Pszemegg\SparkCourse\resources\ml-100k\ratings.csv")
ratings = lines.map(lambda x: x.split(',')[2])
movieRatingsCnt = lines.map(lambda x: x.split(',')[1])
result = ratings.countByValue()
movieResult = movieRatingsCnt.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
sortedMovieResults = collections.OrderedDict(sorted(movieResult.items(), key=lambda x:x[1]))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
