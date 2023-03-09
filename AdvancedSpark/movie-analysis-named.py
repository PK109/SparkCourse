from pyspark.sql import SparkSession, functions as fn
from pyspark import SparkContext, SparkConf

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

#conf = SparkConf().setMaster("local").setAppName("CustomerSummary")
# sc = SparkContext(conf= spark.sparkContext)

def lineExtraction(line):
    fields = line.split(',')
    return fields[0], fields[1]

def loadMovieNames():
    movieNames = {}
    lines = spark.sparkContext.textFile(r"..\resources\ml-100k\moviesWOheader.csv")
    rdd = lines.map(lineExtraction)
    results = rdd.collect()
    for line in results:
        movieNames[int(line[0])] = line[1]
    return movieNames

nameDict = spark.sparkContext.broadcast(loadMovieNames())

# userId,movieId,rating,timestamp
ratings = spark.read.option('header','true').option('inferSchema','true').csv(r"..\resources\ml-100k\ratings.csv")
topMovie = ratings.select("movieId").groupBy("movieId").count().orderBy(fn.desc("count"))

def lookupMovie(movieID):
    return nameDict.value[movieID]

lookupNameUDF = fn.udf(lookupMovie)

# noinspection PyTypeChecker
moviesWithNames = topMovie.withColumn("movieTitle", lookupNameUDF(fn.col("movieID")))

moviesWithNames.show()

spark.stop()