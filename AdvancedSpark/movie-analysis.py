from pyspark.sql import SparkSession, functions as fn

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

# userId,movieId,rating,timestamp
ratings = spark.read.option('header','true').option('inferSchema','true').csv(r"..\resources\ml-100k\ratings.csv")
topMovie = ratings.select("movieId").groupBy("movieId").count().orderBy(fn.desc("count"))

topMovie.show()

spark.stop()