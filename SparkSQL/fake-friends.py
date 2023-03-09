from pyspark.sql import SparkSession, functions as fn

spark = SparkSession.builder.appName("FriendsAverageByAge").getOrCreate()

people = spark.read.option('header','true').option('inferSchema','true').csv(r"..\resources\fakefriends-header.csv")

# people.select(['age','friends']).groupBy('age').avg('friends').show()

people.select(['age','friends']).groupBy('age').agg(fn.round(fn.avg('friends'),2).alias('friends_avg')).sort('age').show()

spark.stop()