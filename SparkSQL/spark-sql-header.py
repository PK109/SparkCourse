from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQLHeader").getOrCreate()

people = spark.read.option('header','true').option('inferSchema','true').csv(r"..\resources\fakefriends-header.csv")

print("Here is our inferred schema:")
people.printSchema()

print("Display selected column:")
people.select('name').show()

print("Show filtered results:")
people.filter(people.age < 21).show()

print("Show aggregated data:")
people.groupBy('age').count().show()

print("Directly affect data values:")
people.select(people.name,10 + people.age).show()

spark.stop()