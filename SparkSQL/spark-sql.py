from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(
        ID= int(fields[0]),
        name= str(fields[1].encode("utf-8")),
        age= int(fields[2]),
        numFriends= int(fields[3])
        )

lines = spark.sparkContext.textFile(r"..\resources\fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

teenagers =  spark.sql("SELECT * FROM people WHERE age >=13 AND age <=19")

for teenager in teenagers.collect() :
    print(teenager)

schemaPeople.select(['name','age','numFriends']).where('age >=13').where('age <=19').orderBy('numFriends').show()
# schemaPeople.groupBy('age').count().orderBy("age").show()

spark.stop()