from pyspark.sql import SparkSession, functions as fn
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

#Creating structure of importing schema
schema = StructType([
    StructField("stationID" , StringType(), True),
    StructField("date"      , IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
    ])

df = spark.read.schema(schema).csv(r"..\resources\1800.csv")
df.printSchema()

minTemps = df.filter(df.measure_type == "TMIN")
stationMinTemps = minTemps.select(minTemps.stationID, minTemps.temperature / 10).groupBy("stationID").agg(fn.min("(temperature / 10)").alias("Minimums"))

stationMinTemps.show()
# Creating new column with its definition
negativeTemps = stationMinTemps.withColumn("negTemperature", (fn.col("Minimums") * (-1))).select("stationID","negTemperature").sort("negTemperature")

rows = negativeTemps.collect()

for row in rows:
    print(row[0] + ":\t" + str(row[1]))


spark.stop()