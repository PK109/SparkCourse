from pyspark.sql import SparkSession, functions as fn
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("CustomersSpent").master("local[*]").getOrCreate()

#Creating structure of importing schema
schema = StructType([
    StructField("CustID" ,   IntegerType(), True),
    StructField("ProductID", IntegerType(), True),
    StructField("Value",     FloatType(), True)
    ])

df = spark.read.schema(schema).csv(r"..\resources\customer-orders.csv")
df.printSchema()

groupedSpend = df.groupBy("CustID").agg(fn.round(fn.sum("Value"),0).alias("Acc Spend")).sort(fn.col("Acc Spend"))

groupedSpend.show(groupedSpend.count())
# Creating new column with its definition
# negativeTemps = stationMinTemps.withColumn("negTemperature", (fn.col("Minimums") * (-1))).select("stationID","negTemperature").sort("negTemperature")
#
# rows = negativeTemps.collect()
#
# for row in rows:
#     print(row[0] + ":\t" + str(row[1]))
#

spark.stop()