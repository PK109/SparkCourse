from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([
                     StructField("id", IntegerType(), True),
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv(r"..\resources\Marvel+names")

lines = spark.read.text(r"..\resources\Marvel+graph")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

mostObscure = connections.filter(func.col("connections")== 1).select("id")
mostObscureCharacters= names.join(mostObscure, "id")
mostObscureCharacters.sort(func.col("id").desc()).select("id","name").show()
print(f"There are {mostObscureCharacters.count()} characters, that have appeared just once in the social graph")
spark.stop()