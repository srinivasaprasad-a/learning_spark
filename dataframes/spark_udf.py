from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master('local').appName('Spark UDF').getOrCreate()

spark.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
print spark.sql("select stringLengthInt('test')").collect()
