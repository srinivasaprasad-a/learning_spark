from pyspark.sql import SparkSession

import os


spark = SparkSession\
    .builder\
    .appName('simple dataframe')\
    .enableHiveSupport()\
    .getOrCreate()

res_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources', 'stocks')
apple_df = spark.read.csv(os.path.join(res_path, 'Apple.csv'))
#google_df = spark.read.csv(os.path.join(res_path, 'Google.csv'))
#print apple_df.first()
#print apple_df.take(5)

apple_df.createOrReplaceTempView("apple_table")
apple_sql_df = spark.sql("select * from apple_table")
# to cache the table
#apple_sql_df.cache()
#print apple_sql_df.take(5)

# working on row and column level data
output_rdd = apple_sql_df.rdd.map(lambda row: row[0])
print output_rdd.take(5)

spark.stop()
