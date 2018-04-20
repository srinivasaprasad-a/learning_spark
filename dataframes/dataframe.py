from pyspark.sql import SparkSession

import os


spark = SparkSession.builder.appName('hellopyspark').getOrCreate()

res_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources', 'stocks')
apple_df = spark.read.csv(os.path.join(res_path, 'Apple.csv'))
google_df = spark.read.csv(os.path.join(res_path, 'Google.csv'))
print apple_df.first()
print apple_df.take(5)
