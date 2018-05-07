from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master('local').appName('load csv').getOrCreate()
input_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'clickstream')
users_file = os.path.join(input_dir, 'users.csv')

users_df = spark.read.csv(users_file)
users_df.show(5)

spark.stop()
