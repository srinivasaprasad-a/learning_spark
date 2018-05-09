from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master('local').appName('RW Json').getOrCreate()
sc = spark.sparkContext
input_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_json.json')
input_df = spark.read.json(input_file)
input_df.show()

spark.stop()
