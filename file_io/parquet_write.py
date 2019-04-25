from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master('local').appName('Parquet Write').getOrCreate()
sc = spark.sparkContext

input_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_json.json')
output_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_parquet')

input_df = spark.read.json(input_file)
input_df.write.mode('append').parquet(output_file)

spark.stop()
