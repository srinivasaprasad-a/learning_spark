from pyspark.sql import SparkSession
import os


spark = SparkSession.builder.master('local').appName('Parquet Read').getOrCreate()
sc = spark.sparkContext

input_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_parquet')

input_df = spark.read.parquet(input_file)
#print input_df.collect()
#<bound method DataFrame.collect of DataFrame[knows: struct<friends:array<string>>, lovesPandas: boolean, name: string]>

#one_col = input_df.rdd.map(lambda row: row[2])
#print one_col.collect()

input_df.createOrReplaceTempView('sample_parquet_table')
output_df = spark.sql('select * from sample_parquet_table')
print output_df.collect()

spark.stop()
