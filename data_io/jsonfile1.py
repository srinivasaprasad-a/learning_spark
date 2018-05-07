from pyspark.sql import SparkSession
import os
import json


spark = SparkSession.builder.master('local').appName('RW Json').getOrCreate()
sc = spark.sparkContext
input_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_json.json')
output_file = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'sample_json_out')

input = sc.textFile(input_file)
data = input.map(lambda x: json.loads(x))
data.filter(lambda x: 'lovesPandas' in x and x['lovesPandas'])\
    .map(lambda x: json.dumps(x))\
    .saveAsTextFile(output_file)

spark.stop()
