from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import datetime
import os


spark = SparkSession.builder.appName('PageRank').getOrCreate()

links_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'links.csv')
urls_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'urls.csv')

urls_schema_names = "id url"
urls_fields = [StructField(field_name, StringType(), True) for field_name in urls_schema_names.split(" ")]
urls_schema = StructType(urls_fields)

links_schema_names = "link_from_id id"
links_fields = [StructField(field_name, StringType(), True) for field_name in links_schema_names.split(" ")]
links_schema = StructType(links_fields)

# By default partitioner is HashPartitioner
links_df = spark.read.schema(links_schema).csv(links_file)
urls_df = spark.read.schema(urls_schema).csv(urls_file).persist()

# Partitioning only the urls_rdd, since it is larger in size than links_rdd. Then only links_rdd is shuffled across the
# nodes. persisting both the RDDs in memory, might hit memory issues

s = datetime.now()
urls_df.join(links_df, 'id', 'inner').select('id', 'url', 'link_from_id').show(5)
e = datetime.now()
print (e-s).seconds + ' secs'

spark.stop()
