from pyspark.sql import SparkSession
import csv
import StringIO
import os


def csvrowwriter(records):
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=['name', 'empid'])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]


input = [
    {"name":"Srinivasa Prasad", "empid":"3242341"},
    {"name":"Siva Kumar", "empid":"4356231"}
]

spark = SparkSession.builder.appName('CSV Write').master('local').getOrCreate()
sc = spark.sparkContext

output_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'csv_output')

input_rdd = sc.parallelize(input)
input_rdd.mapPartitions(csvrowwriter).saveAsTextFile(output_file)
