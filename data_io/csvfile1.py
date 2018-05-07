from pyspark.sql import SparkSession
import csv
import StringIO
import os


def csvrowreader(line):
    input = StringIO.StringIO(line)
    reader = csv.DictReader(input, fieldnames=['user_id', 'user_login_id'])
    return reader.next()


spark = SparkSession.builder.master('local').appName('CSV file').getOrCreate()
sc = spark.sparkContext

input_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'clickstream')
users_file = os.path.join(input_dir, 'users.csv')

csv_file = sc.textFile(users_file).map(lambda x: csvrowreader(x))
csv_file.take(5)

spark.stop()
