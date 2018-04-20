from pyspark.sql import *
from pyspark.sql.types import *
import os


spark = SparkSession.builder.appName('set schema').getOrCreate()

input_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'clickstream')
users_file = os.path.join(input_dir, 'users.csv')

users_schema_names = "user_id user_login_id"
users_fields = [StructField(field_name, StringType(), True) for field_name in users_schema_names.split()]
users_schema = StructType(users_fields)

users_df = spark.read.schema(users_schema).csv(users_file)
users_df.show(5)
