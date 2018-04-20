from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os


spark = SparkSession.builder.appName('Clickstream').getOrCreate()

input_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'clickstream')

users_file = os.path.join(input_dir, 'users.csv')
clicks_file = os.path.join(input_dir, 'clicks.csv')

users_schema_names = "user_id user_login_id"
users_fields = [StructField(field_name, StringType(), True) for field_name in users_schema_names.split()]
users_schema = StructType(users_fields)
users_df = spark.read.schema(users_schema).csv(users_file)
users_df.createOrReplaceTempView('users')

clicks_schema_names = "user_id access_time url_link"
clicks_fields = [StructField(field_name, StringType(), True) for field_name in clicks_schema_names.split()]
clicks_schema = StructType(clicks_fields)
clicks_df = spark.read.schema(clicks_schema).csv(clicks_file)
clicks_df.createOrReplaceTempView('clicks')

# clicks_df.crossJoin(users_df.select('user_login_id')).select('user_id', 'user_login_id', 'access_time', 'url_link') \
#    .show(5)

spark.sql('select u.user_id, u.user_login_id, c.access_time, c.url_link from users u, clicks c '
          'where u.user_id=c.user_id').show(5)
