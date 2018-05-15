from pyspark.sql import SparkSession


spark = SparkSession.builder\
    .master('local')\
    .appName('Hive RW')\
    .enableHiveSupport()\
    .getOrCreate()

spark.sql('CREATE TABLE IF NOT EXISTS sample (name STRING, id INT)')
spark.sql("LOAD DATA LOCAL INPATH 'kv1.txt' INTO TABLE src")
spark.sql('SELECT * FROM sample').show()

spark.stop()
print('End of the Program')
