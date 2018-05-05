from pyspark.sql import SparkSession
from pyspark.sql.types import Row


transactions = [
    {'name': 'Bob', 'amount': 100, 'country': 'United Kingdom'},
    {'name': 'James', 'amount': 15, 'country': 'United Kingdom'},
    {'name': 'Marek', 'amount': 51, 'country': 'Poland'},
    {'name': 'Johannes', 'amount': 200, 'country': 'Germany'},
    {'name': 'Paul', 'amount': 75, 'country': 'Poland'},
]

spark = SparkSession.builder\
    .master('local[4]')\
    .config("spark.sql.shuffle.partitions", 50)\
    .getOrCreate()

trans_rdd = spark.sparkContext\
    .parallelize(transactions)\
    .map(lambda x: Row(**x))

trans_df = spark.createDataFrame(trans_rdd)

# Initial number partitions will be based on the num_of_threads, else default_parallelism
print("Number of partitions: {}".format(trans_df.rdd.getNumPartitions()))
print("Partitioner: {}".format(trans_df.rdd.partitioner))
print("Partitions structure: {}".format(trans_df.rdd.glom().collect()))

# After repartition, num of partitions is based on the spark.sql.shuffle.partitions, if no number is mentioned,
# then it is based on the number in the repartition method
trans_df2 = trans_df.repartition("country")             # Repartition by column
# trans_df2 = trans_df.repartition(10, "country")             # Repartition by column
# if you are increasing the number of partitions use repartition()(performing full shuffle)
# if you are decreasing the number of partitions use coalesce() (minimizes shuffles)

print("\nAfter 'repartition()'")
print("Number of partitions: {}".format(trans_df2.rdd.getNumPartitions()))
print("Partitioner: {}".format(trans_df2.rdd.partitioner))
print("Partitions structure: {}".format(trans_df2.rdd.glom().collect()))

"""
Number of partitions: 2
Partitioner: None
Partitions structure: [[Row(amount=100, country=u'United Kingdom', name=u'Bob'), Row(amount=15, country=u'United Kingdom', name=u'James')], [Row(amount=51, country=u'Poland', name=u'Marek'), Row(amount=200, country=u'Germany', name=u'Johannes'), Row(amount=75, country=u'Poland', name=u'Paul')]]

After 'repartition()'
Number of partitions: 50
Partitioner: None
Partitions structure: [[], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [Row(amount=200, country=u'Germany', name=u'Johannes')], [], [Row(amount=51, country=u'Poland', name=u'Marek'), Row(amount=75, country=u'Poland', name=u'Paul')], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [], [Row(amount=100, country=u'United Kingdom', name=u'Bob'), Row(amount=15, country=u'United Kingdom', name=u'James')], [], [], [], []]
"""