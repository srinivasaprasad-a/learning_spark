from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('RDD Partition').setMaster('local[20]')
sc = SparkContext(conf=sparkconf)

print "defaultParallelism: " + str(sc.defaultParallelism)

nums = range(1, 20)
input_rdd = sc.parallelize(nums).map(lambda x: (x, x))
# It is recommended to persist that partitioned RDD, rather than to partition every time.
rdd = input_rdd.partitionBy(5).persist()
# In case of pairRDD partitionBy value takes precedence over num_of_threads or default parallelism
# Which partition each key-value goes depends on the value of hash(key) % num_partitions
# Actual number of partitions could be less than the num_partitions, due the above formula, but not more than it

# Default partitioner is hashpartitioner
print "partitioner: " + str(rdd.partitioner)
print "no. of partitions: " + str(rdd.getNumPartitions())
print "rdd partition structure: " + str(rdd.glom().collect())

"""
Output:

partitioner: <pyspark.rdd.Partitioner object at 0x7fdf078af050>
no. of partitions: 5
rdd partition structure: [[(5, 5), (10, 10), (15, 15)], [(1, 1), (6, 6), (11, 11), (16, 16)], [(2, 2), (7, 7), (12, 12),
 (17, 17)], [(3, 3), (8, 8), (13, 13), (18, 18)], [(4, 4), (9, 9), (14, 14), (19, 19)]]
"""