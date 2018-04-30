from pyspark import SparkContext, SparkConf
import os


sparkconf = SparkConf().setAppName('Part1').setMaster('local[20]')
sc = SparkContext(conf=sparkconf)

print "defaultParallelism: " + str(sc.defaultParallelism)

nums = range(1, 20)
input_rdd = sc.parallelize(nums).map(lambda x: (x, x))
rdd = input_rdd.partitionBy(2).persist()

print "partitioner: " + str(rdd.partitioner)
print "no. of partitions: " + str(rdd.getNumPartitions())
print "rdd partition structure: " + str(rdd.glom().collect())

urls_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'urls_small.txt')
urls_rdd = sc.textFile(urls_file)

# It is recommended to persist that partitioned RDD, rather than to partition every time.
urls_rdd.partitionBy(2).persist()

print "partitioner: " + str(urls_rdd.partitioner)
print "no. of partitions: " + str(urls_rdd.getNumPartitions())
print "rdd partition structure: " + str(urls_rdd.glom().collect())

"""
Output:

defaultParallelism: 20

partitioner: <pyspark.rdd.Partitioner object at 0x7fdf078af050>
no. of partitions: 2
rdd partition structure: [[(2, 2), (4, 4), (6, 6), (8, 8), (10, 10), (12, 12), (14, 14), (16, 16), (18, 18)], [(1, 1), (3, 3), (5, 5), (7, 7), (9, 9), (11, 11), (13, 13), (15, 15), (17, 17), (19, 19)]]

partitioner: None
no. of partitions: 2
rdd partition structure: [[u'1,http://www1.hollins.edu/ ', u'2,http://www.hollins.edu/ ',...............
"""