from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('Part1').setMaster('local[3]')
sc = SparkContext(conf=sparkconf)

nums = range(1, 10)
rdd = sc.parallelize(nums)
# If you explicitly set the numSlices, then it will be always numSlices number of partitions.
# numSlices takes precedence over defaultParallelism set by threads
# rdd = sc.parallelize(nums, 2)
# In this case - 2 partitions, irrespective of number of threads

print "defaultParallelism: " + str(sc.defaultParallelism)

print "partitioner: " + str(rdd.partitioner)
print "no. of partitions: " + str(rdd.getNumPartitions())
print "rdd partition structure: " + str(rdd.glom().collect())

"""
Output: if master: local and sc.parallelize(nums)

defaultParallelism: 1
partitioner: None
no. of partitions: 1
rdd partition structure: [[1, 2, 3, 4, 5, 6, 7, 8, 9]]

---------------------------------------------------------
Output: if master: local[3]  and sc.parallelize(nums)

defaultParallelism: 3
partitioner: None
no. of partitions: 3
rdd partition structure: [[1, 2, 3], [4, 5, 6], [7, 8, 9]]

----------------------------------------------------------
Output: if master: local and sc.parallelize(nums, 15)

defaultParallelism: 1
partitioner: None
no. of partitions: 15
rdd partition structure: [[], [1], [], [2], [3], [], [4], [], [5], [6], [], [7], [], [8], [9]]

----------------------------------------------------------------------------------------------
Output: if master: local[20] and sc.parallelize(nums)

defaultParallelism: 20
partitioner: None
no. of partitions: 20
rdd partition structure: [[], [], [1], [], [2], [], [3], [], [4], [], [], [5], [], [6], [], [7], [], [8], [], [9]]
"""