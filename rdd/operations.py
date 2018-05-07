from pyspark import SparkConf, SparkContext

sparkconf = SparkConf().setMaster("local").setAppName("RDD Operations")
sc = SparkContext(conf=sparkconf)

# rdd = sc.parallelize([1, 2, 3, 3])
# output_rdd = rdd.map(lambda x: x+1)
# output_rdd = rdd.filter(lambda x: x != 1)
# output_rdd = rdd.distinct()
# output_rdd = rdd.sample(False, 0.5)
# print output_rdd.collect()

# print rdd.count()

# rdd1 = sc.parallelize([1, 2, 3])
# rdd2 = sc.parallelize([3, 4, 5])
# output_rdd = rdd1.union(rdd2)
# rdd1_new = rdd1.subtract(rdd2)
# output_rdd = rdd1.cartesian(rdd2)
# print output_rdd.collect()
# print output_rdd.takeOrdered(6)


strrdd = sc.parallelize([2, 3, 4])
output_strrdd = strrdd.flatMap(lambda x: range(1, x))
# run each element into the function and flatten whole results
# element = 2 :     range(1, 2) :       [1]
# element = 3 :     range(1, 3) :       [1, 2]
# element = 4 :     range(1, 4) :       [1, 2, 3]
# flatten : [1, 1, 2, 1, 2, 3]
print output_strrdd.collect()
# output : [1, 1, 2, 1, 2, 3]

