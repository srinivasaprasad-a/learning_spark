"""
Simple map and reduce using spark
"""

from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('Simple RDD')
sc = SparkContext(conf=sparkconf)

rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6])
rstrdd = rdd1.map(lambda x: x+1)
# Use 'collect' when your entire data-set can be fitted into memory of a machine, else use 'take'
print rstrdd.collect()
# output [2, 3, 4, 5, 6, 7]

rstrdd = rdd1.reduce(lambda x, y: x+y)
print rstrdd
# output - 21
