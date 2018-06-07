"""
simple example of accumulator variable that is used by multiple workers and return an accumulative value at the end
"""

from pyspark import SparkContext, SparkConf


def f(x):
    global acc
    acc += x   # acc = acc + x


sparkconf = SparkConf().setAppName('Simple Accumulator').setMaster('local[*]')
sc = SparkContext(conf=sparkconf)

nums = range(1, 10)
input_rdd = sc.parallelize(nums)
print input_rdd.collect()
print input_rdd.glom().collect()

acc = sc.accumulator(0)
input_rdd.foreach(f)

print acc.value

