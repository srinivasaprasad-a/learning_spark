from pyspark import SparkContext, SparkConf
from pyspark.accumulators import AccumulatorParam


# Spark only implements Accumulator parameter for numeric types.
# This class extends Accumulator support to the string type.
class StringAccumulatorParam(AccumulatorParam):
    def zero(self, value):
        return value

    def addInPlace(self, val1, val2):
        return val1 + val2


# a toy map function
def f(k):
    accumlog.add("Added 1 to %d.\n" % k)
    return k + 1


sparkconf = SparkConf().setAppName('Log Accumulator').setMaster('local')
sc = SparkContext(conf=sparkconf)

accumlog = sc.accumulator("", StringAccumulatorParam())
print "Initial value of the accumulator: '%s'" % accumlog.value

rdd = sc.parallelize(range(10))
print "Initial content of the RDD:", rdd.collect()
print "Now we apply the `f` function to the RDD:", rdd.map(f).collect()

print "Log is updated:"
print accumlog.value

