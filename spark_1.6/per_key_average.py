from pyspark import SparkContext, SparkConf

sparkconf = SparkConf().setAppName('Per Key Average').setMaster('local')
sc = SparkContext(conf=sparkconf)


pair_rdd = sc.parallelize([('Alphabet', 233), ('Google', 45432), ('Tesla', 3443), ('Alphabet', 43543)])
"""
res_rdd = pair_rdd.mapValues(lambda val: (val, 1)).reduceByKey(lambda val1, val2: (val1[0]+val2[0], val2[1]+val2[1]))
print res_rdd.collect()
print res_rdd.mapValues(lambda val: val[0]/val[1]).collect()
"""

# createCombiner - found a new value in each partition (please note - not first in rdd), then
#                   createCombiner is called to create the initial value for the accumulator on that key
# mergeValue - if it is a value seen before in the partition, then
#               mergeValue is called with the current value for the accumulator for that key and the new value
# mergeCombiners - since there will be multiple accumulators for the same key in each partition,
#                    mergeCombiners is called to combine all those
res_rdd = pair_rdd.combineByKey((lambda x: (x, 1)),
                                (lambda x, y: (x[0]+y, x[1]+1)),
                                (lambda x, y: (x[0]+y[0], x[1]+y[1]))
                                )
print res_rdd.collect()
print res_rdd.mapValues(lambda val: val[0]/val[1]).collect()
