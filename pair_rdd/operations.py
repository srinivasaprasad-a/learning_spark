from pyspark import SparkContext, SparkConf


"""
Pair RDD - transformations and actions
"""

sparkconf = SparkConf().setMaster('local').setAppName('PairRDD')
sc = SparkContext(conf=sparkconf)

# Every pairRDD is a python tuple

prdd = sc.parallelize([(1, 2), (3, 6), (3, 4), (2, 9), (2, 5)])
prdd.persist()
print 'input************'
print prdd.collect()

print 'reducebykey'
print prdd.reduceByKey(lambda val1, val2: val1 + val2).collect()

print 'groupbykey'
# groupbykey was giving pair rdd with values as pyspark.resultiterable.ResultIterable, which is a tuple
print prdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()
# output [(1, [2]), (2, [9, 5]), (3, [6, 4])]

print 'mapvalues'
print prdd.mapValues(lambda val: val + 1).collect()

print 'sortbykey'
print prdd.sortByKey().collect()

print 'foldbykey'
print prdd.foldByKey((0, 0), (lambda x, y: (x, y))).collect()


wordsrdd = sc.parallelize([('a', 'Google'), ('b', 'Apple'), ('c', 'Tesla')])
print 'input************'
print wordsrdd.collect()

print 'flatmapvalues'
# use to tokenize
print wordsrdd.flatMapValues(lambda x: x).collect()

print 'keys'
print wordsrdd.keys().collect()

print 'values'
print wordsrdd.values().collect()


print 'input************'
rdd1 = sc.parallelize([(3, 5), (3, 6), (7, 4), (9, 1)])
rdd1.persist()
print rdd1.collect()
rdd2 = sc.parallelize([(7, 8), (2, 9)])
rdd2.persist()
print rdd2.collect()

print 'subtractByKey'
print rdd1.subtractByKey(rdd2).collect()

print 'join'
print rdd1.join(rdd2).collect()

print 'rightOuterJoin'
print rdd1.rightOuterJoin(rdd2).collect()

print 'leftOuterJoin'
print rdd1.leftOuterJoin(rdd2).collect()
# output [(9, (1, None)), (3, (5, None)), (3, (6, None)), (7, (4, 8))]

print 'cogroup'
print rdd1.cogroup(rdd2).mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
# output [(2, ([], [9])), (9, ([1], [])), (3, ([5, 6], [])), (7, ([4], [8]))]

print 'actions********************'
print prdd.collect()
print prdd.collectAsMap()
print prdd.lookup(3)
print prdd.countByValue()
