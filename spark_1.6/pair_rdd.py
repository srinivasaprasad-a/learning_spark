from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('PairRDD')
sc = SparkContext(conf=sparkconf)

prdd = sc.parallelize([(1, 2), (3, 6), (3, 4), (2, 9), (2, 5)])
print 'input************'
print prdd.collect()
print 'reducebykey'
print prdd.reduceByKey(lambda val1, val2: val1 + val2).collect()

print 'groupbykey'
# groupbykey was giving pair rdd with values as pyspark.resultiterable.ResultIterable, which is a tuple
print prdd.groupByKey().map(lambda x: (x[0], list(x[1]))).collect()

print 'mapvalues'
print prdd.mapValues(lambda val: val + 1).collect()

print 'sortbykey'
print prdd.sortByKey().collect()


wordsrdd = sc.parallelize([('a', 'CTS'), ('b', 'TCS'), ('c', 'Wipro')])
print 'input************'
print wordsrdd.collect()
print 'flatmapvalues'
# used to tokenize
print wordsrdd.flatMapValues(lambda x: x).collect()

print 'keys'
print wordsrdd.keys().collect()

print 'values'
print wordsrdd.values().collect()