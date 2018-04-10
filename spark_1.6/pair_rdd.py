from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('PairRDD')
sc = SparkContext(conf=sparkconf)

prdd = sc.parallelize([(1, 2), (3, 6), (3, 4)])
print prdd.reduceByKey(lambda val1, val2: val1 + val2).collect()

#print prdd.groupByKey().foreach(lambda val: val[0] + val[1])

print prdd.mapValues(lambda val: val + 1).collect()

wordsrdd = sc.parallelize([(1, 'Cognizant Technology Solutions'), (2, 'Tata Consultancy Services'), (3, 'EMC')])
print wordsrdd.flatMapValues(lambda x: x).collect()

