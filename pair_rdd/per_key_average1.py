from pyspark import SparkContext, SparkConf

sparkconf = SparkConf().setAppName('Per Key Average').setMaster('local')
sc = SparkContext(conf=sparkconf)


pair_rdd = sc.parallelize([('Alphabet', 233), ('Google', 454), ('Tesla', 344), ('Alphabet', 435)])
res_rdd = pair_rdd.mapValues(lambda val: (val, 1)).reduceByKey(lambda val1, val2: (val1[0]+val2[0], val2[1]+val2[1]))
print res_rdd.collect()
print res_rdd.mapValues(lambda val: val[0]/val[1]).collect()
# output [('Tesla', (344, 1)), ('Alphabet', (668, 2)), ('Google', (454, 1))]
