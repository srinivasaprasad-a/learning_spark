from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('pair_aggregate')
sc = SparkContext(conf=sparkconf)

rdd1 = sc.parallelize([('a', 2), ('b', 4), ('c', 6), ('a', 7), ('d', 8), ('a', 9)], 2)
rstrdd = rdd1.aggregateByKey(('x', 0),                                        # zero value
                        (lambda acc, value: (acc[0], acc[1] + value)),        # sequence function
                        (lambda acc1, acc2: (acc1[0], acc1[1] + acc2[1])))    # combiner function
print rstrdd.collect()
# output - [('a', ('x', 18)), ('c', ('x', 6)), ('b', ('x', 4)), ('d', ('x', 8))]
