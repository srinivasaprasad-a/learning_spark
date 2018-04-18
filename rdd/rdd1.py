from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('parallelize')
sc = SparkContext(conf=sparkconf)

rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6])
rstrdd = rdd1.map(lambda x: x+1)
# Use 'collect' when your entire data-set can be fitted into memory of a machine, else use 'take'
print rstrdd.collect()

rstrdd = rdd1.reduce(lambda x, y: x+y)
print rstrdd

"""rstrdd = rdd1.aggregate((0, 0), 
                        (lambda acc, value: (acc[0] + value, acc[0] + 1)),
                        (lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])))
                        """

rdd2 = sc.parallelize([7, 8, 9], 2)
rstrdd = rdd2.aggregate(0,
                        (lambda acc, value: acc + value),
                        (lambda acc1, acc2: acc1 + acc2))
print rstrdd
