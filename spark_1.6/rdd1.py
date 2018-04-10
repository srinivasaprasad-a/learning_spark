from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('parallelize')
sc = SparkContext(conf=sparkconf)

rdd1 = sc.parallelize([1, 2, 3, 4, 5, 6])
rstrdd = rdd1.map(lambda x: x+1)
# Use 'collect' when your entire dataset can be fitted into memory of a machine, else use 'take'
print rstrdd.collect()
