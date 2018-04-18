from pyspark import SparkConf, SparkContext


def run_avg(iterator):
    yield sum(iterator)/4


sparkconf = SparkConf().setMaster('local').setAppName('running_average')
sc = SparkContext(conf=sparkconf)

ip_rdd = sc.parallelize(list(range(1, 100)), 25)
op_rdd = ip_rdd.mapPartitions(run_avg)
print op_rdd.collect()
