from pyspark import SparkContext, SparkConf
import os


sparkconf = SparkConf().setMaster('local').setAppName('Word Count')
sc = SparkContext(conf=sparkconf)

input_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'resources', '100west.txt')
input_rdd = sc.textFile(input_file)
res_rdd = input_rdd.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda val1, val2: val1+val2)
print res_rdd.collect()
