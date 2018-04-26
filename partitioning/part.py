from pyspark import SparkContext, SparkConf
import os


sparkconf = SparkConf().setMaster('local').setAppName('Partitioning')
sc = SparkContext(conf=sparkconf)

urls_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'urls.txt')
links_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'links.txt')

urls_rdd = sc.textFile(urls_file).map(lambda x: x.split(' ')).partitionBy(10).persist()
links_rdd = sc.textFile(links_file).map(lambda x: x.split(' '))

#urls_rdd.take(5)
#links_rdd.take(5)

