from pyspark import SparkContext, SparkConf
import urlparse
import os


def hash_domain(url):
    print url
    return hash(urlparse.urlparse(url).netloc)


sparkconf = SparkConf().setAppName('Custom Partition').setMaster('local')
sc = SparkContext(conf=sparkconf)

urls_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'urls.txt')
urls_rdd = sc.textFile(urls_file)
hash_urls_rdd = urls_rdd.partitionBy(20, hash_domain(urls_rdd.values()))

print hash_urls_rdd.first()
