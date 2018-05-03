from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('Persist').setMaster('local')
sc = SparkContext(conf=sparkconf)

links_file = sc.sequenceFile("hdfs://172.19.0.2/pagerank/seq/links", keyClass="org.apache.hadoop.io.Text",
                             valueClass="org.apache.hadoop.io.Text")
urls_file = sc.sequenceFile("hdfs://172.19.0.2/pagerank/seq/urls", keyClass="org.apache.hadoop.io.Text",
                            valueClass="org.apache.hadoop.io.Text")
links_rdd = links_file.values()
urls_rdd = urls_file.values().persist()

res_rdd = urls_rdd.join(links_rdd)
print res_rdd.take(5)
