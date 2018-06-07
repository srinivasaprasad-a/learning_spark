from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setMaster('local').setAppName('Simple Broadcast')
sc = SparkContext(conf=sparkconf)


words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
data = words_new.value
print "Stored data -> %s" % (data)

elem = words_new.value[0]
print "Printing a particular element in RDD -> %s" % (elem)

