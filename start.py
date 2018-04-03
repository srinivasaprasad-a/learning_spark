from pyspark import SparkConf, SparkContext

sparkconf = SparkConf().setMaster("local").setAppName("HelloPySpark")
sc = SparkContext(conf=sparkconf)

#sc.textFile("README.md")
