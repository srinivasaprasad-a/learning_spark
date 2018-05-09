from pyspark import SparkContext, SparkConf


"""
org.apache.hadoop.mapred
    KeyValueTextInputFormat
    SequenceFileAsBinaryInputFormat
    SequenceFileAsTextInputFormat
    SequenceFileInputFormat<K,V>
    TextInputFormat
"""

sparkconf = SparkConf().setAppName('hadoop io read').setMaster('local')
sc = SparkContext(conf=sparkconf)

f = sc.hadoopFile('hdfs://172.19.0.2/newtextfile/part-00000',
              'org.apache.hadoop.mapred.TextInputFormat',
              'org.apache.hadoop.io.Text',
              'org.apache.hadoop.io.IntWritable')
print f.collect()
