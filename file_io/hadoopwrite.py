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

input = sc.parallelize([('lion', 34), ('tiger', 53), ('elephant', 313)])
input.saveAsHadoopFile('hdfs://172.19.0.2/newtextfile',
                       'org.apache.hadoop.mapred.TextOutputFormat',
                       'org.apache.hadoop.io.Text',
                       'org.apache.hadoop.io.IntWritable')

