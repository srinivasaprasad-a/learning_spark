from pyspark import SparkContext, SparkConf


"""
SequenceFiles are a popular Hadoop format composed of flat files with key/value pairs. SequenceFiles have sync markers 
that allow Spark to seek to a point in the file and then resynchronize with the record boundaries. This allows Spark 
to efficiently read SequenceFiles in parallel from multiple nodes.
"""

sparkconf = SparkConf().setAppName('Sequence Read').setMaster('local')
sc = SparkContext(conf=sparkconf)

links_file = sc.sequenceFile("hdfs://172.19.0.2/pagerank/seq/links", keyClass="org.apache.hadoop.io.Text",
                             valueClass="org.apache.hadoop.io.Text")
urls_file = sc.sequenceFile("hdfs://172.19.0.2/pagerank/seq/urls", keyClass="org.apache.hadoop.io.Text",
                            valueClass="org.apache.hadoop.io.Text")
links_rdd = links_file.values()
urls_rdd = urls_file.values().persist()

res_rdd = urls_rdd.join(links_rdd)
print res_rdd.take(5)

"""
Datatype and its corresponding hadoop writable types

Int             IntWritable or VIntWritable2
Long            LongWritable or VLongWritable2
Float           FloatWritable
Double          DoubleWritable
Boolean         BooleanWritable
Array[Byte]     BytesWritable
String          Text
Array[T]        ArrayWritable<TW>3
List[T]         ArrayWritable<TW>3
Map[A, B]       MapWritable<AW, BW>3
"""