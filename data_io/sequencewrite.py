from pyspark import SparkContext, SparkConf
import os


sparkconf = SparkConf().setAppName('Sequence Read').setMaster('local')
sc = SparkContext(conf=sparkconf)

output = os.path.join(os.path.dirname(os.path.realpath('__file___')), '..', 'resources', 'seq_output')
input = sc.parallelize([('lion', 34), ('tiger', 53), ('elephant', 313)])
input.saveAsSequenceFile(output)

