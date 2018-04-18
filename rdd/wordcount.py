"""
Wordcount program, with filtering http URLs and writing to output file only those words with size more than 2
"""

from pyspark import SparkConf, SparkContext
import os


def string_not_contains(word, fnd):
    if word.find(fnd) > 0:
        return False
    else:
        return True


sparkconf = SparkConf().setMaster('local').setAppName('wordcount')
sc = SparkContext(conf=sparkconf)

input_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', '100west.txt')
output_file_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', '100west_wordcount')
# Create a RDD
myfile = sc.textFile(input_file)

counts = myfile.flatMap(lambda line: line.split(" ")) \
    .filter(lambda word: string_not_contains(word, 'http')) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda v1, v2: v1 + v2)

# persist this RDD, so that no need to recompute, when it is referred again
counts.persist()
# If persist is not called, at the counts.filter action, whole of counts RDD is recomputed
# counts RDD is a tuple
bigcounts = counts.filter(lambda k: k[1] > 2)

bigcounts.saveAsTextFile(output_file_dir)
