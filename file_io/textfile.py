"""
Wordcount program, reading an input text file and write to text file
"""

from pyspark import SparkConf, SparkContext
import os


sparkconf = SparkConf().setMaster('local').setAppName('wordcount')
sc = SparkContext(conf=sparkconf)

input_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', '100west.txt')
output_file_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', '100west_wordcount')

myfile = sc.textFile(input_file)
counts = myfile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
counts.saveAsTextFile(output_file_dir)
