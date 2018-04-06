from pyspark import SparkConf, SparkContext


def string_not_contains(word, fnd):
    if word.find(fnd) > 0:
        return False
    else:
        return True


sparkconf = SparkConf().setMaster('local').setAppName('wordcount')
sc = SparkContext(conf=sparkconf)

# Create a RDD
myfile = sc.textFile('hdfs://cima-prod-b:8020/tmp/prass016/README.md')

counts = myfile.flatMap(lambda line: line.split(" ")) \
    .filter(lambda word: string_not_contains(word, 'http')) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda v1, v2: v1 + v2)

# persist this RDD, so that no need to recompute, when it is referred again
counts.persist()
# counts RDD is a tuple
bigcounts = counts.filter(lambda k: k[1] > 2)

bigcounts.saveAsTextFile("hdfs://cima-prod-b:8020/tmp/prass016/output_1")

