from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('Dataframe Partition').setMaster('local[2]')
sc = SparkContext(conf=sparkconf)

transactions = [
    {'name': 'Bob', 'amount': 100, 'country': 'United Kingdom'},
    {'name': 'James', 'amount': 15, 'country': 'United Kingdom'},
    {'name': 'Marek', 'amount': 51, 'country': 'Poland'},
    {'name': 'Johannes', 'amount': 200, 'country': 'Germany'},
    {'name': 'Paul', 'amount': 75, 'country': 'Poland'},
]


def country_partition(cnty):
    return hash(cnty)


trans_rdd = sc.parallelize(transactions)
trans_rdd = trans_rdd.map(lambda x: (x['country'], x)).partitionBy(4, country_partition).persist()

print "partitioner: " + str(trans_rdd.partitioner)
print "no. of partitions: " + str(trans_rdd.getNumPartitions())
print "rdd partition structure: " + str(trans_rdd.glom().collect())

"""
partitioner: <pyspark.rdd.Partitioner object at 0x7ff6319f5f10>
no. of partitions: 4
rdd partition structure: [
[('United Kingdom', {'country': 'United Kingdom', 'amount': 100, 'name': 'Bob'}), 
('United Kingdom', {'country': 'United Kingdom', 'amount': 15, 'name': 'James'}), 
('Germany', {'country': 'Germany', 'amount': 200, 'name': 'Johannes'})], 
[], 
[('Poland', {'country': 'Poland', 'amount': 51, 'name': 'Marek'}), 
('Poland', {'country': 'Poland', 'amount': 75, 'name': 'Paul'})], 
[]
]
"""


def sum_amount(iterator):
    yield sum(transaction[1]['amount'] for transaction in iterator)


print trans_rdd.mapPartitions(sum_amount).collect()

"""
[315, 0, 126, 0]
"""
