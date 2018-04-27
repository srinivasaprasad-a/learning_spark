"""
aggregate function on sequence input
"""

from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('Aggregate').setMaster('local')
sc = SparkContext(conf=sparkconf)

# custom partition into 2 partitions
rdd2 = sc.parallelize([7, 8, 9, 4, 8, 6, 7, 8], 2)
# aggregate is a action
rstrdd = rdd2.aggregate(0,                                  # zerovalue
                        (lambda acc, value: acc + value),   # sequence operation
                        (lambda acc1, acc2: acc1 + acc2))   # combiner operation

"""
dataflow - say for example [7, 8, 9, 4] is in 1st partition and [8, 6, 7, 8] is in 2nd partition
partition-1
7   =>  accumulator[7]=seqOp(7, zerovalue)  =>  7+0=7
8   =>  accumulator[8]=seqOp(8, zerovalue)  =>  8+0=8
9   =>  accumulator[9]=seqOp(9, zerovalue)  =>  9+0=9
4   =>  accumulator[4]=seqOp(4, zerovalue)  =>  4+0=4

partition-2
8   =>  accumulator[8]=seqOp(8, zerovalue)  =>  8+0=8
6   =>  accumulator[6]=seqOp(6, zerovalue)  =>  6+0=6
7   =>  accumulator[7]=seqOp(7, zerovalue)  =>  7+0=7
8   =>  accumulator[8]=seqOp(8, zerovalue)  =>  8+8=16

output from partition-1
accumulator[7], accumulator[8], accumulator[9], accumulator[4]

output from partition-2
accumulator[8], accumulator[6], accumulator[7]

combiner operation on all the accumulators in all partitions
accumulator[7] + accumulator[8] + accumulator[9] + accumulator[4] + accumulator[8] + accumulator[6] + accumulator[7]
"""
print rstrdd
