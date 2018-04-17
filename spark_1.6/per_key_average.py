from pyspark import SparkContext, SparkConf

sparkconf = SparkConf().setAppName('Per Key Average').setMaster('local')
sc = SparkContext(conf=sparkconf)


pair_rdd = sc.parallelize([('Alphabet', 233), ('Google', 454), ('Tesla', 344), ('Alphabet', 435)])
"""
res_rdd = pair_rdd.mapValues(lambda val: (val, 1)).reduceByKey(lambda val1, val2: (val1[0]+val2[0], val2[1]+val2[1]))
print res_rdd.collect()
print res_rdd.mapValues(lambda val: val[0]/val[1]).collect()
"""

# Implementing per key average using combineByKey()
"""
combineByKey parameters:
createCombiner - found a new value in each partition (please note - not first in rdd), then
                   createCombiner is called to create the initial value for the accumulator on that key
mergeValue - if it is a value seen before in the partition, then
               mergeValue is called with the current value for the accumulator for that key and the new value
mergeCombiners - since there will be multiple accumulators for the same key in each partition,
                    mergeCombiners is called to combine all those in all the partitions
"""
res_rdd = pair_rdd.combineByKey((lambda x: (x, 1)),                     #createCombiner
                                (lambda x, y: (x[0]+y, x[1]+1)),        #mergeValue
                                (lambda x, y: (x[0]+y[0], x[1]+y[1]))   #mergeCombiners
                                )

# Data flow
"""
('Alphabet', 233)      =>      accumulator['Alphabet']=createCombiner(233)     =>      ('Alphabet', (233, 1))
('Google', 454)        =>      accumulator['Google']=createCombiner(454)       =>      ('Google', (454, 1))
('Tesla', 344)         =>      accumulator['Tesla']=createCombiner(344)        =>      ('Tesla', (344, 1))
('Alphabet', 435)      =>      accumulator['Alphabet']=mergeValue((233, 1), 435)     =>('Alphabet', ((233+435), (1+1)))
                                                                                       ('Alphabet', (668, 2)
# mergeCombiners from all the partitions
mergeCombiners(accumulator['Alphabet'], accumulator['Google'], accumulator['Google'])   =>
                                                    [('Tesla', (344, 1)), ('Alphabet', (668, 2)), ('Google', (454, 1))]
"""
print res_rdd.collect()
print res_rdd.mapValues(lambda val: val[0]/val[1]).collect()
# output [('Tesla', (344, 1)), ('Alphabet', (668, 2)), ('Google', (454, 1))]
