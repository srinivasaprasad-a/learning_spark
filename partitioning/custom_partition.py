from pyspark import SparkContext, SparkConf
import urlparse
import os


def hash_domain(url):
    # netloc gives the domain name, so grouping all the key-values with same domain in to same partition
    # ParseResult(scheme='http', netloc='www1.hollins.edu', path='/Docs/web.htm', params='', query='', fragment='')
    return hash(urlparse.urlparse(url).netloc)


sparkconf = SparkConf().setAppName('Custom Partition').setMaster('local')
sc = SparkContext(conf=sparkconf)

hits_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'hits2.csv')
hits_rdd = sc.textFile(hits_file)\
    .map(lambda line: line.split(","))\
    .map(lambda line: (line[0], line[1], line[2]))
hits_rdd = hits_rdd.map(lambda x: (x[0], x)).partitionBy(2, hash_domain).persist()

# to understand which partition each of the element goes, the func used is hash(key) % num_partitions
# ex_hits_rdd = hits_rdd.map(lambda x: (x[0], hash(urlparse.urlparse(x[0]).netloc) % 4))
# print ex_hits_rdd.collect()

print "partitioner: " + str(hits_rdd.partitioner)
print "no. of partitions: " + str(hits_rdd.getNumPartitions())
print "rdd partition structure: " + str(hits_rdd.glom().collect())

"""
Output

partitioner: <pyspark.rdd.Partitioner object at 0x7f17be325150>
no. of partitions: 4
rdd partition structure: [[(u'http://www1.holl.edu/Docs/CompTech/Network/webmail_faq.htm', 
(u'http://www1.holl.edu/Docs/CompTech/Network/webmail_faq.htm', u'11292017', u'12')), 
(u'http://www1.holl.edu/docs/events/events.htm', 
(u'http://www1.holl.edu/docs/events/events.htm', u'11292017', u'35')), 
(u'http://www1.holl.edu/Docs/CampusLife/CampusLife.htm', 
(u'http://www1.holl.edu/Docs/CampusLife/CampusLife.htm', u'11292017', u'47')), 
(u'http://www1.holl.edu/Docs/Intercultural/default.htm', 
(u'http://www1.holl.edu/Docs/Intercultural/default.htm', u'11292017', u'84'))], 

[], 

[(u'http://www1.ins.edu/Docs/GVCalendar/gvmain.htm', 
(u'http://www1.ins.edu/Docs/GVCalendar/gvmain.htm', u'11292017', u'13')), 
(u'http://www1.ins.edu/Docs/Academics/international_programs/index.htm', 
(u'http://www1.ins.edu/Docs/Academics/international_programs/index.htm', u'11292017', u'98')), 
(u'http://www1.ins.edu/Registrar/registrar.htm', 
(u'http://www1.ins.edu/Registrar/registrar.htm', u'11292017', u'95')), 
(u'http://www.ins.edu/about/map/map.htm', 
(u'http://www.ins.edu/about/map/map.htm', u'11292017', u'39')), 
(u'http://www1.ins.edu/security/Default.htm', 
(u'http://www1.ins.edu/security/Default.htm', u'11292017', u'84'))], 

[(u'http://www1.hollins.edu/', 
(u'http://www1.hollins.edu/', u'11292017', u'95')), 
(u'http://www.hollins.edu/', 
(u'http://www.hollins.edu/', u'11292017', u'3')), 
(u'http://www1.hollins.edu/Docs/Forms/GetForms.htm', 
(u'http://www1.hollins.edu/Docs/Forms/GetForms.htm', u'11292017', u'73')), 
(u'http://www1.hollins.edu/Docs/Academics/acad.htm', 
(u'http://www1.hollins.edu/Docs/Academics/acad.htm', u'11292017', u'67')), 
(u'http://www1.hollins.edu/Docs/academics/online/cyber.htm', 
(u'http://www1.hollins.edu/Docs/academics/online/cyber.htm', u'11292017', u'29')), 
(u'http://www1.hollins.edu/docs/admin/admin.htm', 
(u'http://www1.hollins.edu/docs/admin/admin.htm', u'11292017', u'84'))]]

www1.hollins.edu and www.hollins.edu has got different hash values, but there value after % 4, is same, that's the
reason for same partition.
"""