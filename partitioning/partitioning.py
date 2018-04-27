from pyspark import SparkContext, SparkConf


sparkconf = SparkConf().setAppName('Aggregate').setMaster('local')
sc = SparkContext(conf=sparkconf)

links_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'links.csv')
urls_file = os.path.join(os.path.dirname(os.path.realpath('__file__')), '..', 'resources', 'pagerank', 'urls.csv')

