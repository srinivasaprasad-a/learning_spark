from pyspark import SparkConf, SparkContext, SparkFiles
import os
import urllib3
import json


sparkconf = SparkConf().setAppName('mapPartitions').setMaster('local[*]')
sc = SparkContext(conf=sparkconf)


# in mappartitions, connection is called only once
def processcallsigns(signs):
    # Create a connection pool
    http = urllib3.PoolManager()
    # the URL associated with each call sign record
    urls = map(lambda x: "http://73s.com/qsos/%s.json" % x, signs)
    # create the requests (non-blocking)
    requests = map(lambda x: (x, http.request('GET', x)), urls)
    # fetch the results
    result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
    # remove any empty results and return
    return filter(lambda x: x[1] is not None, result)


def fetchcallsigns(input):
    return input.mapPartitions(lambda cs:processcallsigns(cs))


input_file = os.path.join(os.path.dirname(os.path.realpath('__file__')),'..','resources','callsigns','callsigns.txt')
output_dir = os.path.join(os.path.dirname(os.path.realpath('__file__')),'..','resources','callsigns','callsigns_full')

callsigns = sc.textFile(input_file)
print callsigns.glom().collect()

contactslist = fetchcallsigns(callsigns)

distScript = "./src/R/finddistance.R"
distScriptName = "finddistance.R"
sc.addFile(distScript)


def hasDistInfo(call):
    """Verify that a call has the fields required to compute the distance"""
    requiredFields = ["mylat", "mylong", "contactlat", "contactlong"]
    return all(map(lambda f: call[f], requiredFields))


def formatCall(call):
    """Format a call so that it can be parsed by our R program"""
    return "{0},{1},{2},{3}".format(
        call["mylat"], call["mylong"],
        call["contactlat"], call["contactlong"])


pipeInputs = contactslist.values().flatMap(lambda calls: map(formatCall, filter(hasDistInfo, calls)))
distances = pipeInputs.pipe(SparkFiles.get(distScriptName))
print distances.collect()
