#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TopPopularLinks")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

def getLink(line):
    ret = []
    line = line.split(':')
    outDegree = line[0].strip()
    inDegrees = line[1].strip().split(' ')
    for inDegree in inDegrees:
        if (inDegree != ''):
            ret.append((int(inDegree),1))
    return ret
    
lines = sc.textFile(sys.argv[1], 1) 

counts = lines.flatMap(lambda line: getLink(line))
counts = counts.reduceByKey(lambda a, b: a + b)
counts = counts.takeOrdered(10, lambda x: -x[1])
counts = sorted(counts, key = lambda x: str(x[0]))

output = open(sys.argv[2], "w")

for line in counts[0:10]:
    line = str(line[0]) + "\t" + str(line[1]) + "\n"
    output.write(line)
output.close()

sc.stop()