#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("OrphanPages")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def getLink(line):
    ret = []
    line = line.split(':')
    outDegree = line[0].strip()
    inDegrees = line[1].strip().split(' ')
    ret.append((int(outDegree), 0))
    for inDegree in inDegrees:
        #ret.append((int(inDegree), 1))
        #if (inDegree != '' and (outDegree != inDegree.strip())):
        ret.append((int(inDegree),1))
    return ret

counts = lines.flatMap(lambda line: getLink(line))
counts = counts.reduceByKey(lambda a, b: a + b)
counts = counts.filter(lambda x: x[1] == 0)
counts = counts.sortBy(lambda x: str(x[0]))
counts = counts.collect()


output = open(sys.argv[2], "w")

for count in counts:
    line = str(count[0]) + "\n"
    output.write(line)
output.close()
#write results to output file. Foramt for each line: (line + "\n")

sc.stop()

