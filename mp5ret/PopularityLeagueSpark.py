#!/usr/bin/env python

#Execution Command: spark-submit PopularityLeagueSpark.py dataset/links/ dataset/league.txt
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularityLeague")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1) 

def getLink(line):
    ret = []
    line = line.split(':')
    outDegree = line[0].strip()
    inDegrees = line[1].strip().split(' ')
    for inDegree in inDegrees:
        if (inDegree != ''):
            ret.append((int(inDegree),1))
    return ret

counts = lines.flatMap(lambda line: getLink(line))
counts = counts.reduceByKey(lambda a, b: a + b)

leagueIds = sc.textFile(sys.argv[2], 1)

league_list = leagueIds.map(lambda line: int(line.strip())).collect()
league = counts.filter(lambda x: x[0] in set(league_list))
league = league.sortBy(lambda x: str(x[0]))
counts_type = league.sortBy(lambda x: (x[1] * 1000000000) + (100000000 - x[0]))
counts_type = counts_type.map(lambda x: x[0]).collect()
counts_league = league.collect()

n = len(counts_type)
ret = []

output = open(sys.argv[3], "w")
for i in range(0,n):
    ret.append({'page': counts_league[i][0], 'rank': counts_type.index(counts_league[i][0])})

for line in ret:
    line = str(line['page']) + "\t" + str(line['rank']) + "\n"
    output.write(line)
output.close()

sc.stop()

