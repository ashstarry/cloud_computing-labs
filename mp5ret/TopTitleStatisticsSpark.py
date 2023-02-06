#!/usr/bin/env python
import sys
from pyspark import SparkConf, SparkContext
import math

conf = SparkConf().setMaster("local").setAppName("TopTitleStatistics")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[1], 1)

counts = lines.map(lambda line: int(line.split("\t")[1]))

ans1 = math.floor(counts.mean())
ans2 = counts.sum()
ans3 = counts.min()
ans4 = counts.max()
ans5 = math.floor(counts.map(lambda x: (x - ans1) ** 2).mean())


outputFile = open(sys.argv[2], "w")
'''
TODO write your output here
write results to output file. Format
'''
outputFile.write('Mean\t%s\n' % ans1)
outputFile.write('Sum\t%s\n' % ans2)
outputFile.write('Min\t%s\n' % ans3)
outputFile.write('Max\t%s\n' % ans4)
outputFile.write('Var\t%s\n' % ans5)


sc.stop()

