#!/usr/bin/env python

'''Exectuion Command: spark-submit TitleCountSpark.py stopwords.txt delimiters.txt dataset/titles/ dataset/output'''

import sys
from pyspark import SparkConf, SparkContext

stopWordsPath = sys.argv[1]
delimitersPath = sys.argv[2]
stopWords = []
delimiters = []
with open(stopWordsPath) as f:
	stopWordList = f.readlines()

for stopWord in stopWordList:
    stopWords.append(stopWord.strip())

with open(delimitersPath) as f:
    delimiters = f.read().rstrip('\n')

conf = SparkConf().setMaster("local").setAppName("TitleCount")
conf.set("spark.driver.bindAddress", "127.0.0.1")
sc = SparkContext(conf=conf)

lines = sc.textFile(sys.argv[3], 1)

def getWords(title, delimiters, stopWords):
    title = title.strip().lower()
    for delimiter in delimiters:
        title = title.replace(delimiter, ' ')
    words = title.split(' ')
    ret = []
    for word in words:
        if (word not in stopWords and word != ''):
            ret.append(word)
    
    return ret

outputFile = open(sys.argv[4],"w")


counts = lines.flatMap(lambda line: getWords(line, delimiters, stopWords))
counts = counts.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
counts = counts.takeOrdered(10, lambda x: -x[1])
counts = sorted(counts, key = lambda x: x[0])

outputFile = open(sys.argv[4],"w")
for line in counts[0: 10]:
    line = str(line[0]) + "\t" + str(line[1]) + "\n"
    outputFile.write(line)
outputFile.close()

#write results to output file. Foramt for each line: (line +"\n")

sc.stop()
