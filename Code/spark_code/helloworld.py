import sys
from pyspark import SparkContext, SparkConf
from operator import add
data = "hellloWorld"
#sc.parallelize("get the count of each character as output")
counts = data.map(lambda x: (x,1)).reduceByKey(add).sortBy(lambda x:x[1],ascending=False).collect()
for(word, count)in counts: print("{}.{}".format(word, count))
