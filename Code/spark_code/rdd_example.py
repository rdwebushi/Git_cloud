import sys
from pyspark import SparkContext, SparkConf
sc = SparkContext("locak","PySpark RDD example")
myRDD = sc.parallelize([('Big Data',80),('Python',85),('ML',56),('Java',78),('Web Analytics',78),('Good Practice',67),('software Design',77)])
myRDD.take(7)
myRDD.saveAsTextFile('myRDD/')