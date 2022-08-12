import numbers
from operator import add
import sys

from pyspark import SparkContext, SparkConf
if __name__=="__main__":

    #create spark context with spark configeration 
    conf=SparkConf().setAppName("Read text to RDD - Python")
    sc=SparkContext(conf=conf)

    #read input file to RDD
    #sc=SparkContext("local","filter example")
    numbers=sc.parallelize([1,2,3,4,5,6,7,8,9,10])
    #filtered data 
    filteredData =numbers.filter(lambda x:x!=5).collect()
    print(filteredData)
    #now distinct data
    #rdd=sc.parallelize([1,1,1,1,1,1,1,23,2,33,5,6,7,8,9,0,5,4,3,2,42,42,3])
    #to check string 
    #rdd=sc.parallelize([1,1,1,1,1,1,1,23,2,33,5,"Hello","HeLLo","hi","HI",6,7,8,9,0,5,4,3,2,42,42,3])

    #rdd2=sc.parallelize(["Hello","HeLLo","hi","HI"])
    #rdd3=sc.parallelize([8,9,2,3,4,5,6,7,])
    #even_numbers=rdd.distinct().collect()
    #inter=rdd.intersection(rdd3).collect()
    #addTwoRDD=rdd.union(rdd2).collect()
    #print(even_numbers)
    #print(addTwoRDD)

    #print(inter)