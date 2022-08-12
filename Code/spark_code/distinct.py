from operator import add
import sys

from pyspark import SparkContext, SparkConf
sc=SparkContext("local","filter example")

rdd=sc.parallelize([1,1,1,1,1,1,1,23,2,33,5,6,7,8,9,0,5,4,3,2,42,42,3])
#to check string 
#rdd=sc.parallelize([1,1,1,1,1,1,1,23,2,33,5,"Hello","HeLLo","hi","HI",6,7,8,9,0,5,4,3,2,42,42,3])

rdd2=sc.parallelize(["Hello","HeLLo","hi","HI"])
rdd3=sc.parallelize([8,9,2,3,4,5,6,7,])
#even_numbers=rdd.distinct().collect()
inter=rdd.intersection(rdd3).collect()
addTwoRDD=rdd.union(rdd2).collect()
#print(even_numbers)
print(addTwoRDD)

print(inter)