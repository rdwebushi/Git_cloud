#countByKey()
import sys
from pyspark import SparkConf,SparkContext

sc=SparkContext("local","Pair RDD Example")
mark_rdd=sc.parallelize([('rahul',25),('swati',43),('shreya',32),('abhay',29),('rohan',22),('rahul',19),('swati',41),('rohan',44)])

dirct_rdd=mark_rdd.countByKey().items()
for key, value in dirct_rdd:
    print(key,value)
""" 
mark_rdd = mark_rdd.join(mobile_rdd)
for key,value in full_rdd.collect():
    print(key,list(value)) """