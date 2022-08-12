import sys
from pyspark import SparkConf,SparkContext

sc=SparkContext("local","Pair RDD Example")
mark_rdd=sc.parallelize([('rahul',25),('swati',43),('shreya',32),('abhay',29),('rohan',22),('rahul',19),('swati',41),('rohan',44)])

mobile_rdd=sc.parallelize([('rahul','9898989898'),('swati','9890077654'),('shreya','7876543423'),('rohan','64743789467')])

print("CollectAsMap ==>")
print(mark_rdd.collectAsMap())
