import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext("local","Pair RDD examples")
"""marks=[("Rahul",88),("Sumit",92),("Swapnil",98),("Rahul",68),("Sumit",42),("Swapnil",18)]
output=sc.parallelize(marks).collect()
print(output)
"""
marks_rdd=sc.parallelize([("Rahul",88),("Sumit",92),("Swapnil",98),("Rahul",68),("Sumit",42),("Swapnil",18)])

#display mark sorted by student name
print(marks_rdd.sortByKey(False).collect())
