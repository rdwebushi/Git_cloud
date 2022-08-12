import sys

from pyspark import SparkContext, SparkConf
sc=SparkContext("local","even number")

numbers=sc.parallelize(range(1,50))

even_numbers=numbers.filter(lambda x:x % 2 == 0).collect()
print(even_numbers)
