from posixpath import split
import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext()

div=sc.textFile('bank.csv')\
    .map(lambda x:(x.split(',')[2],x.split(',')[1],x.split(',')[5]))\
    .filter(lambda x: x[0] =='divorced' and x[0]=='blue-collar' and x[0]) \
    

print(div.collect())

