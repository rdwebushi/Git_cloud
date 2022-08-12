"""find total number of married and unmarried people """
import sys
from pyspark import SparkConf,SparkContext
sc= SparkContext()
output=sc.textFile('bank.csv')\
    .map(lambda x:(x.split(',')[2],1))\
    .filter(lambda x: x[0] !='single') \
    .reduceByKey(lambda x, y:x+y)\
    .collect()
print(output)
sc.stop()