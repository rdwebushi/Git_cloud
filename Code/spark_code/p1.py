import sys
from pyspark import SparkConf, SparkContext

sc=SparkContext()
# calculating no of people by same age 
# 1] using reducedByKey
output=sc.textFile("bank.csv")\
        .map(lambda x:(x.split(',')[1],1))\
        .reduceByKey(lambda x,y:x+y)
print(output.collect())
"""
# 2] using countByKey
output2=sc.textFile("bank.csv")\
        .map(lambda x:(x.split(',')[2],1))\
        .countByKey()
out_ar=output2.items()
"""
#print(out_ar)

#print(output2.lookup([('married'),('single')]))

