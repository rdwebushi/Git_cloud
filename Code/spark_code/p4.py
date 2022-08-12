#find total and average balance for each type of job

import sys
from pyspark import SparkConf,SparkContext

sc=SparkContext()
bankData=sc.textFile("bank.csv")

banklines=bankData.map(lambda l:l.split(","))
selectFields=banklines.map(lambda x:(x[1],int(x[5])))
#key,val1,val2:=>(bluecollor,avg,total)
sumBal=selectFields.reduceByKey(lambda a, b:a+b)
for element in sumBal.collect():
    print(element)

print("---------average------------")
avgBal=selectFields.groupByKey().mapValues(lambda x: sum(x)/len(x))
for elements in avgBal.collect():
    print(elements)
