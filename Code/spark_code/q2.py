#average only for second year

import sys
from pyspark import SparkConf,SparkContext

sc=SparkContext()
file=sc.textFile("student.txt")

stdLines=file.map(lambda l:l.split(','))
getData=stdLines.map(lambda x:[x[0],x[1],(float(x[2])+float(x[3]))/2])\
    .filter(lambda x: x[1] !='year1')

markList= getData.collect()
print(markList)

