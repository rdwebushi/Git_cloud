#3. show sorted(highest-to-lowest) average grades in second year
#Hint:()

import sys
from pyspark import SparkConf,SparkContext

sc=SparkContext()
file=sc.textFile("student.txt")

stdLines=file.map(lambda l:l.split(','))
getData=stdLines.map(lambda x:[x[0],x[1],(float(x[2])+float(x[3]))/2])\
    .filter(lambda x: 'year2' in x)
    #.filter(lambda x:x[1]!='year1')
markList= getData.collect()
print(markList)

markList1=getData.sortBy(keyfunc=lambda x:-x[2])
#question 4
topThree=markList1.take(3)

print(markList1.collect())
print(topThree)
