#1. calculate average year wise marks (i.e for 2 semister for 2 years) for each student

from gc import collect
from posixpath import split
import sys
from pyspark import SparkConf,SparkContext
sc=SparkContext()

fileData=sc.textFile("student.txt")

stdLines=fileData.map(lambda x:x.split(","))
print(stdLines.collect())
""" getData=stdLines.map(lambda x:(x[0],x[1],x[2],x[3]))
#convert to a list     
onlyBalance=getData.filter(lambda x:[(float(x[2])+float(x[3]))/2])
#calculate average 
markList= onlyBalance.collect()
print(markList)
 """
getData=stdLines.map(lambda x:[x[0],x[1],(float(x[2])+float(x[3]))/2])
#convert to a list     
#onlyBalance=getData.filter(lambda x:[(float(x[2])+float(x[3]))/2])
#calculate average 
markList= getData.collect()
print(markList)