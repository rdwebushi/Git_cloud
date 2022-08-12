#find jobs and balance that have balance value higher than the average of blue collar people
import sys
from pyspark import SparkConf, SparkContext
sc=SparkContext()
bankData=sc.textFile("bank.csv")

banklines=bankData.map(lambda l:l.split(","))
selectFields=banklines.map(lambda x:(x[1],int(x[5])))

filteredData=selectFields.filter(lambda f:(f[0]=='blue-collar'))

#only get balanced field

onlyBalance= filteredData.map(lambda x:(int(x[1])))

#convert to a list 
balance= onlyBalance.collect()

#avg 
avgBal = sum(balance)/len(balance)

print("The average of blue collar is %d"% avgBal)
higherThanBlueCollarAverage=selectFields.filter(lambda f: int(f[1])>avgBal)
#print output

for element in higherThanBlueCollarAverage.collect():
    print("jobs: %s and Balance: %d"%(element[0],element[1]))